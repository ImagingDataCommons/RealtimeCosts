#
# Copyright 2018 Google LLC
# Copyright 2020, 2021 Institute for Systems Biology
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import base64
import json
import os
import time
import datetime
import math
import pprint

from googleapiclient import discovery
from googleapiclient.errors import HttpError
from google.cloud import storage
from google.cloud import bigquery
from google.cloud import logging as glog


KEY_MAP = os.environ["KEY_MAP"]
GPU_KEY_MAP = os.environ["GPU_KEY_MAP"]
PD_KEY_MAP = os.environ["PD_KEY_MAP"]
LIC_KEY_MAP = os.environ["LIC_KEY_MAP"]

CPU_PRICES = os.environ["CPU_PRICES"]
RAM_PRICES = os.environ["RAM_PRICES"]
GPU_PRICES = os.environ["GPU_PRICES"]
PD_PRICES = os.environ["PD_PRICES"]
BQ_PRICES = os.environ["BQ_PRICES"]
GCS_PRICES = os.environ["GCS_PRICES"]
CPU_LIC_PRICES = os.environ["CPU_LIC_PRICES"]
GPU_LIC_PRICES = os.environ["GPU_LIC_PRICES"]
RAM_LIC_PRICES = os.environ["RAM_LIC_PRICES"]

STATE_BUCKET = os.environ["STATE_BUCKET"]
PULL_MULTIPLIER = float(os.environ["PULL_MULTIPLIER"])
ESTIMATE_PULL_MULTIPLIER = float(os.environ["ESTIMATE_PULL_MULTIPLIER"])
COST_BQ = os.environ["COST_BQ"]
EGRESS_NOTIFY_MULTIPLIER = float(os.environ["EGRESS_NOTIFY_MULTIPLIER"])
MAX_CPUS = int(os.environ["MAX_CPUS"])

PROJECT_ID = os.environ["PROJECT_ID"]


HISTORY = 15
SKU_DEPTH = 8
SKU_DAYS = 4

'''
----------------------------------------------------------------------------------------------
Trigger for HTTP
'''
def web_trigger(request):
    project_id = PROJECT_ID
    control_billing(project_id, None, None, None, True)
    return "Completed"

'''
----------------------------------------------------------------------------------------------
Trigger for PubSub
'''
def pubsub_trigger(data, context):
    pubsub_data = base64.b64decode(data['data']).decode('utf-8')
    pubsub_json = json.loads(pubsub_data)
    cost_amount = pubsub_json['costAmount']
    budget_amount = pubsub_json['budgetAmount']
    budget_name = pubsub_json["budgetDisplayName"]
    project_id = budget_name.replace('-budget', '')
    cis = pubsub_json["costIntervalStart"]
    print('Project {} cost: {} reports at {}: start: {} '.format(project_id, str(pubsub_json["costAmount"]),
                                                                            context.timestamp, str(cis)))
    control_billing(project_id, cost_amount, budget_amount, cis, False)
    return

'''
----------------------------------------------------------------------------------------------
Common function to handle both entry points.
'''

def control_billing(project_id, cost_amount, budget_amount, cis, only_estimate_burn):

    message_root_fmt = "EXTERNAL BUDGET ALERT {}" # USE this to trigger monitoring alerts.

    #
    # Get the states for the budget:
    #

    now_time = datetime.datetime.now(datetime.timezone.utc)
    now_hour = datetime.datetime(now_time.year, now_time.month, now_time.day, hour=now_time.hour, tzinfo=now_time.tzinfo)

    storage_client = storage.Client()
    try:
        bucket = storage_client.get_bucket(STATE_BUCKET)
    except:
        raise Exception("State bucket does not exist") # Better yet, create it and keep going

    burn_blob = "{}_born_state.json".format(project_id) ## FIXME FOR DEPLOY
    blob = bucket.blob(burn_blob)
    burn_blob_str = blob.download_as_string() if blob.exists() else b''
    if burn_blob_str == b'':
        burn_state = {"vm_instances" : {},
                      "pdisks": {},
                      "inventory_time": None,
                      "elapsed_seconds_since_previous_inventory": None,
                      "discount_cpu_tokens": {},
                      "discount_ram_tokens": {},
                      "discount_gpu_tokens": {}
                     }
    else:
        burn_state = json.loads(burn_blob_str)
        if 'discount_cpu_tokens' not in burn_state:
            burn_state["discount_cpu_tokens"] = {}
        if 'discount_ram_tokens' not in burn_state:
            burn_state["discount_ram_tokens"] = {}
        if 'discount_gpu_tokens' not in burn_state:
            burn_state["discount_gpu_tokens"] = {}

    month_rollover = False
    first_time = False
    now_month = datetime.datetime(now_time.year, now_time.month, 1, 0, tzinfo=now_time.tzinfo)
    last_inventory_time = burn_state["inventory_time"]
    if burn_state["inventory_time"] is not None:
        last_inventory_dt = datetime.datetime.strptime(burn_state["inventory_time"], '%Y-%m-%dT%H:%M:%S.%f%z')
        elapsed = now_time - last_inventory_dt
        burn_state["elapsed_seconds_since_previous_inventory"] = (elapsed.days * 86400.0) + elapsed.seconds
        month_rollover = last_inventory_dt < now_month
    else:
        first_time = True
    # For development:
    first_time = len(burn_state["discount_cpu_tokens"]) == 0
    burn_state["inventory_time"] = now_time.strftime('%Y-%m-%dT%H:%M:%S.%f%z')

    discount_tables = _build_sustained_discount_table()
    discount_classes = discount_tables.keys()

    if month_rollover:
        print("clear discount tokens")
        _clear_discount_tokens(burn_state['vm_instances'], burn_state["discount_cpu_tokens"], "cpu")
        _clear_discount_tokens(burn_state['vm_instances'], burn_state["discount_ram_tokens"], "ram")
        _clear_discount_tokens(burn_state['vm_instances'], burn_state["discount_gpu_tokens"], "gpu")

    #
    # If we are not just doing a burn estimate, we get here via a pubsub message about every 20 minutes all
    # month long. We don't really care about the monthly billing here, we care about the cumulative amount
    # over several months which is what will cause a shutdown. But if we just want a burn estimate, we can
    # skip lots of stuff here:
    #

    need_to_act = False
    fraction  = 0.0
    total_spend = 0.0
    additional_fees = 0.0
    total_discounts = 0.0
    per_hour_charge = 0.0
    budget_state = None
    message_state = None
    week_num = None
    project_name = 'projects/{}'.format(project_id)

    log_client = glog.Client(project=project_id)
    log_client.get_default_handler()
    log_client.setup_logging()
    cost_logger = log_client.logger("cost_estimation_log")

    if not only_estimate_burn:
        _process_budget_state(project_id, bucket, now_time, budget_amount,
                              cost_amount, cis, message_root_fmt)

    #
    # We want to limit the number of instances that can be run without having to set a quota. We also want to
    # calculate the current burn rate. Both require an inventory of all VMs in the project:
    #

    excess_vm_stop_list = []
    running_inventory = {}
    stopped_inventory = {}
    all_ids = set()
    disk_inventory = {}
    all_vm_dict = {}
    all_pd_dict = {}
    roll_discount_tokens = (month_rollover or first_time)

    is_enabled = _check_compute_services_for_project(project_id)
    if is_enabled:
        compute = discovery.build('compute', 'v1', cache_discovery=False)
        total_cpus = _prepare_inventory(compute, burn_state, project_id, now_hour, now_time, now_month,
                                        last_inventory_time, discount_tables, roll_discount_tokens,
                                        discount_classes, all_ids)
        print("Project {} cpu count {}".format(project_id, total_cpus))
        if (not only_estimate_burn) and (total_cpus > MAX_CPUS):
            message_key = message_root_fmt.format("3")
            shutdown = total_cpus - MAX_CPUS
            print("{}: Project {} is over CPU limit: {} Shutting down {}".format(message_key, project_id, total_cpus, shutdown))
            excess_vm_stop_list = _prepare_shutdown_list(running_inventory, shutdown)

    else:
        type_dict = None
        compute = None
        print("Project {} does not have compute enabled".format(project_id))

    #
    # With the VM inventory and disk inventories, we now gather up sku information, we run an estimate of current VM burn rate:
    #

    vm_pricing_cache = {}
    gpu_pricing_cache = {}
    pd_pricing_cache = {}
    cpu_lic_pricing_cache = {}
    gpu_lic_pricing_cache = {}
    ram_lic_pricing_cache = {}
    bq_pricing_cache = {}

    usage_hours = []
    for i in range(HISTORY):
        usage_hours.append((now_hour - datetime.timedelta(hours=i)).strftime('%Y-%m-%dT%H%z'))

    sku_set = set()
    vm_count = 0
    sku_desc = {}
    master_chart = {}
    google_max = {}

    if is_enabled:
        for k, v in running_inventory.items():
            vm_dict = all_vm_dict[v['id']]
            _gather_vm_skus(v, vm_pricing_cache, gpu_pricing_cache, vm_dict)
            _gather_license_skus(v, cpu_lic_pricing_cache, gpu_lic_pricing_cache, ram_lic_pricing_cache, vm_dict)
            vm_count += 1

        for disk in disk_inventory:
            pd_dict = all_pd_dict[disk['id']]
            _gather_disk_sku(disk, pd_pricing_cache, pd_dict)

        #
        # Figure out the burn rate:
        #

        for clock_time in usage_hours:
            cost_per_sku = {}
            discount_per_sku = {}
            for k, vm_dict in all_vm_dict.items():
                # Seeing key errors here if the bookkeeping has fallen over for awhile
                # e.g. KeyError: '2021-11-15T06+0000'"
                # Which implies the loaded VM dict does not have all the last 13 hours.
                # So, adding the key test:
                # WRONG! Even if machine is not running NOW, it might have been running a while ago!
                # FIXME: Need to insure SKUs are present. Also, machine will have lost its discount tokens.
                # FIXME: that it had when it was actually running!
                # FIXME: IMPORTANT!! SKU costs in BQ DO NOT CONTAIN DISCOUNTS. THOSE COSTS ARE LIST PRICE!
                #if vm_dict['status'] == "RUNNING":
                minutes_running = vm_dict["usage"][clock_time] if clock_time in vm_dict["usage"] else 0.0
                print("FIXME! VMs never seen and not running have empty SKU fields")
                _core_and_ram_and_gpu_cost(minutes_running/60, vm_dict, cost_per_sku)
                _calc_licensing_cost(minutes_running/60, vm_dict, cost_per_sku)
            for k, pd_dict in all_pd_dict.items():
                minutes_running = pd_dict["usage"][clock_time] if clock_time in pd_dict["usage"] else 0.0
                _calc_disk_cost(minutes_running/60, pd_dict, cost_per_sku)
            for k in cost_per_sku:
                sku_set.add(k)
                if clock_time in master_chart:
                    sku_records = master_chart[clock_time]
                else:
                    sku_records = {}
                    master_chart[clock_time] = sku_records
                if k in sku_records:
                    sku_record = sku_records[k]
                else:
                    sku_record = {"calc": 0.0, "goog": 0.0, "disc": 0.0}
                    sku_records[k] = sku_record
                sku_record["calc"] += cost_per_sku[k]
                if k in discount_per_sku:
                    sku_record["disc"] += discount_per_sku[k]

        #
        # We calculate sustained use discounts working with memory and CPU tokens directly. Count up hours
        # linked to CPU and RAM tokens. Count the unused tokens, and then tokens tied to running VMs. These
        # are aggregated per region:
        #

        token_resource_use = _calc_unused_tokens(burn_state)
        for k, vm_dict in all_vm_dict.items():
            vm_region = vm_dict['region']
            machine_class = vm_dict['machine_type'].split('-')[0]
            _calc_running_tokens(vm_dict, token_resource_use, vm_region, machine_class)

        print("sustained use discounts:")
        _calc_sustained_disc_from_tokens(token_resource_use, discount_tables)


    ip_usage_for_month = _get_billing_account_ip_usage(COST_BQ, now_month.strftime('%Y-%m-%d'))
    for row, amt in ip_usage_for_month.items():
        print(row, amt)

    #
    # Above stuff was for VMs. Now do BQ:
    #

    _calc_running_bq_costs(project_id, usage_hours, sku_set, master_chart, now_month, bq_pricing_cache)

    #
    # This pulls the actual charge history from the BQ cost export table for this project. The problem with
    # these costs is that they lag reality by about 6-12 hours
    #

    # FIXME: The non-zero cutoff would prevent us from finding an old cost that dropped to zero
    chist = _get_charge_history_for_sku(COST_BQ, project_id, .00001, SKU_DEPTH, SKU_DAYS)
    for k, hist in chist.items():
        sku_set.add(k)
        for dicto in hist:
            goog_time = dicto["usage_start_time"].strftime('%Y-%m-%dT%H%z')
            if goog_time in master_chart:
                sku_records = master_chart[goog_time]
            else:
                sku_records = {}
                master_chart[goog_time] = sku_records
            if k in sku_records:
                sku_record = sku_records[k]
            else:
                sku_record = {"calc": 0.0, "goog": 0.0, "disc": 0.0}
                sku_records[k] = sku_record

            sku_record["goog"] += dicto["total_cost"]

            #
            # Track the maximum Google cost per sku:
            #

            if k not in google_max:
                google_max[k] = 0.0
            if sku_record["goog"] > google_max[k]:
                google_max[k] = sku_record["goog"]

            # Capture description from Google:
            if k not in sku_desc:
                sku_desc[k] = dicto['sku_description']

    storage_est = _estimate_current_storage_costs(chist, now_time)
    for sku_id, price in storage_est.items():
        print("Storage ", sku_id, price)

    #
    # All skus that show up in the Google dump need to be present at all time points. This is to
    # create cost items up until the present time for all charges. Up until now, we have created
    # cost estimates for recent charges only for VM-related SKUs. But things like storage and
    # egress and request counts should be estimated as well. Question is whether to take average,
    # or mode, or max, or extrapolate:
    #

    #
    # NO! FIXME: STORAGE costs show up once a day, and "usage" start and stop is for an hour, but actually holds
    # the info for the whole day. Need to treat storage costs in that manner.
    #


    #for goog_time, sku_records in master_chart.items():
    #    for sku in sku_set:
    #        if sku not in sku_records:
    #            goog_val = google_max[sku] if (sku in google_max) else 0.0
    #            # FIXME: THIS IS NOT A GOOGLE VALUE, BUT A CALC VALUE
    #            sku_record = {"calc": 0.0, "goog": goog_val, "disc": 0.0}
    #            sku_records[sku] = sku_record

    #
    # Dump out the sku records:
    #

    for clock_time, sku_records in master_chart.items():
        for sku in sorted(sku_records.keys()):
            record = sku_records[sku]
            diff = record["calc"] - record["goog"]
            sku_text = sku_desc[sku] if sku in sku_desc else "No description collected"
            print("{} {} ({}) calc: {} google: {} disc: {} (delta: {})".format(clock_time, sku, sku_text,
                                                                      record["calc"], record["goog"], record["disc"],
                                                                      str(round(diff, 5))))
            if diff > 0.0:
                additional_fees += diff
                #print("added fees now {} added {} from {}".format(additional_fees, diff, sku_text))
            if record["disc"] > 0.0:
                total_discounts += record["disc"]

        print("-------------------------------------------------------------------------------------")

    print("add {} to the current Google reported spending".format(round(additional_fees, 2)))
    print("Subtract {} discounts".format(round(total_discounts, 2)))

    total_cost, total_credit = _get_project_cost_for_month_by_sku(COST_BQ, now_month.strftime('%Y-%m-%d'), project_id, now_month.strftime('%Y%m'))
    print("Total cost and credit for month {}, {}, {}".format(round(total_cost, 2), round(total_credit, 2), round(total_cost + total_credit, 2)))

    #
    # Compute hours to exhaustion:
    #

    last_full_hour_key = (now_hour - datetime.timedelta(hours=1)).strftime('%Y-%m-%dT%H%z')
    sku_records =  master_chart[last_full_hour_key]
    for sku, record in sku_records.items():
        per_hour_charge += record["calc"]


    #
    # Here we take the opportunity to prune out VMs from our persistent store if they no longer
    # exist (stopped or running)
    #

    all_vm_dict = _prune_vm_persistent_store(all_vm_dict, all_ids)

    burn_state['vm_instances'] = all_vm_dict

    #
    # Save the instance state:
    #

    # Seems we get a checksum complaint if we don't reinitialize the blob:
    blob = bucket.blob(burn_blob)
    #for machine_id, vm_dict in all_vm_dict.items():
    #    pprint.pprint(vm_dict)
    blob.upload_from_string(json.dumps(burn_state, indent=4, sort_keys=True))

    if not only_estimate_burn:

        #
        # Sum up all months of expenses:
        #

        total_spend = 0
        for month, cost_amount in budget_state.items():
            total_spend += cost_amount

        total_spend += additional_fees
        fraction = float(total_spend) / float(budget_amount)

        message = "Project: {} Budget: ${} Charges: ${:.2f} (including estimated additional charges to come: ${:.2f}) Fraction: {:.4f}".format(
            project_id, budget_amount, total_spend, additional_fees, fraction)
        cost_logger.log_text(message)

        #
        # NOT TRUE. This only includes estimated VM costs. Should also take the last reported values of other big ticket
        # items as well! Look for SKUs that appear in our Google history and add them in TOO!
        # Note that storage costs (e.g. SKU 5F7A-5173-CF5B: Standard Storage Northern Virginia) are charged once a
        # day. They are recorded as usage in a ONE HOUR WINDOW (e.g. Start : 021-11-04 19:00:00 UTC and END: 2021-11-04 20:00:00 UTC)
        # but the price that appears is for a one day charge. It appears that bits in buckets is only measured once
        # per day.
        #

        remaining_funds = budget_amount - total_spend
        if remaining_funds > 0:
            hours_to_go = remaining_funds / per_hour_charge
            message = "Project: {} Has an estimated {:.2f} hours left".format(project_id, hours_to_go)
            print(message)
            cost_logger.log_text(message)

        need_to_act = (fraction >= 1.0)

        if need_to_act:
            message_key = message_root_fmt.format("2")
            fire_off = (message_key not in message_state) or message_state[message_key] != week_num
            if fire_off:
                print("{}: Project {} is now over budget! fraction {}".format(message_key, project_id, str(fraction)))
                message_state[message_key] = week_num

        #
        # If we need to act, do it:
        #

        have_appengine = _check_appengine(project_id)
        if need_to_act:
            full_stop_list = _prepare_shutdown_list(running_inventory)
            if len(full_stop_list) > 0:
                print("Project {} turning off VMs".format(project_id))
                _process_shutdown_list(project_id, full_stop_list)
            if have_appengine:
                print("Project {} turning off App Engine".format(project_id))
                _turn_off_appengine(project_id)
                _check_appengine(project_id)
            print("Project {} finished turning off resources".format(project_id))
            if fraction >= PULL_MULTIPLIER:
                message_key = message_root_fmt.format("4")
                print("{}: Project {} pulling billing account".format(message_key, project_id))
                _pull_billing_account(project_id, project_name)
                print("Project {} completed pulling billing account".format(project_id))
        else:
            billing_on = _check_billing(project_id, project_name)
            if billing_on:
                print("Project {} billing enabled: {}".format(project_id, str(billing_on)))
            print("Project {} fraction {} still in bounds".format(project_id, str(fraction)))
            num_excess = len(excess_vm_stop_list)
            if num_excess > 0:
                print("Project {} turning off {} excess VMs".format(project_id, num_excess))
                _process_shutdown_list(project_id, excess_vm_stop_list)

    return

'''
----------------------------------------------------------------------------------------------
process budget state obtained from budget pub/sub, if we are being called in that fashion
'''

def _process_budget_state(project_id, bucket, now_time, budget_amount, cost_amount, cis, message_root_fmt):
    #
    # Get the state for the budget:
    #

    state_blob = "{}_state.json".format(project_id)
    week_num = now_time.strftime("%V")

    blob = bucket.blob(state_blob)
    blob_str = blob.download_as_string() if blob.exists() else b''
    raw_budget_state = {"budget": budget_amount, "messages": {}, "amounts": {cis: 0}} if (
    blob_str == b'') else json.loads(blob_str)
    # Bring old dicts up to speed. Messages were added before budget, so this order catches all cases:
    if "messages" not in raw_budget_state:
        budget_super_state = {"messages": {}, "amounts": raw_budget_state}
    else:
        budget_super_state = raw_budget_state

    if ("budget" not in budget_super_state) or (budget_super_state['budget'] != budget_amount):
        budget_super_state['budget'] = budget_amount

    budget_state = budget_super_state['amounts']
    budget_state[cis] = cost_amount
    message_state = budget_super_state['messages']

    #
    # How much is being spent to move data around:
    #

    total_egress = float(_calc_egress(COST_BQ, project_id))
    print("Project {} total egress and download: {}".format(project_id, total_egress))
    egress_thresh = EGRESS_NOTIFY_MULTIPLIER * float(budget_amount)
    if total_egress > egress_thresh:
        message_key = message_root_fmt.format("1")
        fire_off = (message_key not in message_state) or message_state[message_key] != week_num
        if fire_off:
            print("{}: Project {} total egress and download ({}) exceeds threshold {} ".format(message_key, project_id,
                                                                                               total_egress,
                                                                                               egress_thresh))
            message_state[message_key] = week_num

    #
    # Stash the latest cost data and message status back in the bucket before we wander off and do things, just in
    # case everything goes nuts:
    #

    # Seems we get a checksum complaint if we don't reinitialize the blob:
    blob = bucket.blob(state_blob)
    blob.upload_from_string(json.dumps(budget_super_state, indent=4, sort_keys=True))
    return

'''
----------------------------------------------------------------------------------------------
prepare VM inventory
'''

def _prepare_inventory(compute, burn_state, project_id, now_hour, now_time, now_month,
                       last_inventory_time, discount_tables, roll_discount_tokens,
                       discount_classes, all_ids):

    running_inventory, type_dict, total_cpus, stopped_inventory = \
        _check_compute_engine_inventory_aggregate(compute, project_id)
    all_vm_dict = burn_state['vm_instances']
    # Handle stopped machines first, to insure their cpu tokens can be swapped by running machines
    # in this iteration
    for k, v in stopped_inventory.items():
        _build_vm_inventory(v, type_dict, now_hour, now_time, now_month, last_inventory_time,
                            all_vm_dict, discount_tables)
        all_ids.add(k[4])
        machine_class = k[1].split('-')[0]
        is_preempt = all_vm_dict[k[4]]['preempt']
        if machine_class in discount_classes and not is_preempt:
            _calc_advanced_sustained_use(k[4], all_vm_dict, burn_state, roll_discount_tokens)
    for k, v in running_inventory.items():
        _build_vm_inventory(v, type_dict, now_hour, now_time, now_month, last_inventory_time,
                            all_vm_dict, discount_tables)
        all_ids.add(k[4])
        machine_class = k[1].split('-')[0]
        is_preempt = all_vm_dict[k[4]]['preempt']
        if machine_class in discount_classes and not is_preempt:
            _calc_advanced_sustained_use(k[4], all_vm_dict, burn_state, roll_discount_tokens)

    all_pd_dict = burn_state['pdisks']
    disk_inventory = _check_persistent_disk_inventory_aggregate(compute, project_id)
    for disk in disk_inventory:
        _build_pd_inventory(disk, now_hour, now_time, all_pd_dict)

    return total_cpus


'''
----------------------------------------------------------------------------------------------
Estimate_current_storage_costs
'''

def _estimate_current_storage_costs(charge_hist, now_time):

    retval = {}
    now_stamp = now_time.timestamp()
    gcs_skus = _get_gcs_skus(GCS_PRICES)
    storage_points_per_sku = {}

    for k, hist in charge_hist.items():
        for dicto in hist:
            if k in gcs_skus:
                if k in storage_points_per_sku:
                    storage_points = storage_points_per_sku[k]
                else:
                    storage_points = []
                    storage_points_per_sku[k] = storage_points

                new_point = [dicto["usage_start_time"].timestamp(), dicto["total_cost"]]
                storage_points.append(new_point)

    for k, points in storage_points_per_sku.items():
        if len(points) == 1:
            retval[k] = points[0][1]
        else:
            slope, intercept = best_fit(points)
            now_store = (now_stamp * slope) + intercept
            if now_store < 0.0:
                retval[k] = 0.0
            else:
                retval[k] = now_store

    return retval

'''
----------------------------------------------------------------------------------------------
Calculate current BQ store charges:
'''

def _calc_running_bq_costs(project_id, usage_hours, sku_set, master_chart, now_month, bq_pricing_cache):


    #
    # The 1 TB/mo free tier for BQ is on a per-billing account basis. So to figure out current spending for
    # this project, we need to know of BQ spending across all projects on this billing account
    #

    bq_usage_for_month = _get_billing_account_bq_usage(COST_BQ, now_month.strftime('%Y-%m-%d'))

    #
    # Enumerate the recent BQ jobs that have been run for this project
    #

    bq_history = _find_recent_bq_usage(project_id, usage_hours)
    tebibyte = 2**40
    for hour, bq_hist in bq_history.items():
        cost_per_sku = {}
        for loc, loc_info in bq_hist['per_location'].items():
            price_info = _pull_bq_pricing(BQ_PRICES, loc_info['location'], bq_usage_for_month, bq_pricing_cache)
            if price_info[5] in cost_per_sku:
                the_cost_for_sku = cost_per_sku[price_info[5]]
            else:
                the_cost_for_sku = 0.0

            cost_per_sku[price_info[5]] = the_cost_for_sku + (price_info[3] * (loc_info['byte_count'] / tebibyte))

        for k in cost_per_sku:
            sku_set.add(k)
            if hour in master_chart:
                sku_records = master_chart[hour]
            else:
                sku_records = {}
                master_chart[hour] = sku_records
            if k in sku_records:
                sku_record = sku_records[k]
            else:
                sku_record = {"calc": 0.0, "goog": 0.0, "disc": 0.0}
                sku_records[k] = sku_record
            sku_record["calc"] += cost_per_sku[k]

    return


'''
----------------------------------------------------------------------------------------------
Clean up our VM persistent store
'''

def _prune_vm_persistent_store(all_vm_dict, all_ids):
    #
    # Ditch VMs that no longer exist.
    #
    print("NO! DELETED MACHINES STILL COST MONEY THAT WAS NOT SUNK TO BQ YET")
    print("IF NOT IN INVENTORY, MARK AS TERMINATED AND DELETE ONLY AFTER UNSUNK COSTS ARE RECONCILED")
    if True:
        return all_vm_dict
    print(all_ids)
    new_all_vm_dict = {}
    for machine_id, vm_dict in all_vm_dict.items():
        #
        # Remove machines from our persistent store if they no longer exist
        #
        print(machine_id)
        if machine_id in all_ids:
             print("adding {}".format(machine_id))
             print("Clean up:")
             print("last start", vm_dict['last_start_uptime_record'])
             print("last stop", vm_dict['last_stop_uptime_record'])
             for rec in vm_dict['uptime_records']:
                 print(rec)
             new_all_vm_dict[machine_id] = vm_dict

    return new_all_vm_dict

'''
----------------------------------------------------------------------------------------------
Build up the persistent disk inventory
'''

def _build_pd_inventory(disk, now_hour, now_time, all_pd_dict):
    #
    # pull per-instance record out of the persistent state, or create it:
    #
    if disk['id'] in all_pd_dict:
        pd_dict = all_pd_dict[disk['id']]
    else:
        pd_dict = {
            "id": disk['id'],
            "gb_size": disk['size'],
            "pd_sku": None,
            "usage": {}
        }
        all_pd_dict[disk['id']] = pd_dict
    pd_dict['usage'] = _fill_time_series(now_hour, now_time, disk['created'].strftime('%Y-%m-%dT%H:%M:%S.%f%z'),
                                         pd_dict['usage'])
    return

'''
----------------------------------------------------------------------------------------------
Build up the VM inventory
'''

def _build_vm_inventory(machine_info, type_dict, now_hour, now_time, now_month,
                        last_inventory_time, all_vm_dict, discount_tables):

    #now_month_str = now_month.strftime('%Y-%m-%dT%H:%M:%S.%f%z')

    #
    # pull per-instance record out of the persistent state, or create it:
    #

    if machine_info['id'] in all_vm_dict:
        vm_dict = all_vm_dict[machine_info['id']]
        #mi_started_dt = datetime.datetime.strptime(machine_info['started'], '%Y-%m-%dT%H:%M:%S.%f%z')
        #started_previous_month = now_month > mi_started_dt
        print("machine {} has status {} was {}".format(machine_info['id'], machine_info['status'], vm_dict['status']))
        vm_dict['status'] = machine_info['status']
        #
        # Always keep these fields up to date:
        #
        vm_dict['started'] = machine_info['started']
        vm_dict['last_stop'] = machine_info['lastStop']

        # Deal with stopped machines.
        if vm_dict['status'] != "RUNNING":
            if vm_dict['last_start_uptime_record'] is not None:
                # If the start time for this stopped machine is the same as the one on record, then nothing has
                # happened to the machine since we last saw it, except that it has stopped. We update the
                # record that had just the start time with the stop time, and add it to our uptime records.
                # We won't keep doing this, but just do it the first time:
                if vm_dict['last_start_uptime_record']['start'] == machine_info['started']:
                    vm_dict['last_start_uptime_record']['stop'] = machine_info['lastStop']
                    vm_dict['last_stop_uptime_record'] = vm_dict['last_start_uptime_record']
                    vm_dict['uptime_records'].append(vm_dict['last_stop_uptime_record'])
                    vm_dict['last_start_uptime_record'] = None
                else:
                    # We have a record where we did not get a chance to close out the previous uptime,
                    # but the start time has changed from what we recorded. The machine was stopped, then
                    # briefly cycled start/stop at least once, before our next polling. We only know about
                    # the last. The conservative approach is to record that it shut down at the last
                    # inventory period, since that will underestimate monthly uptime and thus the discount
                    vm_dict['last_start_uptime_record']['stop'] = last_inventory_time
                    vm_dict['last_stop_uptime_record'] = vm_dict['last_start_uptime_record']
                    vm_dict['uptime_records'].append(vm_dict['last_stop_uptime_record'])
                    vm_dict['last_start_uptime_record'] = None
            #
            # It is possible that the machine has been cycled on/off since our last polling. We want
            # to capture that in our records.
            #
            elif ((vm_dict['last_stop_uptime_record'] is None) or
                  ((vm_dict['last_stop_uptime_record']['start'] != machine_info['started']) and
                   (vm_dict['last_stop_uptime_record']['stop'] != machine_info['lastStop']))):
                new_cycle = {'start': machine_info['started'], 'stop': machine_info['lastStop']}
                vm_dict['uptime_records'].append(new_cycle)
                vm_dict['last_stop_uptime_record'] = new_cycle

        else: # machine is running NOW, but we might not have updated the last_start record
              # yet! Needs to be filled in! May not even have a start record yet:
            if vm_dict['last_start_uptime_record'] is None:
                vm_dict['last_start_uptime_record'] = {"start": machine_info['started'], "stop": None}
            elif vm_dict['last_start_uptime_record']['start'] != machine_info['started']:
                vm_dict['last_start_uptime_record']['stop'] = machine_info['lastStop']
                vm_dict['uptime_records'].append(vm_dict['last_start_uptime_record'])
                vm_dict['last_stop_uptime_record'] = vm_dict['last_start_uptime_record']
                vm_dict['last_start_uptime_record'] = {"start": machine_info['started'], "stop": None}

    else:
        print("never seen this before {}".format(machine_info['id']))
        vm_dict = {
            "id": machine_info['id'],
            'status': machine_info['status'],
            "preempt": machine_info['preemptible'],
            "machine_type": machine_info['machineType'],
            "machine_zone": machine_info['zone'],
            "memory_gb": type_dict[(machine_info['zone'], machine_info['machineType'])]['memory'] / 1024,
            "region": '-'.join(machine_info['zone'].split('-')[0:-1]),
            "accel": machine_info['accelerator_inventory'],
            'created': machine_info['created'],
            'started': machine_info['started'],
            'last_stop': machine_info['lastStop'],
            'last_stop_uptime_record': None,
            'last_start_uptime_record': None,
            'uptime_records': [],
            'cpus': machine_info['cpus'],
            'cpus_per_core': None,
            'cpu_tokens': {},
            'ram_tokens': {},
            "skus": {
                "vm_sku": None,
                "ram_sku": None,
                "gpu_sku": None,
                "lic_sku": None,
                "cpu_os_lic_sku": None,
                "gpu_os_lic_sku": None,
                "ram_os_lic_sku": None
            },
            "usage": {}
        }
        all_vm_dict[machine_info['id']] = vm_dict

        #
        # A new record is created either because the machine is new, or because we just started our
        # monitoring. If the former, the machine could, or could not, have a stop time. Since we
        # are now building dicts even for machines that are stopped, there might be a stop even for
        # new machines.
        #
        # If the machine is stopped, then we will have a start time for it that is older than the
        # stop time, and we can create a complete new record. But if the machine is running, and
        # we have a last stop time, we don't know when it started before that. So what start time
        # to associate with that stop time? If the last stop time is before the start of the month,
        # we can ignore it, and consider the start time
        # to be the first of the month. Otherwise, all we can use in that case is the created time,
        # or the last stop time (which is the most conservative, since there will be less of a discount).
        #

        if vm_dict['status'] != "RUNNING":
            new_record = {'start': vm_dict['started'], 'stop': vm_dict['last_stop']}
            vm_dict['last_stop_uptime_record'] = new_record
            vm_dict['uptime_records'].append(new_record)
        else:
            if (vm_dict['last_stop'] is not None) and (vm_dict['last_start_uptime_record'] is None):
                last_stop_dt = datetime.datetime.strptime(vm_dict['last_stop'], '%Y-%m-%dT%H:%M:%S.%f%z')

                # FIXME NO! This makes no sense. The stop is before the start!
                if last_stop_dt < now_month:
                    new_record = {'start': now_month.strftime('%Y-%m-%dT%H:%M:%S.%f%z'), 'stop': vm_dict['last_stop']}
                else:
                    new_record = {'start': vm_dict['last_stop'], 'stop': vm_dict['last_stop']}
                vm_dict['uptime_records'].append(new_record)
                vm_dict['last_stop_uptime_record'] = new_record

            vm_dict['last_start_uptime_record'] = {"start": machine_info['started']}


    #
    # If machine is running, figure out for how long:
    #
    if vm_dict['status'] == "RUNNING":

        last_stop_dt = datetime.datetime.strptime(vm_dict['last_stop'],
                                                  '%Y-%m-%dT%H:%M:%S.%f%z') if vm_dict['last_stop'] is not None else None
        started_dt = datetime.datetime.strptime(vm_dict['started'], '%Y-%m-%dT%H:%M:%S.%f%z')

        #
        # Figure out the uptime for the month. If the uptime records list is empty, the machine has never stopped.
        #

        uptime_this_month = datetime.timedelta(minutes=0)
        uptime_current_run = datetime.timedelta(minutes=0)
        if last_stop_dt is None: # machine has never stopped:
            uptime_this_month = now_time - (now_month if (started_dt < now_month) else started_dt)
            uptime_current_run = uptime_this_month
        else:
            # go though all the uptime records and sum them up, then take the last 'last_start_uptime_record'
            # and calculate the uptime in this month by summing the uptimes
            for uptime_record in vm_dict['uptime_records']:
                uptime_stop_dt = datetime.datetime.strptime(uptime_record['stop'], '%Y-%m-%dT%H:%M:%S.%f%z')
                uptime_start_dt = datetime.datetime.strptime(uptime_record['start'], '%Y-%m-%dT%H:%M:%S.%f%z')
                if uptime_stop_dt < now_month:  # Ignore. This is a previous month
                    continue
                use_start = now_month if (uptime_start_dt < now_month) else uptime_start_dt
                uptime_this_month += uptime_stop_dt - use_start
            uptime_start_dt = datetime.datetime.strptime(vm_dict['last_start_uptime_record']["start"], '%Y-%m-%dT%H:%M:%S.%f%z')
            use_start = now_month if (uptime_start_dt < now_month) else uptime_start_dt
            uptime_current_run = now_time - use_start
            uptime_this_month += uptime_current_run

        print("Uptime this month: {} days, {} seconds".format(uptime_this_month.days, uptime_this_month.seconds))

        vm_dict['uptime_this_month'] = (uptime_this_month.days * 1440.0) + (uptime_this_month.seconds / 60.0)
        vm_dict['uptime_current_run'] = (uptime_current_run.days * 1440.0) + (uptime_current_run.seconds / 60.0)

    #
    # For the last HISTORY hours, even if the machine is stopped, create usage records:
    #

    vm_dict['usage'] = _fill_time_series_too(now_hour, now_time, vm_dict, HISTORY)
    return


'''
----------------------------------------------------------------------------------------------
Do free tier calcs
'''

def _calc_free_tier_use():
    # E2-micro instances enjoy a free tier discount up to a certain amount per month. The tier appears to
    # allow up to 721 core-hours per month, and 721 gibibyte hours a month for free. Relevant SKUs
    # are ("2E72-F977-86B9", "168E-35F8-9C79"). Have not checked, but likely the free tier is
    # consumed across all projects for one billing account.
    #
    # IP addresses for non-preemptible (sku C054-7F72-A02E) also have a free tier up to 721 hours (note that
    # 30 days is 720 hours) then are charged .004 USD/hr per VM. Note that this is applied at the
    # BILLING ACCOUNT level, not the project level. So expenses for a project are affected by IP usage
    # across the all projects under a billing account. So costs will start showing up earlier in the
    # month than you might expect. Note I also see a ragged build up of charges about three hours before
    # the cumulative time across all projects cross 721 hours, e.g. for four IP addresses:
    # ($0.001877, 710.22), ($0.00648, 714.22), ($0.015378, 718.22), ($0.016, 722.22). Possibly due to
    # staggered project exports to BQ.
    #

    pass

'''
----------------------------------------------------------------------------------------------
Clear discount tokens at the start of the month:
'''

def _clear_discount_tokens(all_vm_dict, discount_tokens, part):

    if 'tokens_by_zones' in discount_tokens:
        discount_tokens['tokens_by_zones'].clear()

    key = "{}_tokens".format(part)
    for tid, vm_dict in all_vm_dict.items():
        if key in vm_dict:
            vm_dict[key].clear()

    return


'''
----------------------------------------------------------------------------------------------
Calculate sustained use discount
'''

def _calc_sustained_disc_from_tokens(token_resource_use, discount_tables):

    cpu_cost = 0.0
    ram_cost = 0.0
    gpu_cost = 0.0
    cpu_disc = 0.0
    ram_disc = 0.0
    gpu_disc = 0.0

    for region, per_type in token_resource_use['cpu_usage'].items():
        for machine_class, token_summary in per_type.items():
            if token_summary['num_resource'] != 0:
                if token_summary['token_price'] is None:
                    print("ERROR", token_summary)
                    token_summary['token_price'] = 0.0
                cpu_discount = _get_sustained_discount_for_minutes(
                    token_summary['total_minutes'] / float(token_summary['num_resource']),
                    machine_class, discount_tables)
                cpu_cost += (token_summary['token_price'] * (token_summary['total_minutes'] / 60.0))
                cpu_disc += (token_summary['token_price'] * (token_summary['total_minutes'] / 60.0)) * (1.0 - cpu_discount)

    for region, per_type in token_resource_use['ram_usage'].items():
        for machine_class, token_summary in per_type.items():
            if token_summary['num_resource'] != 0:
                if token_summary['token_price'] is None:
                    print("ERROR", token_summary)
                    token_summary['token_price'] = 0.0
                ram_discount = _get_sustained_discount_for_minutes(
                    token_summary['total_minutes'] / float(token_summary['num_resource']),
                    machine_class, discount_tables)
                ram_cost += (token_summary['token_price'] * (token_summary['total_minutes'] / 60.0))
                ram_disc += (token_summary['token_price'] * (token_summary['total_minutes'] / 60.0)) * (1.0 - ram_discount)

    for region, per_type in token_resource_use['gpu_usage'].items():
        for machine_class, token_summary in per_type.items():
            if token_summary['num_resource'] != 0:
                if token_summary['token_price'] is None:
                    print("ERROR", token_summary)
                    token_summary['token_price'] = 0.0
                gpu_discount = _get_sustained_discount_for_minutes(
                    token_summary['total_minutes'] / float(token_summary['num_resource']),
                    'any_gpu', discount_tables)
                gpu_cost += (token_summary['token_price'] * (token_summary['total_minutes'] / 60.0))
                gpu_disc += (token_summary['token_price'] * (token_summary['total_minutes'] / 60.0)) * (1.0 - gpu_discount)



    print(cpu_cost, cpu_disc, ram_cost, ram_disc, gpu_cost, gpu_disc)
    return

'''
----------------------------------------------------------------------------------------------
Calculate resource tied to tokens in use for the given machine
'''

def _calc_running_tokens(vm_dict, token_resource_use, vm_region, machine_class):

    _sum_running_resource_tokens(vm_dict, token_resource_use, vm_region, machine_class, 'cpu_tokens', 'cpu_usage')
    _sum_running_resource_tokens(vm_dict, token_resource_use, vm_region, machine_class, 'ram_tokens', 'ram_usage')
    _sum_running_resource_tokens(vm_dict, token_resource_use, vm_region, machine_class, 'gpu_tokens', 'gpu_usage')

    return

'''
----------------------------------------------------------------------------------------------
Calculate resource tied to tokens in use for the given machine
'''

def _sum_running_resource_tokens(vm_dict, token_resource_use, vm_region, machine_class, resource_key, use_key):
    minu, tok, price = _add_machine_resource_tokens(vm_dict, resource_key)
    toks = token_resource_use[use_key]

    if vm_region in toks:
        machine_dict = toks[vm_region]
    else:
        machine_dict = {}
        toks[vm_region] = machine_dict

    if machine_class in machine_dict:
        token_summary = machine_dict[machine_class]
    else:
        token_summary = {'num_resource': 0, 'total_minutes': 0.0, 'token_price': None}
        machine_dict[machine_class] = token_summary

    if tok > 0:
        token_summary['total_minutes'] += minu
        token_summary['num_resource'] += tok
        if token_summary['token_price'] is None:
            token_summary['token_price'] = price
        elif token_summary['token_price'] != price:
            raise Exception("Token price mismatch {} {}".format(token_summary['token_price'], price))

    print(token_summary)

    return

'''
----------------------------------------------------------------------------------------------
Calculate hours tied to unused tokens
'''

def _calc_unused_tokens(burn_state):

    print("CUT")
    unused_resource_use = {
        "cpu_usage": _token_sum(burn_state["discount_cpu_tokens"]),
        "ram_usage": _token_sum(burn_state["discount_ram_tokens"]),
        "gpu_usage": _token_sum(burn_state["discount_gpu_tokens"])
    }

    return unused_resource_use

'''
----------------------------------------------------------------------------------------------
Sum up and store: for region x, for machine type y, we have n minutes of time tied to z tokens
'''

def _token_sum(token_resource):
    resource_usage = {}
    print("TS")
    if 'tokens_by_zones' in token_resource:
        tokens_by_zones = token_resource['tokens_by_zones']
        for region, pool in tokens_by_zones.items():

            if region in resource_usage:
                machine_dict = resource_usage[region]
            else:
                machine_dict = {}
                resource_usage[region] = machine_dict

            for machine_class, token_dict in pool.items():

                if machine_class in machine_dict:
                    token_summary = machine_dict[machine_class]
                else:
                    token_summary = {'num_resource': 0, 'total_minutes': 0.0, 'token_price': None}
                    machine_dict[machine_class] = token_summary

                for hour, tok_list_for_hour in token_dict['tokens'].items():
                    for token in tok_list_for_hour:
                        print("TOKEN", token)
                        if token_summary['token_price'] is None:
                            token_summary['token_price'] = token['token_price']
                        elif token_summary['token_price'] != token['token_price']:
                            raise Exception("Token price not consistent {} vs {}".format(token_summary['token_price'], token['token_price']))
                        # FIXME? WAS NOT SUMMED?
                        token_summary['total_minutes'] += token['current_usage'] + token['prior_usage']
                        token_summary['num_resource'] += 1
                        print(token_summary)

    return resource_usage

'''
----------------------------------------------------------------------------------------------
Do advanced sustained use calcs
'''

def _calc_advanced_sustained_use(machine_id, all_vm_dict, burn_state, initialize):
    # Looking at the Google docs:
    # https://cloud.google.com/compute/docs/sustained-use-discounts#calculating_discounts
    # Doing sustained used calcs can be tricky when machines are started and stopped during
    # the month. For example, if a 4-cpu machine runs for half a month, and then is swapped for
    # a 16 cpu machine for the rest of the month, four of the CPUs in the 16-cpu machine will go
    # towards a month-long discount rate for those four CPUs, while the remaining 12 get a
    # half month discount.
    #
    # Note this means that machines running purely in parallel are no problem, but when machines
    # do not overlap, those spans will get charged in a complex fashion. Note this is on a
    # per-region basis.
    #
    # So, for each region, create a collection of CPU tokens. This collection is recreated at the start
    # of each month. When a VM is started, draw tokens from the collection, taking the most-used, but currently
    # free, CPU tokens from the collection. If no tokens are free, we create a new one. When we detect that
    # a VM has been stopped, we return its tokens to the collection. While Google is unclear in theid docs,
    # we will assume that stopping a VM allows its tokens to be immediately taken up by a running VM if the
    # released tokens are older than some tokens the VM is currently using.
    #

    vm_dict = all_vm_dict[machine_id]

    #
    # Preemptible machines are never eligible for sustained discounts:
    #

    if vm_dict["preempt"]:
        return

    cpux_tokens = burn_state["discount_cpu_tokens"]
    ram_tokens = burn_state["discount_ram_tokens"]
    gpu_tokens = burn_state["discount_gpu_tokens"]

    month_uptime = None
    if vm_dict['status'] == "RUNNING":
        uptime = vm_dict['uptime_current_run']
        month_uptime = vm_dict['uptime_this_month']
    else:
        last_stop_uptime_record = vm_dict['last_stop_uptime_record']
        uptime_stop_dt = datetime.datetime.strptime(last_stop_uptime_record['stop'], '%Y-%m-%dT%H:%M:%S.%f%z')
        uptime_start_dt = datetime.datetime.strptime(last_stop_uptime_record['start'], '%Y-%m-%dT%H:%M:%S.%f%z')
        uptime_last_run = uptime_stop_dt - uptime_start_dt
        uptime = (uptime_last_run.days * 1440.0) + (uptime_last_run.seconds / 60.0)

    vm_region = vm_dict['region']
    machine_class = vm_dict['machine_type'].split('-')[0]

    #
    # If running, this machine should have one token for each CPU or GB. If it is running and has no tokens, generally
    # it must be new, and we need to get tokens from the pool. Exception is if we are just beginning to
    # inventory some time in first month. We still need token, but the whole month of uptime must be
    # applied to the token. If the pool is empty, create a new token
    #

    #
    # Do CPU:
    #

    if 'tokens_by_zones' in cpux_tokens:
        tokens_by_zones = cpux_tokens['tokens_by_zones']
    else:
        tokens_by_zones = {}
        cpux_tokens['tokens_by_zones'] = tokens_by_zones

    if vm_region in tokens_by_zones:
        region_token_pool = tokens_by_zones[vm_region]
    else:
        region_token_pool = {}
        tokens_by_zones[vm_region] = region_token_pool

    if 'cpu_tokens' in vm_dict:
        this_machine_tokens = vm_dict['cpu_tokens']
    else:
        this_machine_tokens = {}
        vm_dict['cpu_tokens'] = this_machine_tokens

    need_tokens = int(round(vm_dict['cpus'])) - _num_discount_tokens(this_machine_tokens)

    token_price = 0.0
    for key in vm_dict['skus']['vm_sku']:
        token_price = vm_dict['skus']['vm_sku'][key][3]
        break

    allocate_tokens(need_tokens, vm_dict, machine_id, region_token_pool, uptime,
                    this_machine_tokens, month_uptime, initialize, 'cpu_tokens', machine_class, token_price)

    #
    # Now do RAM, by Gigabyte:
    #

    if 'tokens_by_zones' in ram_tokens:
        tokens_by_zones = ram_tokens['tokens_by_zones']
    else:
        tokens_by_zones = {}
        ram_tokens['tokens_by_zones'] = tokens_by_zones

    if vm_region in tokens_by_zones:
        region_token_pool = tokens_by_zones[vm_region]
    else:
        region_token_pool = {}
        tokens_by_zones[vm_region] = region_token_pool

    if 'ram_tokens' in vm_dict:
        this_machine_tokens = vm_dict['ram_tokens']
    else:
        this_machine_tokens = {}
        vm_dict['ram_tokens'] = this_machine_tokens

    for key in vm_dict['skus']['vm_sku']:
        token_price = vm_dict['skus']['vm_sku'][key][5]
        break

    need_tokens = int(math.ceil(vm_dict['memory_gb'])) - _num_discount_tokens(this_machine_tokens)
    allocate_tokens(need_tokens, vm_dict, machine_id, region_token_pool, uptime,
                    this_machine_tokens, month_uptime, initialize, 'ram_tokens', machine_class, token_price)

    #
    # Now do GPU, by accelerator:
    #

    if len(vm_dict['accel']) > 0:
        if 'tokens_by_zones' in gpu_tokens:
            tokens_by_zones = gpu_tokens['tokens_by_zones']
        else:
            tokens_by_zones = {}
            gpu_tokens['tokens_by_zones'] = tokens_by_zones

        if vm_region in tokens_by_zones:
            region_token_pool = tokens_by_zones[vm_region]
        else:
            region_token_pool = {}
            tokens_by_zones[vm_region] = region_token_pool

        if 'gpu_tokens' in vm_dict:
            this_machine_tokens = vm_dict['gpu_tokens']
        else:
            this_machine_tokens = {}
            vm_dict['gpu_tokens'] = this_machine_tokens

        for accel_entry in vm_dict['accel']:
            need_tokens = accel_entry['count'] - _num_discount_tokens(this_machine_tokens)
            accel_type = accel_entry['type']

            for key in vm_dict['skus']['gpu_sku']:
                print("FIXME: multiple GPU types?")
                token_price = vm_dict['skus']['gpu_sku'][key][3]
                break

            print("FIXME!! we see two GPU token on a machine")
            #if vm_region != accel_entry['zone']:
            #    raise Exception("accel VM region mismatch {} {}".format(vm_region, accel_entry['zone']))
            allocate_tokens(need_tokens, vm_dict, machine_id, region_token_pool, uptime,
                            this_machine_tokens, month_uptime, initialize, 'gpu_tokens', accel_type, token_price)


    return

'''
----------------------------------------------------------------------------------------------
Allocate discount tokens:
'''

def allocate_tokens(need_tokens, vm_dict, machine_id, region_token_pool, uptime,
                    this_machine_tokens, month_uptime, initialize, token_type, machine_class, token_price):

    #
    # If running, this machine should have one token for each CPU. If it is running and has no tokens, generally
    # it must be new, and we need to get tokens from the pool. Exception is if we are just beginning to
    # inventory some time in first month. We still need token, but the whole month of uptime must be
    # applied to the token. If the pool is empty, create a new token
    #

    if vm_dict['status'] == "RUNNING":
        # The cpu count can be fractional, but not for n1, n2 machines (?):
        # RAM count can certainly be fractional!
        print("vm {} needs {} tokens".format(machine_id, need_tokens))
        # If we need tokens, get them:
        if need_tokens > 0:
            oldest_tokens = _get_oldest_tokens(need_tokens, region_token_pool, uptime, machine_class, token_price)
            for token in oldest_tokens:
                _add_token_to_machine(this_machine_tokens, token, uptime, month_uptime, initialize)
        else:
            vm_dict[token_type] = _age_discount_tokens(this_machine_tokens, uptime)
            this_machine_tokens = vm_dict[token_type]
            _swap_in_older_tokens(this_machine_tokens, region_token_pool, uptime, machine_class)

    else: #stopped
        vm_dict[token_type] = _reclaim_discount_tokens(this_machine_tokens, region_token_pool, uptime, machine_class)

    return

'''
----------------------------------------------------------------------------------------------
For a running VM, if there are older tokens that can be swapped in for younger ones, we want
to do that:
'''

def _swap_in_older_tokens(machine_dict, region_class_pool, uptime, machine_class):

    region_tokens, _ = _pull_token_dict_from_class_pool(region_class_pool, machine_class)

    # If the pool is empty, we are done and can leave:
    if len(region_tokens) == 0:
        return

    pool_int_keys = [int(x) for x in region_tokens.keys()]
    descend_pool_sorted_keys = sorted(pool_int_keys, reverse=True)

    machine_int_keys = [int(x) for x in machine_dict.keys()]
    machine_sorted_keys = sorted(machine_int_keys)

    # For each token for our machine, if there is an older one in the pool,
    # we swap it out

    print(machine_sorted_keys)
    print(descend_pool_sorted_keys)
    while machine_sorted_keys[0] < descend_pool_sorted_keys[0]:
        _swap_a_token(str(machine_sorted_keys[0]), str(descend_pool_sorted_keys[0]),
                      machine_dict, region_tokens, uptime, machine_class)
        machine_int_keys = [int(x) for x in machine_dict.keys()]
        machine_sorted_keys = sorted(machine_int_keys)
        pool_int_keys = [int(x) for x in region_tokens.keys()]
        descend_pool_sorted_keys = sorted(pool_int_keys, reverse=True)
        print(machine_sorted_keys)
        print(descend_pool_sorted_keys)

    return

'''
----------------------------------------------------------------------------------------------
Do a single token swap:
'''

def _swap_a_token(younger_machine_token, older_pool_token, machine_dict, region_token_pool, uptime, machine_class):

    print("doing token swap")
    #
    # Remove the older pool token:
    #

    print("rtp", region_token_pool)
    older_token = region_token_pool[older_pool_token].pop(0)
    print("older token key", older_pool_token)
    print("older token", older_token)
    if len(region_token_pool[older_pool_token]) == 0:
        region_token_pool.pop(older_pool_token)
    print("rtppo", region_token_pool)

    #
    # Remove the young machine token:
    #

    print("lmd", machine_dict)
    younger_token = machine_dict[younger_machine_token].pop(0)
    print("younger token key", younger_machine_token)
    print("younger token", younger_token)
    if len(machine_dict[younger_machine_token]) == 0:
        machine_dict.pop(younger_machine_token)
    print("lmdpo", machine_dict)

    #
    # Add older pool token to the machine:
    #

    _add_token_to_machine(machine_dict, older_token, uptime, None, False)
    print(len(machine_dict))

    #
    # Return the younger machine token to the pool:
    #

    print("younger token", younger_token)
    _update_token_use_for_return(younger_token)
    print("younger token post", younger_token)

    #_return_token_to_pool(region_token_pool, younger_token, machine_class)

    use_key = str(int(round(younger_token['prior_usage'])))
    if use_key in region_token_pool:
        region_token_pool[use_key].append(younger_token)
    else:
        region_token_pool[use_key] = [younger_token]
    return


'''
----------------------------------------------------------------------------------------------
Get the oldest tokens we can
'''

def _get_oldest_tokens(token_num, region_class_pool, uptime, machine_class, token_price):

    region_tokens, region_class = _pull_token_dict_from_class_pool(region_class_pool, machine_class)

    retval = []

    int_keys = [int(x) for x in region_tokens.keys()]
    descend_sorted_keys = sorted(int_keys, reverse=True)
    needed = token_num
    for key in descend_sorted_keys:
        free_tokens = region_tokens[str(key)]
        available = needed if len(free_tokens) >= needed else len(free_tokens)
        for i in range(available):
            retval.append(free_tokens.pop(0))
        if len(free_tokens) == 0:
            region_tokens.pop(str(key))
        needed -= available
        if needed == 0:
            break

    if needed > 0:
        next_num = region_class['next_num'] if 'next_num' in region_class else 0
        for i in range(needed):
            retval.append({'serial_num': next_num, 'class': machine_class, 'prior_usage': 0, 'current_usage': uptime, 'token_price': token_price})
            next_num += 1
        region_class["next_num"] = next_num

    return retval

'''
----------------------------------------------------------------------------------------------
Count discount tokens we hold
'''

def _num_discount_tokens(machine_dict):
    count = 0
    for time_key, token_list in machine_dict.items():
        count += len(token_list)
    return count

'''
----------------------------------------------------------------------------------------------
Age tokens held by a running machine
'''

def _age_discount_tokens(machine_dict, uptime):
    new_machine_dict = {}
    for time_key, token_list in machine_dict.items():
        for token in token_list:
            token['current_usage'] = uptime
            list_key = str(int(round(token['current_usage'] + token['prior_usage'])))
            if list_key in new_machine_dict:
                new_machine_dict[list_key].append(token)
            else:
                new_machine_dict[list_key] = [token]
    return new_machine_dict

'''
----------------------------------------------------------------------------------------------
Reclaim tokens from a stopped machine back into the pool
'''

def _reclaim_discount_tokens(machine_tokens, region_token_pool, uptime, machine_class):
    for use, token_list in machine_tokens.items():
        while len(token_list) > 0:
            token = token_list.pop(0)
            print("reclaim", token)
            _update_token_use_for_return(token)
            print("reclaim", token)
            print("rtp", region_token_pool)
            _return_token_to_pool(region_token_pool, token, machine_class)
            print("rtp", region_token_pool)

    return {}

'''
----------------------------------------------------------------------------------------------
Bring token up to date before returning it to the pool
'''

def _update_token_use_for_return(token):
    total_use = token['current_usage'] + token['prior_usage']
    token['current_usage'] = 0
    token['prior_usage'] = total_use
    return

'''
----------------------------------------------------------------------------------------------
Drill down to token dict from class pool
'''

def _pull_token_dict_from_class_pool(region_class_pool, machine_class):

    if machine_class in region_class_pool:
        region_class = region_class_pool[machine_class]
    else:
        region_class = {}
        region_class_pool[machine_class] = region_class

    if 'tokens' in region_class:
        region_tokens = region_class['tokens']
    else:
        region_tokens = {}
        region_class['tokens'] = region_tokens

    return region_tokens, region_class

'''
----------------------------------------------------------------------------------------------
Put a token back into the pool
'''

def _return_token_to_pool(region_class_pool, token, machine_class):

    region_tokens, _ = _pull_token_dict_from_class_pool(region_class_pool, machine_class)

    use_key = str(int(round(token['prior_usage'])))
    if use_key in region_tokens:
        region_tokens[use_key].append(token)
    else:
        region_tokens[use_key] = [token]
    return

'''
----------------------------------------------------------------------------------------------
Add token to machine. In intialization mode, the entire month's uptime is used. Otherwise, the
current uptime is used.
'''

def _add_token_to_machine(machine_dict, token, current_uptime, month_uptime, do_init):
    token['current_usage'] = month_uptime if do_init else current_uptime
    str_use_key = str(int(round(token['current_usage'] + token['prior_usage'])))
    if str_use_key in machine_dict:
        machine_dict[str_use_key].append(token)
    else:
        machine_dict[str_use_key] = [token]
    return

'''
----------------------------------------------------------------------------------------------
The sustained discount table:
'''

def _build_sustained_discount_table():

    # 730 hour month = 43800 minutes
    # 25% = 10950
    # 50% = 21900
    # 75% = 32850
    # 100% = 43800
    # n1: General-purpose N1 predefined and custom machine types, memory-optimized machine types,
    # shared-core machine types, and sole-tenant nodes (e.g. n1-standard-1)
    # n2: General-purpose N2 and N2D predefined and custom machine types, and Compute-optimized machine types
    # (e.g. c2-standard-4)
    # any_gpu: All GPU types are discounted on the same schedule as n1 types
    #

    discount_table = {
        'n1': [
            {'min_minutes_uptime': 0, 'max_minutes_uptime': 10949, 'multiplier' : 1.0}, # 0%-25%
            {'min_minutes_uptime': 10950, 'max_minutes_uptime': 21899, 'multiplier': 0.8}, # 25%-50%
            {'min_minutes_uptime': 21900, 'max_minutes_uptime': 32849, 'multiplier': 0.6}, # 50%-75%
            {'min_minutes_uptime': 32850, 'max_minutes_uptime': None, 'multiplier': 0.4} # 75%-100%
        ],
        'any_gpu': [
            {'min_minutes_uptime': 0, 'max_minutes_uptime': 10949, 'multiplier': 1.0},  # 0%-25%
            {'min_minutes_uptime': 10950, 'max_minutes_uptime': 21899, 'multiplier': 0.8},  # 25%-50%
            {'min_minutes_uptime': 21900, 'max_minutes_uptime': 32849, 'multiplier': 0.6},  # 50%-75%
            {'min_minutes_uptime': 32850, 'max_minutes_uptime': None, 'multiplier': 0.4}  # 75%-100%
        ],
        'n2': [
            {'min_minutes_uptime': 0, 'max_minutes_uptime': 10949, 'multiplier': 1.0},  # 0%-25%
            {'min_minutes_uptime': 10950, 'max_minutes_uptime': 21899, 'multiplier': 0.8678},  # 25%-50%
            {'min_minutes_uptime': 21900, 'max_minutes_uptime': 32849, 'multiplier': 0.733},  # 50%-75%
            {'min_minutes_uptime': 32850, 'max_minutes_uptime': None, 'multiplier': 0.6}  # 75%-100%
        ]
    }

    return discount_table

'''
----------------------------------------------------------------------------------------------
Convert a stored uptime record to a datetime pair
'''

def uptime_record_to_dt(str_record):
    retval = {
       'start': datetime.datetime.strptime(str_record['start'], '%Y-%m-%dT%H:%M:%S.%f%z'),
       'stop': datetime.datetime.strptime(str_record['stop'], '%Y-%m-%dT%H:%M:%S.%f%z')
    }
    return retval

'''
----------------------------------------------------------------------------------------------
Given an uptime record and a set hour, return the number of minutes up during that hour
'''

def uptime_record_to_minutes(dt_record, start_hour_dt, end_hour_dt):

    #
    # No overlap, no minutes:
    #

    if (end_hour_dt <= dt_record['start']) or (start_hour_dt >= dt_record['stop']):
        return 0

    #
    # Overlap. Four cases:
    #

    # hour is completely contained in the uptime:
    if (end_hour_dt <= dt_record['stop']) and (start_hour_dt >= dt_record['start']):
        return 60
    # Machine enters hour running, then shutdown before end of hour:
    elif (end_hour_dt >= dt_record['stop']) and (start_hour_dt >= dt_record['start']):
        return (dt_record['stop'] - start_hour_dt).total_seconds() / 60
    # Machine leaves hour running, and started before end of hour:
    elif (end_hour_dt <= dt_record['stop']) and (start_hour_dt <= dt_record['start']):
        return (end_hour_dt - dt_record['start']).total_seconds() / 60
    #  Machine up and down during hour:
    elif (end_hour_dt >= dt_record['stop']) and (start_hour_dt <= dt_record['start']):
        return (dt_record['stop'] - dt_record['start']).total_seconds() / 60
    else:
        print("ERROR fall through", dt_record, start_hour_dt, end_hour_dt)
        return 0

'''
----------------------------------------------------------------------------------------------
Fill in a time series
'''

def _fill_time_series_too(now_hour, now_time, vm_dict, max_history):


    last_start_record = vm_dict['last_start_uptime_record']
    last_stop_record = vm_dict['last_stop_uptime_record']
    last_stop_str = vm_dict['last_stop']
    start_str = vm_dict['started']
    uptime_records = vm_dict['uptime_records']

    uptime_records_as_dt = []
    for record in uptime_records:
        uptime_records_as_dt.append(uptime_record_to_dt(record))

    time_to_now = None # machine may be stopped
    if last_start_record is not None:
        time_to_now = {
            'start': datetime.datetime.strptime(last_start_record['start'], '%Y-%m-%dT%H:%M:%S.%f%z'),
            'stop': now_time
        }

    usage_hours = []
    for i in range(max_history):
        usage_hours.append((now_hour - datetime.timedelta(hours=i)).strftime('%Y-%m-%dT%H%z'))

    new_time_series = {}

    for hour_str in usage_hours:
        hour = datetime.datetime.strptime(hour_str, '%Y-%m-%dT%H%z')
        next_hour = hour + datetime.timedelta(hours=1)
        minutes_for_slot = 0
        for uptime_dt in uptime_records_as_dt:
            minutes_for_slot += uptime_record_to_minutes(uptime_dt, hour, next_hour)
        if time_to_now is not None:
            minutes_for_slot += uptime_record_to_minutes(time_to_now, hour, next_hour)
        new_time_series[hour_str] = minutes_for_slot

    return new_time_series

'''
----------------------------------------------------------------------------------------------
Fill in a token series
'''

def _fill_token_series(now_hour, now_time, start_time_str, old_time_series):
    #
    # We get sustained usage discounts. If a machine has tokens swapped, or is shutdown and loses its
    # tokens, then we need to know what tokens were used at each hour. So we record the token state at
    # each hour, if they are being used.
    #

    usage_hours = []
    for i in range(HISTORY):
        usage_hours.append((now_hour - datetime.timedelta(hours=i)).strftime('%Y-%m-%dT%H%z'))




    new_time_series = {}
    start_time = datetime.datetime.strptime(start_time_str, '%Y-%m-%dT%H:%M:%S.%f%z')
    for hour_str in usage_hours:
        hour = datetime.datetime.strptime(hour_str, '%Y-%m-%dT%H%z')
        next_hour = hour + datetime.timedelta(hours=1)
        if hour > start_time:
            if now_time > next_hour:
                new_time_series[hour_str] = 60
            else:
                new_time_series[hour_str] = (now_time - now_hour).total_seconds() / 60
        elif start_time < next_hour:
            if next_hour > now_time: # next hour in future, just started up:
                new_time_series[hour_str] = (now_time - start_time).total_seconds() / 60
            else: # next hour in past, need to get time from start to following hour:
                new_time_series[hour_str] = (next_hour - start_time).total_seconds() / 60
        elif hour in old_time_series:
            new_time_series[hour_str] = old_time_series[hour_str]
    return new_time_series






'''
----------------------------------------------------------------------------------------------
Fill in a time series
'''

def _fill_time_series(now_hour, now_time, start_time_str, old_time_series):
    #
    # For the VM/PD, fill in the runtime for hour slots back to the beginning of our series, or to the last
    # start time of the VM, whichever is later. Don't touch older slots, if they exist. Make it so we build
    # the whole series from scratch the first time, and also that we throw out old entries we don't care
    # about anymore:
    #

    usage_hours = []
    for i in range(HISTORY):
        usage_hours.append((now_hour - datetime.timedelta(hours=i)).strftime('%Y-%m-%dT%H%z'))

    new_time_series = {}
    start_time = datetime.datetime.strptime(start_time_str, '%Y-%m-%dT%H:%M:%S.%f%z')
    for hour_str in usage_hours:
        hour = datetime.datetime.strptime(hour_str, '%Y-%m-%dT%H%z')
        next_hour = hour + datetime.timedelta(hours=1)
        if hour > start_time:
            if now_time > next_hour:
                new_time_series[hour_str] = 60
            else:
                new_time_series[hour_str] = (now_time - now_hour).total_seconds() / 60
        elif start_time < next_hour:
            if next_hour > now_time: # next hour in future, just started up:
                new_time_series[hour_str] = (now_time - start_time).total_seconds() / 60
            else: # next hour in past, need to get time from start to following hour:
                new_time_series[hour_str] = (next_hour - start_time).total_seconds() / 60
        elif hour in old_time_series:
            new_time_series[hour_str] = old_time_series[hour_str]
    return new_time_series

'''
----------------------------------------------------------------------------------------------
Check if billing is enabled:
'''

def _check_billing(project_id, project_name):
    billing = discovery.build('cloudbilling', 'v1', cache_discovery=False)
    projects = billing.projects()
    billing_enabled = _is_billing_enabled(project_name, projects)
    return billing_enabled

'''
----------------------------------------------------------------------------------------------
Pull the billing account:
'''

def _pull_billing_account(project_id, project_name):
    billing = discovery.build('cloudbilling', 'v1', cache_discovery=False)

    projects = billing.projects()

    billing_enabled = _is_billing_enabled(project_name, projects)

    if billing_enabled:
        _disable_billing_for_project(project_id, project_name, projects)
    else:
        print('Project {} billing already disabled'.format(project_id))
    return

'''
----------------------------------------------------------------------------------------------
Check if billing is enabled on the project:
'''

def _is_billing_enabled(project_name, projects):
    try:
        res = projects.getBillingInfo(name=project_name).execute()
        billing_enabled = res['billingEnabled']
        return billing_enabled
    except KeyError:
        print("Check for billing enabled returns key error; billing not enabled.")
        # If billingEnabled isn't part of the return, billing is not enabled
        return False
    except Exception as e:
        print('Unable to determine if billing is enabled on specified project, assuming billing is enabled {}'.format(str(e)))
        return True

'''
----------------------------------------------------------------------------------------------
Do the actual pull of the billing account:
'''

def _disable_billing_for_project(project_id, project_name, projects):
    body = {'billingAccountName': ''}  # Disable billing
    try:
        res = projects.updateBillingInfo(name=project_name, body=body).execute()
        print('Project {} billing response: {}'.format(project_id, json.dumps(res)))
        print('Project {} billing disabled'.format(project_id))
    except Exception:
        print('Project {} failed to disable billing, possibly check permissions'.format(project_id))
    return

'''
----------------------------------------------------------------------------------------------
Determine if compute services are enabled:
'''
def _check_compute_services_for_project(project_id):
    service_u = discovery.build('serviceusage', 'v1', cache_discovery=False)

    s_request = service_u.services().get(name="projects/{}/services/compute.googleapis.com".format(project_id))
    response = s_request.execute()
    return response['state'] == "ENABLED"


'''
----------------------------------------------------------------------------------------------
Return information on all VMs in the project:
'''
def _check_compute_engine_inventory_aggregate(service, project_id):

    instance_dict = {}
    stopped_dict = {}
    type_dict = {}
    total_cpus = 0

    request = service.instances().aggregatedList(project=project_id)

    while request is not None:
        response = request.execute()
        for zone, instance_scoped_list in response['items'].items():
            if "warning" in instance_scoped_list and instance_scoped_list["warning"]["code"] == 'NO_RESULTS_ON_PAGE':
                continue
            for instance in instance_scoped_list['instances']:
                total_cpus += _describe_instance(project_id, zone.split('/')[-1], service,
                                                 instance, instance_dict, type_dict, stopped_dict)
        request = service.instances().aggregatedList_next(previous_request=request, previous_response=response)

    return instance_dict, type_dict, total_cpus, stopped_dict

'''
----------------------------------------------------------------------------------------------
# Describe an instance:
'''
def _describe_instance(project_id, zone, compute, item, instance_dict, type_dict, stopped_dict):

    cpu_count = 0

    accel_inventory = []
    if 'guestAccelerators' in item:
        for accel in item['guestAccelerators']:
            type_chunks = accel['acceleratorType'].split('/')
            accel_entry = {
                'count': int(accel['acceleratorCount']),
                'zone': type_chunks[-3],
                'type': type_chunks[-1]
            }
            print("Accelerator: %i %s %s" % (accel_entry['count'], accel_entry['zone'], accel_entry['type']))
            accel_inventory.append(accel_entry)

    machine_key = item['machineType'].split('/')[-1]
    instance_info = {'zone': zone,
                     'id': item['id'],
                     'name': item['name'],
                     'status': item['status'],
                     'created': item['creationTimestamp'],
                     'started': item['lastStartTimestamp'],
                     'lastStop': item['lastStopTimestamp'] if 'lastStopTimestamp' in item else None,
                     'machineType': machine_key,
                     'firstDiskSize': item['disks'][0]['diskSizeGb'],
                     'firstDiskKind': item['disks'][0]['kind'],
                     'firstDiskFirstLicense': item['disks'][0]['licenses'][0].split('/')[-1],
                     'preemptible': item['scheduling']['preemptible'],
                     'accelerator_inventory': accel_inventory,
                     'cpus': 0.0
                     }

    full_key = (zone, machine_key)
    if full_key not in type_dict:
        request = compute.machineTypes().get(project=project_id, zone=zone, machineType=instance_info['machineType'])
        response = request.execute()
        machine_info = {'name': response['name'],
                        'cpus': float(response['guestCpus']),
                        'memory': int(response['memoryMb']),
                        'zone': response['zone']
                       }
        type_dict[full_key] = machine_info
        instance_info['cpus'] = float(response['guestCpus'])
    else:
        instance_info['cpus'] = type_dict[full_key]['cpus']

    full_key = (zone, machine_key, instance_info['cpus'], instance_info['name'], instance_info['id'])

    if item['status'] == 'RUNNING':
        cpu_count += instance_info['cpus']
        instance_dict[full_key] = instance_info
    else: # machine is not running:
        stopped_dict[full_key] = instance_info
    return cpu_count

'''
----------------------------------------------------------------------------------------------
Prepare the list of VMs to shut down:
'''

def _prepare_shutdown_list(compute_inventory, stop_count=None):
    by_stamp = {}
    for k, v in compute_inventory.items():
        result = datetime.datetime.strptime(v['started'], '%Y-%m-%dT%H:%M:%S.%f%z')
        if result not in by_stamp:
            tuples_for_stamp = []
            by_stamp[result] = tuples_for_stamp
        else:
            tuples_for_stamp = by_stamp[result]
        tuples_for_stamp.append(k)

    count = 0
    retval = []
    for started in sorted(by_stamp.keys(), reverse=True):
        for_stamp = by_stamp[started]
        for machine_tuple in for_stamp:
            retval.append(machine_tuple)
            count += machine_tuple[2]
            if stop_count is not None and count > stop_count:
                break
        if stop_count is not None and count > stop_count:
            break

    return retval

'''
----------------------------------------------------------------------------------------------
Stop the instances that need to be shut down:
'''

def _process_shutdown_list(project_id, stop_list):

    if len(stop_list) > 0:
        names_by_zone = {}
        for zone_and_name in stop_list:
            zone = zone_and_name[0]
            if zone not in names_by_zone:
                names_in_zone = []
                names_by_zone[zone] = names_in_zone
            else:
                names_in_zone = names_by_zone[zone]
            names_in_zone.append(zone_and_name[3])

        compute = discovery.build('compute', 'v1', cache_discovery=False)
        instances = compute.instances()
        for zone, names in names_by_zone.items():
            _stop_instances(project_id, zone, names, instances)
    return


'''
----------------------------------------------------------------------------------------------
Do the actual stop call:
'''

def _stop_instances(project_id, zone, instance_names, instances):

    if not len(instance_names):
        print('No running instances were found in zone {}.'.format(zone))
        return

    for name in instance_names:
        instances.stop(project=project_id, zone=zone, instance=name).execute()
        print('Instance stopped successfully: {}'.format(name))

    return

'''
----------------------------------------------------------------------------------------------
Find out app engine status:
'''

def _check_appengine(project_id):

    appengine = discovery.build('appengine', 'v1', cache_discovery=False)
    apps = appengine.apps()

    # Get the target app's serving status
    try:
        target_app = apps.get(appsId=project_id).execute()
    except HttpError as e:
        if e.resp.status == 404:
            print('Project {} does not have App Engine enabled'.format(project_id))
            return
        else:
            raise e

    current_status = target_app['servingStatus']

    print('Project {} App Engine status {}'.format(project_id, str(current_status)))

    return current_status == "SERVING"

'''
----------------------------------------------------------------------------------------------
Turn off app engine:
'''

def _turn_off_appengine(project_id):

    appengine = discovery.build('appengine', 'v1', cache_discovery=False)
    apps = appengine.apps()

    # Get the target app's serving status
    target_app = apps.get(appsId=project_id).execute()
    current_status = target_app['servingStatus']

    # Disable target app, if necessary
    if current_status == 'SERVING':
        print('Attempting to disable app {}...'.format(project_id))
        body = {'servingStatus': 'USER_DISABLED'}
        apps.patch(appsId=project_id, updateMask='serving_status', body=body).execute()

    return

'''
----------------------------------------------------------------------------------------------
Figure out if we are spending before going through extensive calculations
'''

def are_we_spending(vm_cost_breakdown, disks_cost_breakdown):

    total = 0.0
    for val in vm_cost_breakdown.values():
        total += val
    for val in disks_cost_breakdown.values():
        total += val
    return total > 0.0

'''
----------------------------------------------------------------------------------------------
Gather up the SKU info for a VM
'''

def _gather_vm_skus(machine_info, vm_pricing_cache, gpu_pricing_cache, vm_dict):

    machine_type_only = '-'.join(vm_dict["machine_type"].split('-')[0:2])
    key = (machine_type_only, vm_dict['region'], vm_dict["preempt"])

    if key in vm_pricing_cache:
        price_lineitem = vm_pricing_cache[key]
    else:
        price_lineitem = _pull_vm_pricing(machine_type_only, vm_dict["preempt"], vm_dict['region'], CPU_PRICES, RAM_PRICES, KEY_MAP)
        if price_lineitem is not None:
            vm_pricing_cache[key] = price_lineitem
        else:
            raise Exception("No VM pricing found")

    vm_dict['cpus_per_core'] = price_lineitem[9]
    vm_dict["skus"]["vm_sku"] = {price_lineitem[7]: price_lineitem}
    vm_dict["skus"]["ram_sku"] = {price_lineitem[8]: price_lineitem}

    all_accel = {}

    for accel_entry in machine_info['accelerator_inventory']:
        gpu_key = "{}/{}/{}".format(accel_entry['type'], vm_dict['region'], vm_dict["preempt"])
        if gpu_key in gpu_pricing_cache:
            gpu_price_lineitem = gpu_pricing_cache[gpu_key]
        else:
            gpu_price_lineitem = _pull_gpu_pricing(accel_entry['type'], vm_dict["preempt"], vm_dict['region'], GPU_PRICES, GPU_KEY_MAP)
            if gpu_price_lineitem is not None:
                gpu_pricing_cache[gpu_key] = gpu_price_lineitem
            else:
                raise Exception("GPU pricing not found: {}".format(gpu_key))
        if gpu_key not in all_accel:
            all_accel[gpu_key] = gpu_price_lineitem

    vm_dict["skus"]["gpu_sku"] = all_accel
    return

'''
----------------------------------------------------------------------------------------------
Get the license SKU info for a machine:
'''

def _gather_license_skus(machine_info,
                         cpu_lic_pricing_cache, gpu_lic_pricing_cache, ram_lic_pricing_cache,
                         vm_dict):

    license_name = machine_info['firstDiskFirstLicense']
    num_gpu = len(machine_info['accelerator_inventory'])
    num_cpu = machine_info['cpus']

    #
    # Licensing depends on the OS. It can vary by the number of CPUs, the size of RAM, and the number of GPUs.
    # Note that the pricing is NOT regional.
    #

    machine_class = machine_info['machineType']
    special_class = machine_class if (machine_class == "f1-micro") or (machine_class == "g1-small") else None

    gpu_key = (license_name, num_gpu)
    ram_key = (license_name,)
    cpu_key = (license_name, special_class, num_cpu)

    if cpu_key in cpu_lic_pricing_cache:
        cpu_price_lineitem = cpu_lic_pricing_cache[cpu_key]
    else:
        cpu_price_lineitem = _pull_cpu_lic_pricing(license_name, special_class, num_cpu, CPU_LIC_PRICES, LIC_KEY_MAP)
        if cpu_price_lineitem is not None:
            cpu_lic_pricing_cache[cpu_key] = cpu_price_lineitem
        else:
            raise Exception("No CPU license pricing found")

    vm_dict["skus"]["cpu_os_lic_sku"] = {cpu_price_lineitem[3]: cpu_price_lineitem}

    if num_gpu > 0:
        if gpu_key in gpu_lic_pricing_cache:
            gpu_price_lineitem = gpu_lic_pricing_cache[gpu_key]
        else:
            gpu_price_lineitem = _pull_gpu_lic_pricing(license_name, num_gpu, GPU_LIC_PRICES, LIC_KEY_MAP)
            if gpu_price_lineitem is not None:
                gpu_lic_pricing_cache[cpu_key] = gpu_price_lineitem
            else:
                raise Exception("No GPU license pricing found")

        vm_dict["skus"]["gpu_os_lic_sku"] = {gpu_price_lineitem[3]: gpu_price_lineitem}

    else:
        vm_dict["skus"]["gpu_os_lic_sku"] = None

    if ram_key in ram_lic_pricing_cache:
        ram_price_lineitem = ram_lic_pricing_cache[gpu_key]
    else:
        ram_price_lineitem = _pull_ram_lic_pricing(license_name, RAM_LIC_PRICES, LIC_KEY_MAP)
        if ram_price_lineitem is not None:
            ram_lic_pricing_cache[cpu_key] = ram_price_lineitem
        else:
            raise Exception("No RAM license pricing found")

    vm_dict["skus"]["ram_os_lic_sku"] = {ram_price_lineitem[3]: ram_price_lineitem}

    return

'''
----------------------------------------------------------------------------------------------
Get disk sku info:
'''

def _gather_disk_sku(disk, disk_pricing_cache, pd_dict):

    disk_type = disk['type']
    disk_zone = disk['zone']
    is_regional = disk['is_regional']
    region = '-'.join(disk_zone.split('-')[0:-1]) if not is_regional else disk['disk_region']

    pd_key = (disk_type, region, is_regional)
    if pd_key in disk_pricing_cache:
        pd_price_lineitem = disk_pricing_cache[pd_key]
    else:
        pd_price_lineitem = _pull_pd_pricing(disk_type, str(is_regional), region, PD_PRICES, PD_KEY_MAP)
        if pd_price_lineitem is not None:
            disk_pricing_cache[pd_key] = pd_price_lineitem
        else:
            raise Exception("No PD sku found")

    pd_dict["pd_sku"] = {pd_price_lineitem[5]: pd_price_lineitem}
    return

'''
----------------------------------------------------------------------------------------------
Find the appropriate sustained use discount:
'''

def _get_sustained_discount_for_minutes(minutes, machine_type, discount_tables):

    machine_class = machine_type.split('-')[0]

    if machine_class not in discount_tables:
        return 1.0

    numerator = 0.0
    denominator = 0.0
    for candidate in discount_tables[machine_class]:
        if candidate['max_minutes_uptime'] is None:
            if minutes > candidate['min_minutes_uptime']:
                delta_min = minutes - candidate['min_minutes_uptime']
                numerator += delta_min * candidate['multiplier']
                denominator += delta_min
        elif minutes > candidate['max_minutes_uptime']:
            delta_min = candidate['max_minutes_uptime'] - candidate['min_minutes_uptime']
            numerator += delta_min * candidate['multiplier']
            denominator += delta_min
        elif (minutes > candidate['min_minutes_uptime']) and (minutes < candidate['max_minutes_uptime']):
            delta_min = minutes - candidate['min_minutes_uptime']
            numerator += delta_min * candidate['multiplier']
            denominator += delta_min

    net_multiplier = numerator / denominator
    return net_multiplier


'''
----------------------------------------------------------------------------------------------
How many hours of sustained discount on this machine?
'''

def _add_machine_resource_tokens(vm_dict, resource_key):

    minutes = 0.0
    num_tok = 0
    tok_price = None
    if resource_key not in vm_dict:
        return minutes, num_tok, tok_price
    cpu_tokens = vm_dict[resource_key]
    for key, val in cpu_tokens.items():
        for tok in val:
            minutes += (tok['current_usage'] + tok['prior_usage'])
            num_tok += 1
            if tok_price is None:
                tok_price = tok['token_price']
            elif tok_price != tok['token_price']:
                raise Exception("Token price mismatch {} {}".format(tok_price, tok['token_price']))
    return minutes, num_tok, tok_price

'''
----------------------------------------------------------------------------------------------
Compute the cost of the given machine over the given number of hours:
'''

def _core_and_ram_and_gpu_cost(hours, vm_dict, cost_per_sku):

    cpu_price_lineitem = None
    if vm_dict["skus"]["vm_sku"] is None:
        print("machine", vm_dict['id'], "is None")
        return
    for k, v in vm_dict["skus"]["vm_sku"].items():
        if cpu_price_lineitem is None:
            cpu_price_lineitem = v
        else:
            print("Multiple CPU SKUs")
            break

    ram_price_lineitem = None
    for k, v in vm_dict["skus"]["ram_sku"].items():
        if ram_price_lineitem is None:
            ram_price_lineitem = v
        else:
            print("Multiple RAM SKUs")
            break

    if 'cpu_discounts_per_hour' in vm_dict: # FIXME DEV ONLY
        vm_dict.pop('cpu_discounts_per_hour')

    core_per_hour = 0.0
    gb_per_hour = 0.0
    no_calc = False
    if cpu_price_lineitem[4] == "HOUR":
        core_per_hour = cpu_price_lineitem[3]
    else:
        no_calc = True

    if ram_price_lineitem[6] is None:
        gb_per_hour = 0.0
    elif ram_price_lineitem[6] == "GIBIBYTE_HOUR":
        gb_per_hour = ram_price_lineitem[5]
    else:
        no_calc = True
    if not no_calc:
        #
        # Originally multiplied by cpu count. But then when testing on e2-medium machines, that gave the
        # wrong answer. It looked like the CPU count (2 CPUs) was not applicable. So I dropped
        # that factor. But that was wrong! The e2-micro, e2-small, and e2-medium machines all have 2 CPUs,
        # but a different number of CPUs "per core", and pricing is actually by core. So, there is now a
        # BQ table field for "cpus per core", and that needs to be used as well as CPU count.
        #

        cpu_sku = cpu_price_lineitem[7]
        this_cpu_sku = hours * core_per_hour * (vm_dict['cpus'] / vm_dict['cpus_per_core'])
        #
        # This is the running total across ALL machines in the machine class:
        #
        cost_per_sku[cpu_sku] = (this_cpu_sku + cost_per_sku[cpu_sku]) if (cpu_sku in cost_per_sku) else this_cpu_sku

        ram_sku = cpu_price_lineitem[8]
        this_ram_sku = hours * vm_dict['memory_gb'] * gb_per_hour

    else:
        raise Exception("unexpected pricing units: {} {}".format(cpu_price_lineitem[4], ram_price_lineitem[6]))

    #
    # Now do the GPUs:
    #

    all_accel = vm_dict["skus"]["gpu_sku"]
    accel_cost = 0.0
    for accel_entry in vm_dict["accel"]:
        gpu_key = "{}/{}/{}".format(accel_entry['type'], vm_dict['region'], vm_dict["preempt"])
        gpu_price_lineitem = all_accel[gpu_key]
        if gpu_price_lineitem is not None:
            if gpu_price_lineitem[4] == "HOUR":
                accel_per_hour = gpu_price_lineitem[3]
                this_sku = hours * accel_per_hour * accel_entry['count']
                if gpu_price_lineitem[5] in cost_per_sku:
                    this_sku += cost_per_sku[gpu_price_lineitem[5]]
                cost_per_sku[gpu_price_lineitem[5]] = this_sku
                accel_cost += this_sku
            else:
                raise Exception("unexpected pricing units: {}".format(gpu_price_lineitem[4]))
        else:
            raise Exception("gpu key not present: {}".format(gpu_key))

'''
----------------------------------------------------------------------------------------------
Compute uptime of the given machine:
'''

def _machine_uptime(machine_info):

    #
    # For each machine, calculate the time it has been up since started.
    #
    start_time = machine_info['started']
    if ":" == start_time[-3:-2]:
        start_time = start_time[:-3] + start_time[-2:]
    start_time_obj = datetime.datetime.strptime(start_time, '%Y-%m-%dT%H:%M:%S.%f%z')


    last_stop = machine_info['lastStop']
    if last_stop is not None:
        if ":" == last_stop[-3:-2]:
            last_stop = last_stop[:-3] + last_stop[-2:]
        last_stop_obj = datetime.datetime.strptime(last_stop, '%Y-%m-%dT%H:%M:%S.%f%z')

    now_time = datetime.datetime.now(datetime.timezone.utc)
    uptime = now_time - start_time_obj

    uptime_hours = uptime.total_seconds() / (60 * 60)
    return uptime_hours

'''
----------------------------------------------------------------------------------------------
Compute the license cost of the given machine:
'''

def _calc_licensing_cost(hours, vm_dict, cost_per_sku):

    cpu_lic_price_lineitem = None
    if vm_dict["skus"]["vm_sku"] is None:
        print("machine", vm_dict['id'], "is None")
        return

    for k, v in vm_dict["skus"]["cpu_os_lic_sku"].items():
        if cpu_lic_price_lineitem is None:
            cpu_lic_price_lineitem = v
        else:
            print("Multiple CPU licensing SKUs")
            break

    gpu_lic_price_lineitem = None
    if vm_dict["skus"]["gpu_os_lic_sku"] is not None:
        for k, v in vm_dict["skus"]["gpu_os_lic_sku"].items():
            if gpu_lic_price_lineitem is None:
                gpu_lic_price_lineitem = v
            else:
                print("Multiple GPU licensing SKUs")
                break

    ram_lic_price_lineitem = None
    for k, v in vm_dict["skus"]["ram_os_lic_sku"].items():
        if ram_lic_price_lineitem is None:
            ram_lic_price_lineitem = v
        else:
            print("Multiple ram licensing SKUs")
            break

    no_calc = False
    cpu_lic_per_hour = 0.0
    gpu_lic_per_hour = 0.0
    ram_lic_per_hour_per_gib = 0.0

    if cpu_lic_price_lineitem[2] == "HOUR":
        cpu_lic_per_hour = cpu_lic_price_lineitem[1]
    else:
        no_calc = True

    if gpu_lic_price_lineitem is not None:
        if gpu_lic_price_lineitem[2] == "HOUR":
            gpu_lic_per_hour = gpu_lic_price_lineitem[1]
        else:
            no_calc = True
        gpu_units = gpu_lic_price_lineitem[2]
    else:
        gpu_lic_per_hour = 0.0
        gpu_units = "NO UNITS"

    if ram_lic_price_lineitem[2] == "GIBIBYTE_HOUR":
        ram_lic_per_hour_per_gib = ram_lic_price_lineitem[1]
    else:
        no_calc = True

    if not no_calc:
        this_sku = cpu_lic_per_hour * hours
        if cpu_lic_price_lineitem[3] in cost_per_sku:
            this_sku += cost_per_sku[cpu_lic_price_lineitem[3]]
        cost_per_sku[cpu_lic_price_lineitem[3]] = this_sku

        if gpu_lic_price_lineitem is not None:
            this_sku = gpu_lic_per_hour * hours
            if gpu_lic_price_lineitem[3] in cost_per_sku:
                this_sku += cost_per_sku[gpu_lic_price_lineitem[3]]
            cost_per_sku[gpu_lic_price_lineitem[3]] = this_sku

        this_sku = ram_lic_per_hour_per_gib * hours * vm_dict["memory_gb"]
        if ram_lic_price_lineitem[3] in cost_per_sku:
            this_sku += cost_per_sku[ram_lic_price_lineitem[3]]
        cost_per_sku[ram_lic_price_lineitem[3]] = this_sku

        license_cost = (cpu_lic_per_hour * hours) + (gpu_lic_per_hour * hours) + (vm_dict["memory_gb"] * ram_lic_per_hour_per_gib * hours)
    else:
        raise Exception("Unexpected pricing units {} {} {}".format(cpu_lic_price_lineitem[2],
                                                                   gpu_units,
                                                                   ram_lic_price_lineitem[2]))
    return license_cost


'''
----------------------------------------------------------------------------------------------
Compute the cost of the given disk:
'''
def _calc_disk_cost(hours, pd_dict, cost_per_sku):

    pd_price_lineitem = None
    for k, v in pd_dict["pd_sku"].items():
        if pd_price_lineitem is None:
            pd_price_lineitem = v
        else:
            print("Multiple PD SKUs")
            break

    no_calc = False
    pd_per_month = 0.0

    if pd_price_lineitem[4] == "GIBIBYTE_MONTH":
        pd_per_month = pd_price_lineitem[3]
    else:
        no_calc = True

    if not no_calc:
        this_sku = pd_per_month * (hours / 730.0) * pd_dict["gb_size"]
        if pd_price_lineitem[5] in cost_per_sku:
            this_sku += cost_per_sku[pd_price_lineitem[5]]
        cost_per_sku[pd_price_lineitem[5]] = this_sku

    else:
        raise Exception("unexpected pricing units {}".format(pd_price_lineitem[4]))

    return pd_per_month * (hours / 730) * pd_dict["gb_size"]


'''
----------------------------------------------------------------------------------------------
Compute the cost of the given disk:
'''
def _disk_cost(disk, disk_pricing_cache, pd_dict):
    #
    # For each disk, calculate the time it has been up since started.
    #
    start_time_obj = disk['created']

    now_time = datetime.datetime.now(datetime.timezone.utc)
    uptime = now_time - start_time_obj

    month_dict = {}
    uptime_hours = uptime.total_seconds() / (60 * 60)
    month_dict['uptime'] = (uptime_hours / 24.0) / 30.0
    month_dict['last_day'] = ((24.0 if uptime_hours > 24 else uptime_hours) / 24.0) / 30.0
    month_dict['hourly'] = (1.0 / 24) / 30

    memory_gb = disk['size']
    disk_type = disk['type']
    disk_zone = disk['zone']
    is_regional = disk['is_regional']
    region = '-'.join(disk_zone.split('-')[0:-1]) if not is_regional else disk['disk_region']

    cost_breakdown = {}
    for interval, months in month_dict.items():
        disk_cost = 0.0
        pd_key = (disk_type, region, is_regional)
        if pd_key in disk_pricing_cache:
            pd_price_lineitem = disk_pricing_cache[pd_key]
        else:
            pd_price_lineitem = _pull_pd_pricing(disk_type, str(is_regional), region, PD_PRICES, PD_KEY_MAP)
            if pd_price_lineitem is not None:
                disk_pricing_cache[pd_key] = pd_price_lineitem

        if pd_price_lineitem is not None:
            if pd_price_lineitem[4] == "GIBIBYTE_MONTH":
                pd_per_month = pd_price_lineitem[3]
                disk_cost = months * pd_per_month * memory_gb
            else:
                raise Exception("unexpected pricing units {}".format(pd_price_lineitem[4]))
        else:
            raise Exception("pd key not present {}".format(pd_key))

        pd_dict["pd_sku"] = {pd_price_lineitem[5]: pd_price_lineitem}

        cost_breakdown[interval] = disk_cost

    return cost_breakdown

'''
----------------------------------------------------------------------------------------------
Return information on all persistent disks in the project:
'''
def _check_persistent_disk_inventory_aggregate(service, project_id):
    disk_list = []
    request = service.disks().aggregatedList(project=project_id)
    while request is not None:
        response = request.execute()
        for zone, disks_scoped_list in response['items'].items():
            if "warning" in disks_scoped_list and disks_scoped_list["warning"]["code"] == 'NO_RESULTS_ON_PAGE':
                continue
            disk_list_per_zone = disks_scoped_list['disks']
            for disk in disk_list_per_zone:
                is_regional = False
                #
                # There can be "regional" disks, that are replicated between two zones. They are charged
                # differently, so we need to figure this out
                #
                disk_region = None
                if 'region' in disk and 'replicaZones' in disk and len(disk['replicaZones']) > 1:
                    is_regional = True
                    disk_region = disk['region'].split('/')[-1]
                    print("regional disk %s appears in zone %s" % (disk['region'], zone))
                disk_list.append({'id': disk['id'],
                                  'zone': zone.split('/')[-1],
                                  'name': disk['name'],
                                  'created': datetime.datetime.strptime(disk['creationTimestamp'], '%Y-%m-%dT%H:%M:%S.%f%z'),
                                  'size': int(disk['sizeGb']),
                                  'is_regional': is_regional,
                                  'disk_region': disk_region,
                                  'type': disk['type'].split('/')[-1]})
        request = service.disks().aggregatedList_next(previous_request=request, previous_response=response)
    return disk_list



'''
----------------------------------------------------------------------------------------------
Get the pricing for the vm type:
'''
def _pull_vm_pricing(machine_key, preemptible, region, cpu_table_name, ram_table_name, key_mapping):
    sql = _machine_cost_sql(machine_key, preemptible, region, cpu_table_name, ram_table_name, key_mapping)
    results = _bq_harness_with_result(sql, False)
    vm_costs = None
    if results is not None:
        for row in results:
            if vm_costs is None:
                vm_costs = (row.machine_key, row.preemptible, row.region,
                            row.cpu_max_usd, row.cpu_pricing_unit, row.ram_max_usd, row.ram_pricing_unit,
                            row.cpu_sku_id, row.ram_sku_id, row.cpus_per_core)
            else:
                print(row)
                print("Too many vm cost results")
                #raise Exception("Too many vm cost results")
    return vm_costs

'''
----------------------------------------------------------------------------------------------
Machine Cost SQL
'''
def _machine_cost_sql(machine_key, preemptible, region, cpu_table_name, ram_table_name, key_mapping):

    sql = '''
         WITH a1 as (SELECT
          machine_key,
          mk2c.cpu as cpu,
          mk2c.ram as ram,
          mk2c.cpus_per_core as cpus_per_core,
          cpup.preemptible,
          cpup.region,
          cpup.sku_id,
          cpup.max_usd,
          cpup.min_usd,
          cpup.pricing_unit
          FROM `{5}` as mk2c
          JOIN `{3}` as cpup
          ON mk2c.cpu = cpup.cpu
        WHERE cpup.region = "{2}"
        AND mk2c.machine_key = "{0}"
        AND cpup.preemptible = {1})
        SELECT a1.machine_key,
               a1.cpu,
               a1.ram,
               a1.cpus_per_core,
               a1.preemptible,
               a1.region,
               a1.sku_id as cpu_sku_id,
               a1.max_usd as cpu_max_usd,
               a1.min_usd as cpu_min_usd,
               a1.pricing_unit as cpu_pricing_unit,
               ramp.sku_id as ram_sku_id,
               ramp.max_usd as ram_max_usd,
               ramp.min_usd as ram_min_usd,
               ramp.pricing_unit as ram_pricing_unit
        FROM a1
        LEFT JOIN `{4}` as ramp
        ON a1.ram = ramp.cpu
        WHERE (ramp.region = "{2}" OR ramp.region IS NULL)
        AND a1.machine_key = "{0}"
        AND (ramp.preemptible = {1} OR ramp.preemptible IS NULL)
        '''.format(machine_key, str(preemptible), region, cpu_table_name, ram_table_name, key_mapping)
    return sql


'''
----------------------------------------------------------------------------------------------
Get the pricing for the gpu type:
'''
def _pull_gpu_pricing(gpu_key, preemptible, region, gpu_table_name, key_mapping):
    sql = _gpu_cost_sql(gpu_key, preemptible, region, gpu_table_name, key_mapping)
    results = _bq_harness_with_result(sql, False)
    gpu_pricing = None
    if results is not None:
        for row in results:
            if gpu_pricing is None:
                gpu_pricing = (row.gpu_key, row.preemptible, row.region, row.max_usd, row.pricing_unit, row.sku_id)
            else:
                print(row)
                print("Too many gpu cost results")
                #raise Exception("Too many gpu cost results")
    return gpu_pricing

'''
----------------------------------------------------------------------------------------------
GPU Cost SQL
'''

def _gpu_cost_sql(gpu_key, preemptible, region, gpu_table_name, key_mapping):

    sql = '''
        SELECT
            gpu_key,
            gk2g.gpu as gpu,
            gpup.preemptible,
            gpup.region,
            gpup.sku_id,
            gpup.max_usd,
            gpup.min_usd,
            gpup.pricing_unit
        FROM `{4}` as gk2g
        JOIN `{3}` as gpup
        ON gk2g.gpu = gpup.gpu
        WHERE gpup.region = "{2}"
        AND gk2g.gpu_key = "{0}"
        AND gpup.preemptible = {1}
        '''.format(gpu_key, str(preemptible), region, gpu_table_name, key_mapping)
    return sql

'''
----------------------------------------------------------------------------------------------
Get the pricing for the pd type:
'''
def _pull_pd_pricing(pd_key, is_regional, region, pd_table_name, pd_key_mapping):
    sql = _pd_cost_sql(pd_key, is_regional, region, pd_table_name, pd_key_mapping)
    results = _bq_harness_with_result(sql, False)
    pd_pricing = None
    if results is not None:
        for row in results:
            if pd_pricing is None:
                pd_pricing = (row.pd_key, row.is_regional, row.region, row.max_usd, row.pricing_unit, row.sku_id)
            else:
                print(row)
                print("Too many pd cost results")
                #raise Exception("Too many pd cost results")
    return pd_pricing

'''
----------------------------------------------------------------------------------------------
PD Cost SQL
'''

def _pd_cost_sql(pd_key, is_regional, region, pd_table_name, key_mapping):

    sql = '''
        SELECT
            pd_key,
            pk2p.pd as pd,
            pdp.is_regional,
            pdp.region,
            pdp.sku_id,
            pdp.max_usd,
            pdp.min_usd,
            pdp.pricing_unit
        FROM `{4}` as pk2p
        JOIN `{3}` as pdp
        ON pk2p.pd = pdp.pd
        WHERE pdp.region = "{2}"
        AND pdp.is_regional = {1}
        AND pk2p.pd_key = "{0}"
        '''.format(pd_key, str(is_regional), region, pd_table_name, key_mapping)
    return sql


'''
----------------------------------------------------------------------------------------------
Get the pricing for the os license:
'''
def _pull_cpu_lic_pricing(os_key, special_class, cpu_count, cpu_lic_table_name, lic_key_mapping):
    sql = _cpu_lic_cost_sql(os_key, special_class, cpu_count, cpu_lic_table_name, lic_key_mapping)
    results = _bq_harness_with_result(sql, False)
    cpul_pricing = None
    if results is not None:
        for row in results:
            if cpul_pricing is None:
                cpul_pricing = (row.lic_key, row.max_usd, row.pricing_unit, row.sku_id)
            else:
                print(row)
                print("Too many cpu lic cost results")
                #raise Exception("Too many cpu license cost results")
    return cpul_pricing

'''
----------------------------------------------------------------------------------------------
SQL for above
'''

def _cpu_lic_cost_sql(os_key, special_class, cpu_count, cpu_lic_table_name, lic_key_mapping):

    class_test = 'licp.machine_class IS NULL' if special_class is None else 'licp.machine_class = "{}"'.format(special_class)
    sql = '''
        SELECT
            licp.machine_class,
            licp.lic_key,
            lk2l.vm_lic_key,
            lk2l.vm_license,
            licp.sku_id,
            licp.min_cpu,
            licp.max_cpu,
            licp.min_usd,
            licp.max_usd,
            licp.pricing_unit
        FROM `{4}` as lk2l
        JOIN `{3}` as licp
        ON lk2l.vm_license = licp.lic_key
        WHERE {1}
        AND (licp.max_cpu is NULL OR licp.max_cpu >= {2})
        AND (licp.min_cpu is NULL OR licp.min_cpu <= {2})
        AND lk2l.vm_lic_key = "{0}"
        '''.format(os_key, class_test, cpu_count, cpu_lic_table_name, lic_key_mapping)
    return sql

'''
----------------------------------------------------------------------------------------------
Get the pricing for the os license for the GPU:
'''
def _pull_gpu_lic_pricing(os_key, gpu_count, gpu_lic_table_name, lic_key_mapping):
    sql = _gpu_lic_cost_sql(os_key, gpu_count, gpu_lic_table_name, lic_key_mapping)
    results = _bq_harness_with_result(sql, False)
    gpul_pricing = None
    if results is not None:
        for row in results:
            if gpul_pricing is None:
                gpul_pricing = (row.lic_key, row.max_usd, row.pricing_unit, row.sku_id)
            else:
                print(row)
                print("Too many gpu lic cost results")
                #raise Exception("Too many gpu license cost results")
    return gpul_pricing

'''
----------------------------------------------------------------------------------------------
SQL for above
'''

def _gpu_lic_cost_sql(os_key, gpu_count, gpu_lic_table_name, lic_key_mapping):

    sql = '''
        SELECT
            licp.lic_key,
            lk2l.vm_lic_key,
            lk2l.vm_license,
            licp.sku_id,
            licp.min_gpu,
            licp.max_gpu,
            licp.min_usd,
            licp.max_usd,
            licp.pricing_unit
        FROM `{3}` as lk2l
        JOIN `{2}` as licp
        ON lk2l.vm_license = licp.lic_key
        WHERE (licp.max_gpu is NULL OR licp.max_gpu >= {1})
        AND (licp.min_gpu is NULL OR licp.min_gpu <= {1})
        AND lk2l.vm_lic_key = "{0}"
        '''.format(os_key, gpu_count, gpu_lic_table_name, lic_key_mapping)
    return sql

'''
----------------------------------------------------------------------------------------------
Get the pricing for the os license for the RAM:
'''
def _pull_ram_lic_pricing(os_key, ram_lic_table_name, lic_key_mapping):
    sql = _ram_lic_cost_sql(os_key, ram_lic_table_name, lic_key_mapping)
    results = _bq_harness_with_result(sql, False)
    raml_pricing = None
    if results is not None:
        for row in results:
            if raml_pricing is None:
                raml_pricing = (row.lic_key, row.max_usd, row.pricing_unit, row.sku_id)
            else:
                print(row)
                print("Too many ram lic cost results")
                #raise Exception("Too many ram license cost results")
    return raml_pricing

'''
----------------------------------------------------------------------------------------------
SQL for above
'''

def _ram_lic_cost_sql(os_key, ram_lic_table_name, lic_key_mapping):

    sql = '''
        SELECT
            licp.lic_key,
            lk2l.vm_lic_key,
            lk2l.vm_license,
            licp.sku_id,
            licp.min_usd,
            licp.max_usd,
            licp.pricing_unit
        FROM `{2}` as lk2l
        JOIN `{1}` as licp
        ON lk2l.vm_license = licp.lic_key
        WHERE lk2l.vm_lic_key = "{0}"
        '''.format(os_key, ram_lic_table_name, lic_key_mapping)
    return sql


'''
----------------------------------------------------------------------------------------------
Calculate egress charges
'''
def _calc_egress(table_name, project_id):
    sql = _egress_sql(table_name, project_id)
    results = _bq_harness_with_result(sql, False)
    data_move_costs = 0.0
    if results is not None:
        for row in results:
            data_move_costs += row.totalCost
    return data_move_costs

'''
----------------------------------------------------------------------------------------------
Download SQL
'''
def _egress_sql(table_name, project_id):

    # Download APAC = 1F8B-71B0-3D1B
    # Download Australia = 9B2D-2B7D-FA5C
    # Download China = 4980-950B-BDA6
    # Download Worldwide Destinations (excluding Asia & Australia) = 22EB-AAE8-FBCD
    #

    # Inter-region GCP Storage egress within NA
    #    * gsutil cp from us-west bucket to a VM running in us-central
    #    * gsutil cp from us-west bucket to a bucket in us-east
    #    * Both operations cost: 2.739579800516365 gibibyte -> $0.0224 each
    # Download Worldwide Destinations (excluding Asia & Australia):
    #    * gsutil cp from from us-west bucket to local laptop
    #    * Operation cost : 2.7395823346450907 gibibyte -> $0.209
    # All operations took place on Sunday evening and appeared in the BQ table ~ 1 PM Monday
    # In another test, download charges appeared in the budget total sent to cloud function (4 AM PST Tuesday)
    # approx 13 hours after the operation (2-3 PM PST Monday). Appeared in BQ table at 4:40 AM PST Tuesday.
    #  "GCP Storage egress between"

    sql = '''
        WITH
          t1 AS (
          SELECT
            project.id AS project_id,
            project.name AS project_name,
            service.description AS service,
            sku.description AS sku,
            cost,
            usage.amount AS usage_amount,
            usage.unit AS usage_unit,
            invoice.month AS invoice_yyyymm
          FROM
            `{0}`
          WHERE
            project.id = "{1}" AND (sku.description LIKE "%ownload%" OR sku.description LIKE "%gress%")),
          t2 AS (
          SELECT
            project_id,
            project_name,
            service,
            sku,
            SUM(cost) AS totalCost,
            SUM(usage_amount) AS totalUsage,
            usage_unit,
            invoice_yyyymm
          FROM
            t1
          GROUP BY
            1,
            2,
            3,
            4,
            7,
            8)
        SELECT
          *
        FROM
          t2
        ORDER BY
          totalCost DESC

        '''.format(table_name, project_id)
    return sql


'''
----------------------------------------------------------------------------------------------
Find out when Google last reported charges for each significant SKU
'''
def _estimate_burn_for_sku(sku_history):

    #
    # What is the last complete usage slot? If we have at least three slots, and the last is *less* than
    # the previous two (or better, three), we consider that last slot to be incomplete. We return the
    # cost of that second to last slot, and the number of hours it needs to be applied to get up to the present
    # moment.
    #

    cost_history = []
    for slot in sku_history:
        cost_history.append(slot['total_cost'])

    if len(cost_history) == 1:
        # One point is all we have.
        return cost_history[0], sku_history[0]['usage_end_time']
    elif len(cost_history) == 2:
        # If we have the two points and the last is lower, we consider it incomplete.
        # If the last point is higher, we were ramping up.
        # Basically, return the maxiumum
        return cost_history[0], sku_history[0]['usage_end_time']
    elif len(cost_history) == 3:
        pass
    elif len(cost_history) == 4:
        pass

    return
    '''

    charge_history_for_sku = {}
    if results is not None:
        for row in results:
            if row.sku not in charge_history_for_sku:
                history = []
                charge_history_for_sku[row.sku] = history
            else:
                history = charge_history_for_sku[row.sku]

            history.append({'description': row.description,
                            'sku_description': row.sku_description,
                            'usage_start_time': row.usage_start_time,
                            'usage_end_time': row.usage_end_time,
                            'first_export': row.first_export,
                            'last_export': row.last_export,
                            'total_cost': row.total_cost,
                            'delay': row.delay})

    return charge_history_for_sku

    '''



'''
----------------------------------------------------------------------------------------------
Find out when Google last reported charges for each significant SKU
'''
def _get_charge_history_for_sku(table_name, project_id, min_charge, depth, days):
    sql = _last_n_charges_sql(table_name, project_id, min_charge, depth, days)
    results = _bq_harness_with_result(sql, False)

    charge_history_for_sku = {}
    if results is not None:
        for row in results:
            if row.sku_id not in charge_history_for_sku:
                history = []
                charge_history_for_sku[row.sku_id] = history
            else:
                history = charge_history_for_sku[row.sku_id]

            history.append({'description': row.description,
                            'sku_description': row.sku_description,
                            'usage_start_time': row.usage_start_time,
                            'usage_end_time': row.usage_end_time,
                            'first_export': row.first_export,
                            'last_export': row.last_export,
                            'total_cost': row.total_cost,
                            'delay': row.delay})

    return charge_history_for_sku


'''
----------------------------------------------------------------------------------------------
SQL for above
'''

def _last_n_charges_sql(table_name, project_id, tiny_min_charge, depth, days):

    #
    # For charges that are not tiny, over the last two days, sum them all up in each usage time interval.
    # Return the last "depth" intervals. Also report the oldest and newest export times for that interval,
    # and calculate how stale the last reported interval is.
    #
    # What is currently seen is that with constant charges (e.g. a running VM), the last export will be
    # several hours (e.g. five or six) ago, and that last export is often a partial accounting. You need to
    # go to the second oldest value to get a number matching the long-term trend.
    #

    sql = '''
        WITH a1 as (
          SELECT
            service.description AS description,
            sku.id AS sku_id,
            sku.description AS sku_description,
            usage_start_time,
            usage_end_time,
            MIN(export_time) AS first_export,
            MAX(export_time) AS last_export,
            ROUND(SUM(cost), 5) as total_cost,
            RANK() OVER (PARTITION BY sku.id ORDER BY usage_end_time desc) AS rank,
            CURRENT_TIMESTAMP() - usage_end_time as delay
          FROM `{0}`
            WHERE
            _PARTITIONTIME > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {4} DAY)
            AND cost > {2}
            AND project.id = "{1}"
          GROUP BY service.description, sku.id, sku.description, usage_start_time, usage_end_time
        )
        SELECT * from a1 where rank <= {3} ORDER BY sku_id, usage_start_time desc
        '''.format(table_name, project_id, tiny_min_charge, depth, days)

    return sql

'''
----------------------------------------------------------------------------------------------
Use to run queries where we want to get the result back to use (not write into a table)
'''
def _bq_harness_with_result(sql, do_batch):
    """
    Handles all the boilerplate for running a BQ job
    """

    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig()
    if do_batch:
        job_config.priority = bigquery.QueryPriority.BATCH
    location = 'US'

    # API request - starts the query
    query_job = client.query(sql, location=location, job_config=job_config)

    # Query
    job_state = 'NOT_STARTED'
    while job_state != 'DONE':
        query_job = client.get_job(query_job.job_id, location=location)
        #print('Job {} is currently in state {}'.format(query_job.job_id, query_job.state))
        job_state = query_job.state
        if job_state != 'DONE':
            time.sleep(5)
    #print('Job {} is done'.format(query_job.job_id))

    query_job = client.get_job(query_job.job_id, location=location)
    if query_job.error_result is not None:
        print('Error result!! {}'.format(query_job.error_result))
        return None

    results = query_job.result()

    return results


'''
----------------------------------------------------------------------------------------------
We can get recent BQ cost data by querying the API to list jobs in the last x hours. This returns
the number of bytes billed, which allows us to calculate the expense.
'''
def _find_recent_bq_usage(proj_id, usage_hours):

    bq_history = {}
    start_hour = None
    for hour_str in usage_hours:
        hour = datetime.datetime.strptime(hour_str, '%Y-%m-%dT%H%z')
        if (start_hour is None) or (hour < start_hour):
            start_hour = hour
        next_hour = hour + datetime.timedelta(hours=1)
        bq_history[hour_str] = {
            'start_hour': hour,
            'end_hour': next_hour,
            'per_location': {}
        }

    client = bigquery.Client(proj_id)
    try:
        for job in client.list_jobs(min_creation_time=start_hour, all_users=True):
            # We might be dealing with a bigquery.job.LoadJob:
            if not isinstance(job, bigquery.job.query.QueryJob):
                continue
            strt = job.started
            history_key_obj = job.started - datetime.timedelta(minutes=strt.minute, seconds=strt.second, microseconds=strt.microsecond)
            history_key = history_key_obj.strftime('%Y-%m-%dT%H%z')
            if history_key in bq_history:
                job_entry = bq_history[history_key]['per_location']
                if job.location in job_entry:
                    location_entry = job_entry[job.location]
                else:
                    location_entry = {
                        'location': job.location,
                        'job_count': 0,
                        'byte_count': 0
                    }
                    job_entry[job.location] = location_entry

                location_entry['job_count'] += 1
                if job.total_bytes_billed is not None:
                    location_entry['byte_count'] += job.total_bytes_billed

    except Exception as e:
        print("BigQuery job listing bad reply {}:".format(e))

    return bq_history

'''
----------------------------------------------------------------------------------------------
Get the pricing for BigQuery:
'''
def _get_billing_account_bq_usage(cost_table, first_of_month):
    sql = _get_billing_account_bq_usage_sql(cost_table, first_of_month)
    results = _bq_harness_with_result(sql, False)
    bq_usage = {}
    if results is not None:
        for row in results:
            bq_usage[row.location] = row.use_this_month
    return bq_usage

'''
----------------------------------------------------------------------------------------------
SQL for above
'''

def _get_billing_account_bq_usage_sql(table_name, first_of_month):

    sql = '''
      SELECT
      location.location AS location,
      SUM(usage.amount_in_pricing_units) AS use_this_month,
      usage.pricing_unit AS units
      FROM `{0}`
      WHERE DATE(_PARTITIONTIME) >= "{1}"
      AND service.description = "BigQuery"
      AND sku.description LIKE "%Analysis%"
      AND usage_start_time >= "{1}"
      GROUP BY location, usage.pricing_unit
    '''.format(table_name, first_of_month)
    return sql


'''
----------------------------------------------------------------------------------------------
Get cumulative use of IP addresses for this billing account
'''
def _get_billing_account_ip_usage(cost_table, first_of_month):
    sql = _get_billing_account_ip_usage_sql(cost_table, first_of_month)
    results = _bq_harness_with_result(sql, False)
    ip_usage = {}
    if results is not None:
        for row in results:
            ip_usage[row.location] = row.use_this_month
    return ip_usage

'''
----------------------------------------------------------------------------------------------
SQL for above
'''

def _get_billing_account_ip_usage_sql(table_name, first_of_month):

    sql = '''
      SELECT
      location.location AS location,
      SUM(usage.amount_in_pricing_units) AS use_this_month,
      usage.pricing_unit AS units
      FROM `{0}`
      WHERE DATE(_PARTITIONTIME) >= "{1}"
      AND service.description = "Compute Engine"
      AND sku.description LIKE "%External IP Charge%"
      AND usage_start_time >= "{1}"
      GROUP BY location, usage.pricing_unit
    '''.format(table_name, first_of_month)
    return sql

'''
----------------------------------------------------------------------------------------------
Get the SKUs for storage-related items
'''
def _get_gcs_skus(sku_table):
    sql = _get_gcs_skus_sql(sku_table)
    results = _bq_harness_with_result(sql, False)
    gcs_skus = []
    if results is not None:
        for row in results:
            gcs_skus.append(row.sku_id)
    return gcs_skus

'''
----------------------------------------------------------------------------------------------
SQL for above
'''

def _get_gcs_skus_sql(table_name):

    sql = '''
      SELECT DISTINCT
      id as sku_id
      FROM `{0}`
    '''.format(table_name)
    return sql

'''
----------------------------------------------------------------------------------------------
Get cumulative project cost for month
'''
def _get_project_cost_for_month(cost_table, first_of_month, project_id, invoice):
    sql = _get_project_cost_for_month_sql(cost_table, first_of_month, project_id, invoice)
    results = _bq_harness_with_result(sql, False)
    if results is not None:
        for row in results:
            return row.total_cost, row.total_credit

'''
----------------------------------------------------------------------------------------------
SQL for above
'''

def _get_project_cost_for_month_sql(table_name, first_of_month, project_id, invoice):

    sql = '''
      SELECT
      project.id AS project_id,
      ROUND(SUM(cost), 5) as total_cost,
      ROUND(SUM(cr.amount), 5) as total_credit,
      FROM `{0}`
      LEFT JOIN UNNEST(credits) as cr
      WHERE DATE(_PARTITIONTIME) >= "{1}"
      AND project.id = "{2}"
      AND invoice.month = "{3}"
      GROUP BY project.id
    '''.format(table_name, first_of_month, project_id, invoice)
    return sql


'''
----------------------------------------------------------------------------------------------
Get cumulative project cost for month
'''
def _get_project_cost_for_month_by_sku(cost_table, first_of_month, project_id, invoice):
    sql = _get_project_cost_for_month_by_sku_sql(cost_table, first_of_month, project_id, invoice)
    results = _bq_harness_with_result(sql, False)
    tc = 0.0
    tcr = 0.0
    stt = 0.0
    if results is not None:
        for row in results:
            cred = row.total_credit if (row.total_credit is not None) else 0.0
            subTot = row.total_cost + cred
            print(row.sku_desc, row.sku_id, row.total_cost, cred,
                  row.usage_amt, row.pricing_unit, subTot)
            tc += row.total_cost
            tcr += cred
            stt += subTot

    print(stt)
    return tc, tcr

'''
----------------------------------------------------------------------------------------------
SQL for above
'''

def _get_project_cost_for_month_by_sku_sql(table_name, first_of_month, project_id, invoice):

    sql = '''
      WITH a1 as (SELECT
      project.id AS project_id,
      sku.id AS sku_id,
      sku.description as sku_desc,
      ROUND(SUM(cost), 2) as total_cost,
      ROUND(SUM(cr.amount), 2) as total_credit,
      ROUND(SUM(usage.amount_in_pricing_units), 5) as usage_amt,
      usage.pricing_unit as pricing_unit
      FROM `{0}`
      LEFT JOIN UNNEST(credits) as cr
      WHERE DATE(_PARTITIONTIME) >= "{1}"
      AND project.id = "{2}"
      AND invoice.month = "{3}"
      GROUP BY project.id, sku.id, sku.description, pricing_unit)
      SELECT * FROM a1 ORDER BY a1.total_cost desc
    '''.format(table_name, first_of_month, project_id, invoice)
    return sql


'''
----------------------------------------------------------------------------------------------
Get the pricing for BigQuery:
'''
def _pull_bq_pricing(bq_table_name, region, bq_usage_for_month, bq_pricing_cache):
    #
    # We have a slight hack to implement, where "US" is mapped to "MULTI_REGIONAL" and an arbitrary us region, and
    # "EU" is also mapped in a similar fashion.
    #
    if region == "US":
        type_reg = ("Analysis", "MULTI_REGIONAL", "us-west2")
    elif region == "EU":
        type_reg = ("Analysis", "MULTI_REGIONAL", "europe-west3")
    else:
        type_reg = ("Analysis ({})".format(region), "REGIONAL", region)

    bq_month_for_region = bq_usage_for_month[region.lower()]

    tier_levels = []

    if type_reg in bq_pricing_cache:
        bq_tier_tuples = bq_pricing_cache[type_reg]
        for tup in bq_tier_tuples:
            tier_levels.append(tup[2])
    else:
        sql = _bq_cost_sql(bq_table_name, type_reg)
        results = _bq_harness_with_result(sql, False)
        bq_tier_tuples = []

        if results is not None:
            for row in results:
                bq_tier_tuples.append((row.type, row.region, row.start_usage_amount, row.usd_amount, row.pricing_unit, row.sku_id))
                tier_levels.append(row.start_usage_amount)

        bq_pricing_cache[type_reg] = bq_tier_tuples

    # Plus, we need to apply the tiered pricing, based on the BQ usage across all billing account projects
    tier_index = -1
    for tier in sorted(tier_levels):
        if bq_month_for_region < tier:
            break
        tier_index += 1

    return bq_tier_tuples[tier_index]

'''
----------------------------------------------------------------------------------------------
BQ Cost SQL
'''

def _bq_cost_sql(bq_table, type_reg):

    sql = '''
        SELECT
            bqc.type,
            bqc.gt_region as region,
            bqc.id as sku_id,
            bqc.sku_desc,
            bqc.start_usage_amount,
            bqc.usd_amount,
            bqc.pricing_unit
        FROM `{0}` as bqc
        WHERE bqc.gt_region = "{3}"
        AND bqc.type = "{2}"
        AND bqc.sku_desc = "{1}"
        '''.format(bq_table, type_reg[0], type_reg[1], type_reg[2])
    return sql


'''
----------------------------------------------------------------------------------------------
  h/t Joshua Davies, https://commandlinefanatic.com/cgi-bin/showarticle.cgi?article=art084
  Compute the average of a column of data
  df is a list of lists of floats, i is the index of the data within
  df to average.
'''
def stats(df, i):
  ave = sum([l[i] for l in df]) / len(df)
  return ave


'''
----------------------------------------------------------------------------------------------
  h/t Joshua Davies, https://commandlinefanatic.com/cgi-bin/showarticle.cgi?article=art084
  Braindead linear regression. Don't need to haul in numpy and pandas just to fit a line thru four points
  df is a list of list of float values
  m is the slope of the line that best fits df, b is the y-intercept
'''

def best_fit(df):
  ave_x = stats(df, 0)
  ave_y = stats(df, 1)
  m = sum([l[0] * (l[1] - ave_y) for l in df]) / sum([l[0] * (l[0] - ave_x) for l in df])
  b = ave_y - m * ave_x
  retval = (m, b)

  return retval


if __name__ == "__main__":
    web_trigger(None)