#
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

from googleapiclient import discovery
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
CPU_LIC_PRICES = os.environ["CPU_LIC_PRICES"]
GPU_LIC_PRICES = os.environ["GPU_LIC_PRICES"]
RAM_LIC_PRICES = os.environ["RAM_LIC_PRICES"]

PROJECT_ID = os.environ["PROJECT_ID"]

'''
----------------------------------------------------------------------------------------------
Trigger for HTTP
'''
def web_trigger(request):
    project_id = PROJECT_ID
    state_bucket = os.environ["STATE_BUCKET"]
    state_blob = "{}_state.json".format(project_id)
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(state_bucket)
    cost_state = retrieve_cost_state(bucket, state_blob)
    cost_calculate(project_id)
    roll_cost_state(cost_state)
    stash_cost_state(cost_state, bucket, state_blob)
    return "Completed"

'''
----------------------------------------------------------------------------------------------
Trigger for PubSub
'''
def pubsub_trigger(data, context):
    pubsub_data = base64.b64decode(data['data']).decode('utf-8')
    pubsub_json = json.loads(pubsub_data)
    budget_name = pubsub_json["budgetDisplayName"]
    project_id = budget_name.replace('-budget', '')
    state_bucket = os.environ["STATE_BUCKET"]
    state_blob = "{}_state.json".format(project_id)
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(state_bucket)
    cost_state = retrieve_cost_state(bucket, state_blob)
    cost_calculate(project_id)
    roll_cost_state(cost_state)
    stash_cost_state(cost_state, bucket, state_blob)
    return

'''
----------------------------------------------------------------------------------------------
Retrieve stored function state from bucket
'''
def retrieve_cost_state(state_bucket, state_blob):

    blob = state_bucket.blob(state_blob)
    blob_str = blob.download_as_string() if blob.exists() else b''
    cost_state = {"costs_previous_months_archived": {},
                  "costs_current_month_archived": {},
                  "costs_current_month_live": {}
                 } if (blob_str == b'') else json.loads(blob_str)

    return cost_state
'''
----------------------------------------------------------------------------------------------
Stash stored function state in bucket
'''
def stash_cost_state(cost_state, state_bucket, state_blob):
    blob = state_bucket.blob(state_blob)
    blob.upload_from_string(json.dumps(cost_state))
    return


'''
----------------------------------------------------------------------------------------------
Update cost state
'''
def roll_cost_state(cost_state):
    # Not doing this yet...
    return

'''
----------------------------------------------------------------------------------------------
Calculate costs
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
Calculate costs
'''
def cost_calculate(project_id):

    vm_pricing_cache = {}
    gpu_pricing_cache = {}
    pd_pricing_cache = {}
    cpu_lic_pricing_cache = {}
    gpu_lic_pricing_cache = {}
    ram_lic_pricing_cache = {}

    vm_cost_breakdown = {'uptime': 0.0,
                         'last_day': 0.0,
                         'hourly': 0.0
                        }
    disks_cost_breakdown = {'uptime': 0.0,
                            'last_day': 0.0,
                            'hourly': 0.0
                           }
    vm_count = 0
    is_enabled = _check_compute_services_for_project(project_id)
    if is_enabled:
        compute = discovery.build('compute', 'v1', cache_discovery=False)
        compute_inventory, type_dict, total_cpus = _check_compute_engine_inventory_aggregate(compute, project_id)
        for k, v in compute_inventory.items():
            machine_breakdown = _machine_cost(v, type_dict, vm_pricing_cache, gpu_pricing_cache,
                                              cpu_lic_pricing_cache, gpu_lic_pricing_cache, ram_lic_pricing_cache)
            vm_cost_breakdown['uptime'] += machine_breakdown['uptime']
            vm_cost_breakdown['last_day'] += machine_breakdown['last_day']
            vm_cost_breakdown['hourly'] += machine_breakdown['hourly']
            vm_count += 1
        disk_inventory = _check_persistent_disk_inventory_aggregate(compute, project_id)
        for disk in disk_inventory:
            disk_cost_breakdown = _disk_cost(disk, pd_pricing_cache)
            disks_cost_breakdown['uptime'] += disk_cost_breakdown['uptime']
            disks_cost_breakdown['last_day'] += disk_cost_breakdown['last_day']
            disks_cost_breakdown['hourly'] += disk_cost_breakdown['hourly']

    else:
        print("Project {} does not have compute enabled".format(project_id))

    if are_we_spending(vm_cost_breakdown, disks_cost_breakdown):
        log_client = glog.Client(project=project_id)
        log_client.get_default_handler()
        log_client.setup_logging()
        cost_logger = log_client.logger("cost_estimation_log")
        total_uptime_cost = vm_cost_breakdown['uptime'] + disks_cost_breakdown['uptime']
        total_last_day_cost = vm_cost_breakdown['last_day'] + disks_cost_breakdown['last_day']
        total_hourly_cost = vm_cost_breakdown['hourly'] + disks_cost_breakdown['hourly']
        try:
            print("Full uptime vm costs: vm cost %f disk cost %f total cost: %f" %
                  (vm_cost_breakdown['uptime'], disks_cost_breakdown['uptime'], total_uptime_cost))
            print("Last day vm costs: vm cost %f disk cost %f total cost: %f" %
                  (vm_cost_breakdown['last_day'], disks_cost_breakdown['last_day'], total_last_day_cost))
            print("Current hourly burn rates: vm count = %i; vm cost = %f; disk cost = %f; total cost = %f" %
                  (vm_count, vm_cost_breakdown['hourly'], disks_cost_breakdown['hourly'], total_hourly_cost))
            print("Projected cost next six hours: vm count = %i; vm cost = %f; disk cost = %f; total cost = %f" %
                  (vm_count, 6.0 * vm_cost_breakdown['hourly'], 6.0 * disks_cost_breakdown['hourly'], 6.0 * total_hourly_cost))
            print("Projected cost next 12 hours: vm count = %i; vm cost = %f; disk cost = %f; total cost = %f" %
                  (vm_count, 12.0 * vm_cost_breakdown['hourly'], 12.0 * disks_cost_breakdown['hourly'], 12.0 * total_hourly_cost))
            print("Projected cost next 24 hours: vm count = %i; vm cost = %f; disk cost = %f; total cost = %f" %
                  (vm_count, 24.0 * vm_cost_breakdown['hourly'], 24.0 * disks_cost_breakdown['hourly'], 24.0 * total_hourly_cost))

            cost_logger.log_text("Current hourly burn rates: vm cost: %f disk cost: %f total cost: %f" %
                                 (vm_cost_breakdown['hourly'], disks_cost_breakdown['hourly'], total_hourly_cost))
        except Exception as e:
            print("Exception while logging VM cost %s" % str(e))

    return

'''
----------------------------------------------------------------------------------------------
Compute the cost of the given machine:
'''
def _machine_cost(machine_info, type_dict, vm_pricing_cache, gpu_pricing_cache,
                  cpu_lic_pricing_cache, gpu_lic_pricing_cache, ram_lic_pricing_cache):

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

    hour_dict = {}
    uptime_hours = uptime.total_seconds() / (60 * 60)
    hour_dict['uptime'] = uptime_hours

    hour_dict['last_day'] = 24 if uptime_hours > 24 else uptime_hours
    hour_dict['hourly'] = 1

    license_name = machine_info['firstDiskFirstLicense']
    preempt = machine_info['preemptible']

    machine_type = machine_info['machineType']
    machine_zone = machine_info['zone']
    memory_size = type_dict[(machine_zone, machine_type)]['memory']

    memory_gb = memory_size / 1024
    cpus = machine_info['cpus']
    print(machine_info['name'], cpus)

    num_gpu = len(machine_info['accelerator_inventory'])

    cost_breakdown = {}
    machine_type_only = '-'.join(machine_type.split('-')[0:2])
    region = '-'.join(machine_zone.split('-')[0:-1])
    key = (machine_type_only, region, preempt)

    if key in vm_pricing_cache:
        price_lineitem = vm_pricing_cache[key]
    else:
        price_lineitem = _pull_vm_pricing(machine_type_only, preempt, region, CPU_PRICES, RAM_PRICES, KEY_MAP)
        if price_lineitem is not None:
            vm_pricing_cache[key] = price_lineitem
        else:
            raise Exception("No VM pricing found")

    for interval, hours in hour_dict.items():
        machine_cost = 0.0
        core_per_hour = 0.0
        gb_per_hour = 0.0
        if price_lineitem is not None:
            no_calc = False
            if price_lineitem[4] == "HOUR":
                core_per_hour = price_lineitem[3]
            else:
                no_calc = True
            if price_lineitem[6] is None:
                gb_per_hour = 0.0
            elif price_lineitem[6] == "GIBIBYTE_HOUR":
                gb_per_hour = price_lineitem[5]
            else:
                no_calc = True
            if not no_calc:
                # Machine type includes the CPU count. Do not multiply by CPU count!
                machine_cost = (hours * core_per_hour) + (hours * memory_gb * gb_per_hour)
            else:
                raise Exception("unexpected pricing units: {} {}".format(price_lineitem[4], price_lineitem[6]))
        else:
            raise Exception("machine key not present: {}".format(key))

        #
        # Calculate license costs
        #

        lic_cost = _license_cost(machine_info, license_name, cpus, num_gpu, memory_gb, hours,
                                 cpu_lic_pricing_cache, gpu_lic_pricing_cache, ram_lic_pricing_cache)
        machine_cost += lic_cost

        accel_cost = 0.0
        for accel_entry in machine_info['accelerator_inventory']:
            gpu_key = (accel_entry['type'], region, preempt)
            if gpu_key in gpu_pricing_cache:
                gpu_price_lineitem = gpu_pricing_cache[gpu_key]
            else:
                gpu_price_lineitem = _pull_gpu_pricing(accel_entry['type'], preempt, region, GPU_PRICES, GPU_KEY_MAP)
                if gpu_price_lineitem is not None:
                    gpu_pricing_cache[gpu_key] = gpu_price_lineitem

            if gpu_price_lineitem is not None:
                if gpu_price_lineitem[4] == "HOUR":
                    accel_per_hour = gpu_price_lineitem[3]
                    accel_cost += hours * accel_per_hour * accel_entry['count']
                else:
                    raise Exception("unexpected pricing units: {}".format(gpu_price_lineitem[4]))
            else:
                raise Exception("gpu key not present: {}".format(gpu_key))

        cost_breakdown[interval] = machine_cost + accel_cost

    return cost_breakdown

'''
----------------------------------------------------------------------------------------------
Compute the license cost of the given machine:
'''
def _license_cost(machine_info, license_name, num_cpu, num_gpu, memory_gb, hours,
                  cpu_lic_pricing_cache, gpu_lic_pricing_cache, ram_lic_pricing_cache):

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

    if num_gpu > 0:
        if gpu_key in gpu_lic_pricing_cache:
            gpu_price_lineitem = gpu_lic_pricing_cache[gpu_key]
        else:
            gpu_price_lineitem = _pull_gpu_lic_pricing(license_name, num_gpu, GPU_LIC_PRICES, LIC_KEY_MAP)
            if gpu_price_lineitem is not None:
                gpu_lic_pricing_cache[cpu_key] = gpu_price_lineitem
            else:
                raise Exception("No GPU license pricing found")
    else:
        gpu_price_lineitem = None

    if ram_key in ram_lic_pricing_cache:
        ram_price_lineitem = ram_lic_pricing_cache[gpu_key]
    else:
        ram_price_lineitem = _pull_ram_lic_pricing(license_name, RAM_LIC_PRICES, LIC_KEY_MAP)
        if ram_price_lineitem is not None:
            ram_lic_pricing_cache[cpu_key] = ram_price_lineitem
        else:
            raise Exception("No RAM license pricing found")


    no_calc = False
    cpu_lic_per_hour = 0.0
    gpu_lic_per_hour = 0.0
    ram_lic_per_hour_per_gib = 0.0

    if cpu_price_lineitem[2] == "HOUR":
        cpu_lic_per_hour = cpu_price_lineitem[1]
    else:
        no_calc = True

    if gpu_price_lineitem is not None:
        if gpu_price_lineitem[2] == "HOUR":
            gpu_lic_per_hour = gpu_price_lineitem[1]
        else:
            no_calc = True
        gpu_units = gpu_price_lineitem[2]
    else:
        gpu_lic_per_hour = 0.0
        gpu_units = "NO UNITS"

    if ram_price_lineitem[2] == "GIBIBYTE_HOUR":
        ram_lic_per_hour_per_gib = ram_price_lineitem[1]
    else:
        no_calc = True

    if not no_calc:
        license_cost = (hours * cpu_lic_per_hour) + \
                       (hours * gpu_lic_per_hour) + \
                       (hours * memory_gb * ram_lic_per_hour_per_gib)
        if hours == 1:
            print("Hourly licensing: license = {}; cpu = {} gpu = {} memory = {}".format(cpu_price_lineitem[0],
                                                                                         cpu_lic_per_hour,
                                                                                         gpu_lic_per_hour,
                                                                                         memory_gb * ram_lic_per_hour_per_gib))

    else:
        raise Exception("Unexpected pricing units {} {} {}".format(cpu_price_lineitem[2],
                                                                   gpu_units,
                                                                   ram_price_lineitem[2]))
    return license_cost

'''
----------------------------------------------------------------------------------------------
Compute the cost of the given disk:
'''
def _disk_cost(disk, disk_pricing_cache):
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

        cost_breakdown[interval] = disk_cost

    return cost_breakdown

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
                disk_list.append({'zone': zone.split('/')[-1],
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
Return information on all VMs in the project:
'''
def _check_compute_engine_inventory_aggregate(service, project_id):

    instance_dict = {}
    type_dict = {}
    total_cpus = 0

    request = service.instances().aggregatedList(project=project_id)

    while request is not None:
        response = request.execute()
        for zone, instance_scoped_list in response['items'].items():
            if "warning" in instance_scoped_list and instance_scoped_list["warning"]["code"] == 'NO_RESULTS_ON_PAGE':
                continue
            for instance in instance_scoped_list['instances']:
                total_cpus += _describe_running_instance(project_id, zone.split('/')[-1], service, instance, instance_dict, type_dict)
        request = service.instances().aggregatedList_next(previous_request=request, previous_response=response)

    return instance_dict, type_dict, total_cpus


'''
----------------------------------------------------------------------------------------------
# Describe a running instance:
'''
def _describe_running_instance(project_id, zone, compute, item, instance_dict, type_dict):

    cpu_count = 0
    if item['status'] == 'RUNNING':
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

        full_key = (zone, machine_key, instance_info['cpus'], instance_info['name'])
        cpu_count += instance_info['cpus']
        instance_dict[full_key] = instance_info

    return cpu_count

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
                            row.cpu_max_usd, row.cpu_pricing_unit, row.ram_max_usd, row.ram_pricing_unit, row.sku_id)
            else:
                raise Exception("Too many vm cost results")
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
               a1.preemptible,
               a1.region,
               a1.sku_id,
               a1.max_usd as cpu_max_usd,
               a1.min_usd as cpu_min_usd,
               a1.pricing_unit as cpu_pricing_unit,
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
                gpu_pricing = (row.gpu_key, row.preemptible, row.region, row.max_usd, row.pricing_unit)
            else:
                raise Exception("Too many gpu cost results")
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
                pd_pricing = (row.pd_key, row.is_regional, row.region, row.max_usd, row.pricing_unit)
            else:
                raise Exception("Too many pd cost results")
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
                cpul_pricing = (row.lic_key, row.max_usd, row.pricing_unit)
            else:
                raise Exception("Too many cpu license cost results")
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
                gpul_pricing = (row.lic_key, row.max_usd, row.pricing_unit)
            else:
                raise Exception("Too many gpu license cost results")
    return gpul_pricing

'''
----------------------------------------------------------------------------------------------
SQL for above
'''

def _gpu_lic_cost_sql(os_key, gpu_count, gpu_lic_table_name, lic_key_mapping):

    sql = '''
        SELECT
            licp.machine_class,
            licp.lic_key,
            lk2l.vm_lic_key,
            lk2l.vm_license,
            licp.min_cpu,
            licp.max_cpu,
            licp.min_usd,
            licp.max_usd,
            licp.pricing_unit
        FROM `{3}` as lk2l
        JOIN `{2}` as licp
        ON lk2l.vm_license = licp.lic_key
        WHERE (licp.max_cpu is NULL OR licp.max_cpu >= {1})
        AND (licp.min_cpu is NULL OR licp.min_cpu <= {1})
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
                raml_pricing = (row.lic_key, row.max_usd, row.pricing_unit)
            else:
                raise Exception("Too many ram license cost results")
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
