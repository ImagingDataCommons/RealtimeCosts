"""

Copyright 2021, Institute for Systems Biology

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

"""

from google.cloud import bigquery
from google.cloud import storage
import time
import yaml
import sys
import io
import os
from os.path import expanduser
from json import loads as json_loads


'''
----------------------------------------------------------------------------------------------
The configuration reader. Parses the YAML configuration into dictionaries
'''
def load_config(yaml_config):
    yaml_dict = None
    config_stream = io.StringIO(yaml_config)
    try:
        yaml_dict = yaml.load(config_stream, Loader=yaml.FullLoader)
    except yaml.YAMLError as ex:
        print(ex)

    if yaml_dict is None:
        return None, None, None

    return yaml_dict['files_and_buckets_and_tables'], yaml_dict['mappings'], yaml_dict['steps']


'''
----------------------------------------------------------------------------------------------
Extract CPU-related skus from the whole-sku table exported by Google
'''
def generate_cpu_table(sku_table, target_dataset, dest_table, do_batch):

    sql = generate_cpu_table_sql(sku_table)
    return generic_bq_harness(sql, target_dataset, dest_table, do_batch, True)

'''
----------------------------------------------------------------------------------------------
SQL for above
'''
def generate_cpu_table_sql(sku_table):
    return '''
        WITH a1 AS (SELECT DISTINCT
            TRIM(REPLACE(REPLACE(REGEXP_EXTRACT(sku.description, "(.*) in .*"), "Preemptible ", " "), "running", "")) as cpu,
            (STRPOS(sku.description, "reemptible") != 0) as preemptible,
            region,
            sku.id as sku_id,
            tr.usd_amount as usd_amount,
            pricing_unit
        FROM `{0}`, UNNEST(geo_taxonomy.regions) as region,
                    UNNEST(billing_account_price.tiered_rates) as tr
        WHERE (sku.description LIKE "%Instance%" OR sku.description LIKE "%Core%")
        AND NOT sku.description LIKE "%Sole Tenancy%"
        AND NOT sku.description LIKE "%Proxy Instance Charge%"
        AND NOT sku.description LIKE "%Upgrade Premium%"
        AND NOT sku.description LIKE "%Reserved%"
        AND NOT sku.description LIKE "%Commitment%"
        AND NOT sku.description LIKE "%Ram%"
        AND service.description LIKE "%Compute%")
        SELECT cpu, preemptible, region, sku_id, MAX(usd_amount) AS max_usd, MIN(usd_amount) AS min_usd, pricing_unit
        FROM a1
        GROUP BY cpu, preemptible, region, sku_id, pricing_unit
        '''.format(sku_table)

'''
----------------------------------------------------------------------------------------------
Extract RAM-related skus from the whole-sku table exported by Google
'''
def generate_ram_table(sku_table, target_dataset, dest_table, do_batch):

    sql = generate_ram_table_sql(sku_table)
    return generic_bq_harness(sql, target_dataset, dest_table, do_batch, True)

'''
----------------------------------------------------------------------------------------------
SQL for above
'''
def generate_ram_table_sql(sku_table):
    return '''
        WITH a1 AS (SELECT DISTINCT
            TRIM(REPLACE(REPLACE(REGEXP_EXTRACT(sku.description, "(.*) in .*"), "Preemptible ", " "), "running", "")) as cpu,
            (STRPOS(sku.description, "reemptible") != 0) as preemptible,
            region,
            sku.id as sku_id,
            tr.usd_amount as usd_amount,
            pricing_unit
        FROM `{0}`, UNNEST(geo_taxonomy.regions) as region,
                    UNNEST(billing_account_price.tiered_rates) as tr
        WHERE (sku.description LIKE "%Instance%" OR sku.description LIKE "%Core%" OR sku.description LIKE "%Ram%")
        AND NOT sku.description LIKE "%Sole Tenancy%"
        AND NOT sku.description LIKE "%Proxy Instance Charge%"
        AND NOT sku.description LIKE "%Upgrade Premium%"
        AND NOT sku.description LIKE "%Reserved%"
        AND NOT sku.description LIKE "%Commitment%"
        AND (sku.description LIKE "%Ram%" OR sku.description LIKE "%RAM%")
        AND service.description LIKE "%Compute%")
        SELECT cpu, preemptible, region, sku_id, MAX(usd_amount) AS max_usd, MIN(usd_amount) AS min_usd, pricing_unit FROM a1
        GROUP BY cpu, preemptible, region, sku_id, pricing_unit
        '''.format(sku_table)

'''
----------------------------------------------------------------------------------------------
Extract GPU-related skus from the whole-sku table exported by Google
'''
def generate_gpu_table(sku_table, target_dataset, dest_table, do_batch):

    sql = generate_gpu_table_sql(sku_table)
    return generic_bq_harness(sql, target_dataset, dest_table, do_batch, True)

'''
----------------------------------------------------------------------------------------------
SQL for above
'''
def generate_gpu_table_sql(sku_table):
    return '''
        WITH a1 AS (SELECT DISTINCT
               CONCAT("Nvidia ", REGEXP_EXTRACT(sku.description, ".*Nvidia (.*) GPU .*")) as gpu,
               (STRPOS(sku.description, "reemptible") != 0) as preemptible,
               region,
               sku.id as sku_id,
               tr.usd_amount as usd_amount,
               pricing_unit
        FROM `{0}`, UNNEST(geo_taxonomy.regions) as region, UNNEST(billing_account_price.tiered_rates) as tr
        WHERE sku.description LIKE "%GPU%"
        AND NOT sku.description LIKE "%Sole Tenancy%"
        AND NOT sku.description LIKE "%Proxy Instance Charge%"
        AND NOT sku.description LIKE "%Upgrade Premium%"
        AND NOT sku.description LIKE "%Reserved%"
        AND NOT sku.description LIKE "%Commitment%"
        AND NOT sku.description LIKE "%Ram%"
        AND service.description LIKE "%Compute%")
        SELECT gpu, preemptible, region, sku_id, MAX(usd_amount) AS max_usd, MIN(usd_amount) AS min_usd, pricing_unit FROM a1
        GROUP BY gpu, preemptible, region, sku_id, pricing_unit
        '''.format(sku_table)


'''
----------------------------------------------------------------------------------------------
Extract persistent disk related skus from the whole-sku table exported by Google
'''
def generate_pd_table(sku_table, target_dataset, dest_table, do_batch):

    sql = generate_pd_table_sql(sku_table)
    return generic_bq_harness(sql, target_dataset, dest_table, do_batch, True)

'''
----------------------------------------------------------------------------------------------
SQL for above
'''
def generate_pd_table_sql(sku_table):
    return '''
        WITH a1 AS (SELECT DISTINCT
               TRIM(REPLACE(CONCAT(REGEXP_EXTRACT(sku.description, "(.*) PD Capacity.*"), " PD Capacity"), "Regional ", "")) as pd,
               (STRPOS(sku.description, "Regional") != 0) as is_regional,
               region,
               sku.id as sku_id,
               tr.usd_amount as usd_amount,
               pricing_unit
        FROM `{0}`, UNNEST(geo_taxonomy.regions) as region, UNNEST(billing_account_price.tiered_rates) as tr
        WHERE sku.description LIKE "%PD Capacity%")
        SELECT pd, is_regional, region, sku_id, MAX(usd_amount) AS max_usd, MIN(usd_amount) AS min_usd, pricing_unit FROM a1
        GROUP BY pd, is_regional, region, sku_id, pricing_unit
        '''.format(sku_table)

'''
----------------------------------------------------------------------------------------------
Extract BigQuery related skus from the whole-sku table exported by Google
'''
def generate_bq_table(sku_table, target_dataset, dest_table, do_batch):

    sql = generate_bq_table_sql(sku_table)
    return generic_bq_harness(sql, target_dataset, dest_table, do_batch, True)

'''
----------------------------------------------------------------------------------------------
SQL for above
'''
def generate_bq_table_sql(sku_table):
    return '''
        SELECT DISTINCT service.description as service_desc,
               sku.id,
               sku.description as sku_desc,
               geo_taxonomy.type,
               gt_region,
               pricing_unit,
               tr.start_usage_amount,
               tr.usd_amount
        FROM `{0}`,
        UNNEST(billing_account_price.tiered_rates) as tr,
        UNNEST(geo_taxonomy.regions) as gt_region
        WHERE service.description LIKE "%BigQuery%"
        AND sku.description LIKE "%Analysis%"
        '''.format(sku_table)

'''
----------------------------------------------------------------------------------------------
Extract ip address related skus from the whole-sku table exported by Google
'''
def generate_ip_table(sku_table, target_dataset, dest_table, do_batch):

    sql = generate_ip_table_sql(sku_table)
    return generic_bq_harness(sql, target_dataset, dest_table, do_batch, True)

'''
----------------------------------------------------------------------------------------------
SQL for above
'''
def generate_ip_table_sql(sku_table):
    return '''
        WITH a1 AS (SELECT DISTINCT
            TRIM(REGEXP_EXTRACT(sku.description, "(.*) on a .*")) AS ip_key,
               (STRPOS(sku.description, "reemptible") != 0) as preemptible,
               sku.id as sku_id,
               tr.usd_amount as usd_amount,
               pricing_unit
        FROM `{0}`,
        UNNEST(billing_account_price.tiered_rates) as tr
        WHERE (sku.description LIKE "% IP %")
        AND service.description LIKE "%Compute%")
        SELECT ip_key, preemptible, sku_id, MAX(usd_amount) AS max_usd, MIN(usd_amount) AS min_usd, pricing_unit FROM a1
        GROUP BY ip_key, preemptible, sku_id, pricing_unit
        '''.format(sku_table)

'''
----------------------------------------------------------------------------------------------
Extract GPU licensing pricing related skus from the whole-sku table exported by Google
'''
def generate_gpu_lic_table(sku_table, target_dataset, dest_table, do_batch):

    sql = generate_gpu_lic_table_sql(sku_table)
    return generic_bq_harness(sql, target_dataset, dest_table, do_batch, True)

'''
----------------------------------------------------------------------------------------------
SQL for above
'''
def generate_gpu_lic_table_sql(sku_table):
    return '''
        WITH a1 AS (SELECT DISTINCT
                CASE WHEN (STRPOS(sku.description, " on VM with ") != 0) THEN
                           TRIM(REGEXP_EXTRACT(sku.description, "Licensing Fee for (.*) on VM with .*"))
                     ELSE
                           TRIM(REPLACE(REGEXP_EXTRACT(sku.description, "Licensing Fee for (.*)"), "(GPU cost)", ""))
                END as lic_key,
                CASE WHEN (sku.description LIKE '% on VM with up to %') THEN
                           CAST(REGEXP_EXTRACT(sku.description, '.* on VM with up to ([0-9]+) GPU') AS INT64)
                     WHEN (sku.description LIKE '% on VM with % to %') THEN
                           CAST(REGEXP_EXTRACT(sku.description, '.* on VM with [0-9]+ to ([0-9]+) GPU') AS INT64)
                     ELSE
                           NULL
                END as max_gpu,
                CASE WHEN (sku.description LIKE '% or more %') THEN
                           CAST(REGEXP_EXTRACT(sku.description, '.* on VM with ([0-9]+) or more GPU') AS INT64)
                     WHEN (sku.description LIKE '% on VM with % to %') THEN
                           CAST(REGEXP_EXTRACT(sku.description, '.* on VM with ([0-9]+) to [0-9]+ GPU') AS INT64)
                     ELSE
                           NULL
                END as min_gpu,
                sku.id as sku_id,
                tr.usd_amount as usd_amount,
                pricing_unit
        FROM `{0}`,
        UNNEST(billing_account_price.tiered_rates) as tr
        WHERE (sku.description LIKE "%Licen%" AND sku.description LIKE "%GPU%")
        AND service.description LIKE "%Compute%")
        SELECT lic_key, min_gpu, max_gpu, sku_id, MAX(usd_amount) AS max_usd, MIN(usd_amount) AS min_usd, pricing_unit FROM a1
        GROUP BY lic_key, min_gpu, max_gpu, sku_id, pricing_unit
        '''.format(sku_table)

'''
----------------------------------------------------------------------------------------------
Extract CPU licensing pricing related skus from the whole-sku table exported by Google
'''
def generate_cpu_lic_table(sku_table, target_dataset, dest_table, do_batch):

    sql = generate_cpu_lic_table_sql(sku_table)
    return generic_bq_harness(sql, target_dataset, dest_table, do_batch, True)

'''
----------------------------------------------------------------------------------------------
SQL for above
'''
def generate_cpu_lic_table_sql(sku_table):
    return '''
        WITH a1 AS (SELECT DISTINCT
               CASE WHEN (STRPOS(sku.description, " on g1-small") != 0) THEN "g1-small"
                    WHEN (STRPOS(sku.description, " on f1-micro") != 0) THEN "f1-micro"
                    ELSE NULL
               END as machine_class,
               CASE WHEN (STRPOS(sku.description, " on VM with ") != 0) THEN
                        TRIM(REGEXP_EXTRACT(sku.description, "Licensing Fee for (.*) on VM with .*"))
                     WHEN (STRPOS(sku.description, " on g1-small") != 0) OR
                          (STRPOS(sku.description, " on f1-micro") != 0)THEN
                        TRIM(REGEXP_EXTRACT(sku.description, "Licensing Fee for (.*) on .*"))
                     ELSE
                        TRIM(REPLACE(REGEXP_EXTRACT(sku.description, "Licensing Fee for (.*)"), "(CPU cost)", ""))
               END as lic_key,
               CASE WHEN (sku.description LIKE '% on VM with up to %') THEN
                           CAST(REGEXP_EXTRACT(sku.description, '.* on VM with up to ([0-9]+) VCPU') AS INT64)
                     WHEN (sku.description LIKE '% on VM with % to %') THEN
                           CAST(REGEXP_EXTRACT(sku.description, '.* on VM with [0-9]+ to ([0-9]+) VCPU') AS INT64)
                     WHEN (sku.description LIKE '% on VM with % VCPU') THEN
                           CAST(REGEXP_EXTRACT(sku.description, '.* on VM with ([0-9]+) VCPU') AS INT64)
                     ELSE
                           NULL
               END as max_cpu,
               CASE WHEN (sku.description LIKE '% or more %') THEN
                           CAST(REGEXP_EXTRACT(sku.description, '.* on VM with ([0-9]+) or more VCPU') AS INT64)
                     WHEN (sku.description LIKE '% on VM with % to %') THEN
                           CAST(REGEXP_EXTRACT(sku.description, '.* on VM with ([0-9]+) to [0-9]+ VCPU') AS INT64)
                     WHEN (sku.description LIKE '% on VM with % VCPU') THEN
                           CAST(REGEXP_EXTRACT(sku.description, '.* on VM with ([0-9]+) VCPU') AS INT64)
                     ELSE
                           NULL
               END as min_cpu,
               sku.id as sku_id,
               tr.usd_amount as usd_amount,
               pricing_unit
        FROM `{0}`,
        UNNEST(billing_account_price.tiered_rates) as tr
        WHERE (sku.description LIKE "%Licen%" AND NOT sku.description LIKE "%GPU%" AND NOT sku.description LIKE "%RAM cost%")
        AND service.description LIKE "%Compute%")
        SELECT machine_class, lic_key, min_cpu, max_cpu, sku_id, MAX(usd_amount) AS max_usd, MIN(usd_amount) AS min_usd, pricing_unit FROM a1
        GROUP BY machine_class, lic_key, min_cpu, max_cpu, sku_id, pricing_unit
        '''.format(sku_table)

'''
----------------------------------------------------------------------------------------------
Extract RAM licensing pricing related skus from the whole-sku table exported by Google
'''
def generate_ram_lic_table(sku_table, target_dataset, dest_table, do_batch):

    sql = generate_ram_lic_table_sql(sku_table)
    return generic_bq_harness(sql, target_dataset, dest_table, do_batch, True)

'''
----------------------------------------------------------------------------------------------
SQL for above
'''
def generate_ram_lic_table_sql(sku_table):
    return '''
        WITH a1 AS (SELECT DISTINCT
            TRIM(REPLACE(REGEXP_EXTRACT(sku.description, "Licensing Fee for (.*)"), "(RAM cost)", "")) AS lic_key,
            sku.id as sku_id,
            tr.usd_amount as usd_amount,
            pricing_unit
        FROM `{0}`,
        UNNEST(billing_account_price.tiered_rates) as tr
        WHERE (sku.description LIKE "%Licen%" AND sku.description LIKE "%RAM%")
        AND service.description LIKE "%Compute%")
        SELECT lic_key, sku_id, MAX(usd_amount) AS max_usd, MIN(usd_amount) AS min_usd, pricing_unit FROM a1
        GROUP BY lic_key, sku_id, pricing_unit
        '''.format(sku_table)

'''
----------------------------------------------------------------------------------------------
Run BQ and put results in table
'''

def generic_bq_harness(sql, target_dataset, dest_table, do_batch, do_replace):
    """
    Handles all the boilerplate for running a BQ job
    """
    job_config = bigquery.QueryJobConfig()
    if do_batch:
        job_config.priority = bigquery.QueryPriority.BATCH
    write_depo = "WRITE_TRUNCATE" if do_replace else None
    return generic_bq_harness_write_depo(sql, target_dataset, dest_table, do_batch, write_depo)


'''
----------------------------------------------------------------------------------------------
Guts of previous function
'''

def generic_bq_harness_write_depo(sql, target_dataset, dest_table, do_batch, write_depo):
    """
    Handles all the boilerplate for running a BQ job
    """
    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig()
    if do_batch:
        job_config.priority = bigquery.QueryPriority.BATCH
    if write_depo is not None:
        job_config.write_disposition = write_depo

    target_ref = client.dataset(target_dataset).table(dest_table)
    job_config.destination = target_ref
    print(target_ref)
    location = 'US'

    # API request - starts the query
    query_job = client.query(sql, location=location, job_config=job_config)

    # Query
    query_job = client.get_job(query_job.job_id, location=location)
    job_state = query_job.state

    while job_state != 'DONE':
        query_job = client.get_job(query_job.job_id, location=location)
        print('Job {} is currently in state {}'.format(query_job.job_id, query_job.state))
        job_state = query_job.state
        if job_state != 'DONE':
            time.sleep(5)
    print('Job {} is done'.format(query_job.job_id))

    query_job = client.get_job(query_job.job_id, location=location)
    if query_job.error_result is not None:
        print('Error result!! {}'.format(query_job.error_result))
        return False
    return True

'''
----------------------------------------------------------------------------------------------
Upload file to staging bucket
'''
def upload_to_bucket(target_tsv_bucket, target_tsv_file, local_tsv_file):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(target_tsv_bucket)
    blob = bucket.blob(target_tsv_file)
    print(blob.name)
    blob.upload_from_filename(local_tsv_file)
    return

'''
----------------------------------------------------------------------------------------------
Create BQ from CSV
'''

def csv_to_bq(schema, csv_uri, dataset_id, targ_table, do_batch):
    client = bigquery.Client()

    dataset_ref = client.dataset(dataset_id)
    job_config = bigquery.LoadJobConfig()
    if do_batch:
        job_config.priority = bigquery.QueryPriority.BATCH

    schema_list = []
    for mydict in schema:
        schema_list.append(bigquery.SchemaField(mydict['name'], mydict['type'].upper(),
                                                mode=mydict['mode'], description=mydict['description']))

    job_config.schema = schema_list
    job_config.skip_leading_rows = 0
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.write_disposition =  bigquery.WriteDisposition.WRITE_TRUNCATE
    # Can make the "CSV" file a TSV file using this:
    #job_config.field_delimiter = '\t'

    load_job = client.load_table_from_uri(
        csv_uri,
        dataset_ref.table(targ_table),
        job_config=job_config)  # API request
    print('Starting job {}'.format(load_job.job_id))

    location = 'US'
    job_state = 'NOT_STARTED'
    while job_state != 'DONE':
        load_job = client.get_job(load_job.job_id, location=location)
        print('Job {} is currently in state {}'.format(load_job.job_id, load_job.state))
        job_state = load_job.state
        if job_state != 'DONE':
            time.sleep(5)
    print('Job {} is done'.format(load_job.job_id))

    load_job = client.get_job(load_job.job_id, location=location)
    if load_job.error_result is not None:
        print('Error result!! {}'.format(load_job.error_result))
        for err in load_job.errors:
            print(err)
        return False

    destination_table = client.get_table(dataset_ref.table(targ_table))
    print('Loaded {} rows.'.format(destination_table.num_rows))
    return True

'''
----------------------------------------------------------------------------------------------
Load Schema
'''
def load_schema(schema_loc):
    with open(schema_loc, mode='r') as schema_def:
        full_schema = json_loads(schema_def.read())

    return full_schema

'''
----------------------------------------------------------------------------------------------
Main Control Flow
Note that the actual steps run are configured in the YAML input! This allows you
to e.g. skip previously run steps.
'''


def main(args):

    if len(args) != 2:
        print(" ")
        print(" Usage : {} <configuration_yaml>".format(args[0]))
        return

    print('job started')

    #
    # Get the YAML config loaded:
    #

    with open(args[1], mode='r') as yaml_file:
        params, mappings, steps = load_config(yaml_file.read())

    #
    # BQ does not like to be given paths that have "~". So make all local paths absolute:
    #

    home = expanduser("~")

    if 'upload_map_csvs' in steps:
        for table_map in mappings:
            _, table_dict = next(iter(table_map.items()))
            tsv_loc = "%s/%s/%s" % (home, params['MAP_TSV_DIR'], table_dict['csv'])
            upload_to_bucket(params['WORKING_BUCKET'], table_dict['csv'], tsv_loc)

    if 'build_map_tables_from_csvs' in steps:
        for table_map in mappings:
            dest_table, table_dict = next(iter(table_map.items()))
            schema_loc = "%s/%s/%s" % (home, params['SCHEMA_DIR'], table_dict['schema'])
            bucket_src_url = 'gs://{}/{}'.format(params['WORKING_BUCKET'], table_dict['csv'])
            schema = load_schema(schema_loc)
            success = csv_to_bq(schema, bucket_src_url, params['TARGET_DATASET'], dest_table, params['BQ_AS_BATCH'])
            if not success:
                print("Build map tables failed")
                return

    if 'build_cpu_pricing' in steps:
        success = generate_cpu_table(params['SKU_TABLE'], params['TARGET_DATASET'], params['CPU_PRICING'], params['BQ_AS_BATCH'])
        if not success:
            print("Build cpu table failed")
            return

    if 'build_ram_pricing' in steps:
        success = generate_ram_table(params['SKU_TABLE'], params['TARGET_DATASET'], params['RAM_PRICING'], params['BQ_AS_BATCH'])
        if not success:
            print("Build ram table failed")
            return

    if 'build_gpu_pricing' in steps:
        success = generate_gpu_table(params['SKU_TABLE'], params['TARGET_DATASET'], params['GPU_PRICING'], params['BQ_AS_BATCH'])
        if not success:
            print("Build gpu table failed")
            return

    if 'build_pd_pricing' in steps:
        success = generate_pd_table(params['SKU_TABLE'], params['TARGET_DATASET'], params['PD_PRICING'], params['BQ_AS_BATCH'])
        if not success:
            print("Build pd table failed")
            return

    if 'build_cpu_lic_pricing' in steps:
        success = generate_cpu_lic_table(params['SKU_TABLE'], params['TARGET_DATASET'], params['CPU_LIC_PRICING'], params['BQ_AS_BATCH'])
        if not success:
            print("Build CPU license table failed")
            return

    if 'build_gpu_lic_pricing' in steps:
        success = generate_gpu_lic_table(params['SKU_TABLE'], params['TARGET_DATASET'], params['GPU_LIC_PRICING'], params['BQ_AS_BATCH'])
        if not success:
            print("Build CPU license table failed")
            return

    if 'build_ram_lic_pricing' in steps:
        success = generate_ram_lic_table(params['SKU_TABLE'], params['TARGET_DATASET'], params['RAM_LIC_PRICING'], params['BQ_AS_BATCH'])
        if not success:
            print("Build CPU license table failed")
            return

    if 'build_ip_pricing' in steps:
        success = generate_ip_table(params['SKU_TABLE'], params['TARGET_DATASET'], params['IP_PRICING'], params['BQ_AS_BATCH'])
        if not success:
            print("Build ip table failed")
            return

    if 'build_bq_pricing' in steps:
        success = generate_bq_table(params['SKU_TABLE'], params['TARGET_DATASET'], params['BQ_PRICING'], params['BQ_AS_BATCH'])
        if not success:
            print("Build bq table failed")
            return

    if 'print_env_vars' in steps:
        print("Environment variables for function, using these tables:")
        for table_map in mappings:
            dest_table, table_dict = next(iter(table_map.items()))
            print("{}={}.{}.{}".format(table_dict['env'], params['WORKING_PROJECT'], params['TARGET_DATASET'], dest_table))


        print("CPU_PRICES={}.{}.{}".format(params['WORKING_PROJECT'], params['TARGET_DATASET'], params['CPU_PRICING']))
        print("RAM_PRICES={}.{}.{}".format(params['WORKING_PROJECT'], params['TARGET_DATASET'], params['RAM_PRICING']))
        print("GPU_PRICES={}.{}.{}".format(params['WORKING_PROJECT'], params['TARGET_DATASET'], params['GPU_PRICING']))
        print("PD_PRICES={}.{}.{}".format(params['WORKING_PROJECT'], params['TARGET_DATASET'], params['PD_PRICING']))
        print("IP_PRICES={}.{}.{}".format(params['WORKING_PROJECT'], params['TARGET_DATASET'], params['IP_PRICING']))
        print("CPU_LIC_PRICES={}.{}.{}".format(params['WORKING_PROJECT'], params['TARGET_DATASET'], params['CPU_LIC_PRICING']))
        print("GPU_LIC_PRICES={}.{}.{}".format(params['WORKING_PROJECT'], params['TARGET_DATASET'], params['GPU_LIC_PRICING']))
        print("RAM_LIC_PRICES={}.{}.{}".format(params['WORKING_PROJECT'], params['TARGET_DATASET'], params['RAM_LIC_PRICING']))
        print("Plus the PROJECT_ID if function uses webapp trigger")

    print('job completed')


if __name__ == "__main__":
    main(sys.argv)





'''




        SELECT DISTINCT service.description as service_desc,
               sku.id,
               sku.description as sku_desc,
               geo_taxonomy.type,
               gt_region,
               pricing_unit,
               tr.start_usage_amount,
               tr.usd_amount
        FROM `idc-external-admin.idc_external_skus.cloud_pricing_export`,
        UNNEST(billing_account_price.tiered_rates) as tr,
        UNNEST(geo_taxonomy.regions) as gt_region
        WHERE service.description LIKE "%BigQuery%"


        SELECT DISTINCT service.description as service_desc,
               sku.id,
               sku.description as sku_desc,
               geo_taxonomy.type,
               gt_region,
               pricing_unit,
               tr.start_usage_amount,
               tr.usd_amount
        FROM `idc-external-admin.idc_external_skus.cloud_pricing_export`,
        UNNEST(billing_account_price.tiered_rates) as tr,
        UNNEST(geo_taxonomy.regions) as gt_region,
        WHERE service.description LIKE "%BigQuery%"


        SELECT DISTINCT service.description as service_desc,
               sku.id,
               sku.description as sku_desc,
               geo_taxonomy.type,
               gt_region,
               pricing_unit,
               tr.start_usage_amount,
               tr.usd_amount
        FROM `idc-external-admin.idc_external_skus.cloud_pricing_export`,
        UNNEST(billing_account_price.tiered_rates) as tr,
        UNNEST(geo_taxonomy.regions) as gt_region,
        WHERE service.description LIKE "%BigQuery%"

        SELECT DISTINCT service.description as service_desc,
               sku.id,
               sku.description as sku_desc,
               pricing_unit,
               tr.start_usage_amount,
               tr.usd_amount
        FROM `idc-external-admin.idc_external_skus.cloud_pricing_export`,
        UNNEST(billing_account_price.tiered_rates) as tr
        WHERE service.description LIKE "%BigQuery%"



'''