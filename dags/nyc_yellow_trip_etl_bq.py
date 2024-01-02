"""
File: nyc_yellow_trip_etl_bq.py
Author: Tolgahan Cepel
Email: tolgahan.cepel@gmail.com
Date: January 1, 2024
Description: ETL pipeline from HTTP data to BigQuery.
"""

from datetime import datetime, timedelta
import tempfile
import requests
import pandas as pd
from google.cloud import bigquery
from airflow import DAG
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 6, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'nyc_yellow_trip_etl_bq',
    default_args=default_args,
    description='ETL pipeline for NYC Yellow Taxi Trip data processing',
    schedule_interval='0 6 1 * *'
)

def af_extract_http_load_to_gcs(execution_date):
    http_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_' + execution_date[:7] + '.parquet'

    gcs_bucket_name = 'nyc-taxi-etl-pipeline-cepel'
    gcs_object_name = 'raw_data/yellow_tripdata_' + execution_date[:7].replace("-", "") + '.parquet'
    
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')

    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        response = requests.get(http_url)
        temp_file.write(response.content)
        temp_file_path = temp_file.name

    if response.status_code == 200:
        gcs_hook.upload(
            bucket_name=gcs_bucket_name,
            object_name=gcs_object_name,
            filename=temp_file_path
        )
        print('File uploaded to GCS successfully.')
    else:
        print('Failed to fetch data from HTTP source!')


def af_transform_and_load_to_bq(execution_date):
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')

    gcs_bucket = 'nyc-taxi-etl-pipeline-cepel'
    gcs_object = 'raw_data/yellow_tripdata_' + execution_date[:7].replace("-", "") + '.parquet'

    gcs_file_path = gcs_hook.download(
        bucket_name=gcs_bucket,
        object_name=gcs_object,
        filename='/tmp/tmp_yellow_tripdata_' + execution_date[:7].replace("-", "") + '.parquet'
    )

    df = pd.read_parquet(gcs_file_path)

    fact_taxi_trips = df[["VendorID", "RatecodeID", "store_and_fwd_flag", "PULocationID", "DOLocationID",
                      "payment_type", "passenger_count", "trip_distance", "fare_amount", "extra", "mta_tax",
                      "tip_amount", "tolls_amount", "improvement_surcharge", "total_amount"]]
    
    fact_taxi_trips = fact_taxi_trips.rename(
        columns={"VendorID": "vendor_id",
                "RatecodeID": "rate_code_id",
                "PULocationID": "pu_location_id",
                "DOLocationID": "do_location_id"}
    )

    credentials_path = "/home/cepel/airflow/nyc-taxi-409720-84d443b80c1e.json"

    client = bigquery.Client.from_service_account_json(credentials_path, project="nyc-taxi-409720")

    dataset_id = "fact_dataset"
    table_id = "fact_yellow_trips_m_" + execution_date[:7].replace("-", "")

    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job_config.create_disposition = bigquery.CreateDisposition.CREATE_IF_NEEDED
    job_config.autodetect = True

    client.load_table_from_dataframe(fact_taxi_trips, table_ref, job_config=job_config).result()


def af_load_bq_to_bq_looker():

    sql_query = """
    CREATE OR REPLACE TABLE `nyc-taxi-409720.analytics_nyc_taxi.looker_nyc_taxi_{{execution_date.strftime("%Y%m")}}` AS (
    SELECT 
        a.vendor_id
        ,a.rate_code_id
        ,a.store_and_fwd_flag
        ,a.pu_location_id
        ,a.do_location_id
        ,a.payment_type
        ,a.passenger_count
        ,a.trip_distance
        ,a.fare_amount
        ,a.extra
        ,a.mta_tax
        ,a.tip_amount
        ,a.tolls_amount
        ,a.improvement_surcharge
        ,a.total_amount
        ,b.rate_code_name
        ,c.payment_type_name
    FROM `nyc-taxi-409720.fact_dataset.fact_yellow_trips_m_{{execution_date.strftime("%Y%m")}}` a
    LEFT JOIN `nyc-taxi-409720.dim_dataset.dim_rate_code` b ON a.rate_code_id=b.rate_code_id  
    LEFT JOIN `nyc-taxi-409720.dim_dataset.dim_payment_type` c ON a.payment_type=c.payment_type_id
    )
    """

    return sql_query

extract_http_load_to_gcs = PythonOperator(
    task_id='extract_http_load_to_gcs',
    python_callable=af_extract_http_load_to_gcs,
    op_kwargs={'execution_date': '{{ execution_date }}'},
    dag=dag,
)

transform_and_load_to_bq = PythonOperator(
    task_id='transform_and_load_to_bq',
    python_callable=af_transform_and_load_to_bq,
    provide_context=True,
    op_kwargs={'execution_date': '{{ execution_date }}'},
    dag=dag
)

load_bq_to_bq_looker = BigQueryExecuteQueryOperator(
    task_id='load_bq_to_bq_looker',
    sql=af_load_bq_to_bq_looker(),
    use_legacy_sql=False,
    location='EU',
    gcp_conn_id='google_cloud_default',
    params={'execution_date': '{{ execution_date }}'},
    dag=dag,
)

extract_http_load_to_gcs >> transform_and_load_to_bq >> load_bq_to_bq_looker
