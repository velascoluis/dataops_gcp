
import datetime

from airflow import models
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from . import sql_queries

YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

default_args = {
    "owner": "velascoluis",
    "depends_on_past": False,
    "email": [""],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "start_date": YESTERDAY,
    "project_id": "velascoluis-dev-sandbox",
    "dataset_id": "airline",
    "base_tables: { 'fact_flight_table' : "fact_flight" , 'dim_flight_table' : "dim_flight" ,  'dim_airport_table' : "dim_airport" }
}




with models.DAG(
    "composer_airline_dag",
    "catchup=False",
    default_args=default_args,
    schedule_interval=datetime.timedelta(days=1),
) as dag:
    
    fact_flight_table = default_args['fact_flight_table']
    dim_flight_table = default_args['dim_flight_table']
    dim_airport_table = default_args['dim_airport_table']
    
    airline_etl_step_1 = BigQueryInsertJobOperator(
    task_id="airline_etl_step_1",
    configuration={
        "query": {
            "query": sql_queries.QUERY_STEP_1,
            "useLegacySql": False,
            "writeDisposition": "WRITE_TRUNCATE",
            'destinationTable': {
                'projectId': default_args['project_id'],
                'datasetId': default_args['dataset_id'],
                'tableId': "etl_step_1_delays_sql"
            },
        }
    },
)
    airline_etl_step_2 = BigQueryInsertJobOperator(
    task_id="airline_etl_step_2",
    configuration={
        "query": {
            "query": sql_queries.QUERY_STEP_2,
            "useLegacySql": False,
            "writeDisposition": "WRITE_TRUNCATE",
            'destinationTable': {
                'projectId': default_args['project_id'],
                'datasetId': default_args['dataset_id'],
                'tableId': "etl_step_2_flight_delays_with_airports_sql"
            },
        }
    },
)
    
    airline_etl_step_3 = BigQueryInsertJobOperator(
    task_id="airline_etl_step_3",
    configuration={
        "query": {
            "query": sql_queries.QUERY_STEP_3,
            "useLegacySql": False,
            "writeDisposition": "WRITE_TRUNCATE",
            'destinationTable': {
                'projectId': default_args['project_id'],
                'datasetId': default_args['dataset_id'],
                'tableId': "etl_step_2_flight_delays_with_airports_sql"
            },
        }
    },
)
    airline_etl_step_1 >> >> airline_etl_step_2 >> airline_etl_step_3