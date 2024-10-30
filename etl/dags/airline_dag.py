import datetime

from airflow import models
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

default_args = {
    "owner": "velascoluis",
    "depends_on_past": False,
    "email": [""],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "start_date": YESTERDAY
}


with models.DAG(
    "composer_airline_dag",
    "catchup=False",
    default_args=default_args,
    schedule_interval=datetime.timedelta(days=1),
) as dag:
    
    project_id = "velascoluis-dev-sandbox"
    dataset_id = "airline"
    base_tables = {
        'fact_flight_table': "fact_flight",
        'dim_flight_table': "dim_flight",
        'dim_airport_table': "dim_airport"
    }

    QUERY_STEP_1 = """
    SELECT
        flights.flight_key,
        COALESCE(flights.departure_delay, 0) + COALESCE(flights.arrival_delay, 0) AS total_delay,
        CASE WHEN COALESCE(flights.departure_delay, 0) + COALESCE(flights.arrival_delay, 0) <= 0 THEN 1 ELSE 0 END AS on_time_performance
    FROM
        `{project_id}.{dataset_id}.{fact_flight_table}` AS flights;"""

    QUERY_STEP_2 = """
    SELECT
        d.total_delay,
        d.on_time_performance,
        a.airport_name
    FROM
        airline.etl_step_1_delays_sql AS d
    JOIN
        `{project_id}.{dataset_id}.{dim_flight_table}` AS df ON d.flight_key = df.flight_key
    JOIN
    `{project_id}.{dataset_id}.{dim_airport_table}` AS a ON df.departure_airport_key = a.airport_key;"""

    QUERY_STEP_3 = """
    SELECT
        a.airport_name,
        AVG(a.total_delay) AS average_total_delay,
        AVG(a.on_time_performance) AS on_time_percentage
    FROM
        airline.etl_step_2_flight_delays_with_airports_sql AS a
    GROUP BY
        a.airport_name;"""

    fact_flight_table = base_tables['fact_flight_table']
    dim_flight_table = base_tables['dim_flight_table']
    dim_airport_table = base_tables['dim_airport_table']

    formatted_query_1 = QUERY_STEP_1.format(
        project_id=project_id,
        dataset_id=dataset_id,
        fact_flight_table=fact_flight_table
    )

    formatted_query_2 = QUERY_STEP_2.format(
        project_id=project_id,
        dataset_id=dataset_id,
        dim_flight_table=dim_flight_table,
        dim_airport_table=dim_airport_table
    )

    formatted_query_3 = QUERY_STEP_3

    airline_etl_step_1 = BigQueryInsertJobOperator(
        task_id="airline_etl_step_1",
        configuration={
            "query": {
                "query": formatted_query_1,
                "useLegacySql": False,
                "writeDisposition": "WRITE_TRUNCATE",
                'destinationTable': {
                    'projectId': project_id,
                    'datasetId': dataset_id,
                    'tableId': "etl_step_1_delays_sql"
                },
            }
        },
    )
    airline_etl_step_2 = BigQueryInsertJobOperator(
        task_id="airline_etl_step_2",
        configuration={
            "query": {
                "query": formatted_query_2,
                "useLegacySql": False,
                "writeDisposition": "WRITE_TRUNCATE",
                'destinationTable': {
                    'projectId': project_id,
                    'datasetId': dataset_id,
                    'tableId': "etl_step_2_flight_delays_with_airports_sql"
                },
            }
        },
    )
  
    airline_etl_step_3 = BigQueryInsertJobOperator(
        task_id="airline_etl_step_3",
        configuration={
            "query": {
                "query": formatted_query_3,
                "useLegacySql": False,
                "writeDisposition": "WRITE_TRUNCATE",
                'destinationTable': {
                    'projectId': project_id,
                    'datasetId': dataset_id,
                    'tableId': "etl_step_3_flight_delays_with_airports_sql"
                },
            }
        },
    )
    airline_etl_step_1 >> airline_etl_step_2 >> airline_etl_step_3
