import datetime

from airflow import models
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator


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
}


with models.DAG(
    "composer_airline_dag_pyspark",
    catchup=False,
    default_args=default_args,
    schedule_interval=datetime.timedelta(days=1),
) as dag:

    project_id = "velascoluis-dev-sandbox"
    bucket_name = "us-central1-airline-compose-c26b5e81-bucket/dags"
    region = "us-central1"
    dataset_id = "airline"
    base_tables = {
        "fact_flight_table": "fact_flight",
        "dim_flight_table": "dim_flight",
        "dim_airport_table": "dim_airport",
    }

    airline_etl_step_1 = DataprocCreateBatchOperator(
        task_id="airline_etl_step_1",
        project_id=project_id,
        region=region,
        batch={
            "pyspark_batch": {
                "main_python_file_uri": f"gs://{bucket_name}/airline_dag_pyspark_etl.py",
                "args": [
                    f"--project_id={project_id}",
                    "--dataset_id=airline",
                    "--step=1",
                ]
            },
        },
    )
    airline_etl_step_2 = DataprocCreateBatchOperator(
        task_id="airline_etl_step_2",
        project_id=project_id,
        region=region,
        batch={
            "pyspark_batch": {
                "main_python_file_uri": f"gs://{bucket_name}/airline_dag_pyspark_etl.py",
                "args": [
                    f"--project_id={project_id}",
                    "--dataset_id=airline",
                    "--step=2",
                ]
            },
        },
    )

    airline_etl_step_3 = DataprocCreateBatchOperator(
        task_id="airline_etl_step_3",
        project_id=project_id,
        region=region,
        batch={
            "pyspark_batch": {
                "main_python_file_uri": f"gs://{bucket_name}/airline_dag_pyspark_etl.py",
                "args": [
                    f"--project_id={project_id}",
                    "--dataset_id=airline",
                    "--step=3",
                ]
            },
        },
    )
    airline_etl_step_1 >> airline_etl_step_2 >> airline_etl_step_3
