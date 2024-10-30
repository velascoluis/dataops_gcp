import datetime

from airflow import models
from airflow.operators.python_operator import PythonVirtualenvOperator


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


def bigframes_step_1(project_id: str, dataset_id: str, fact_flight_table: str):
    import bigframes.pandas as bpd
    flights_df = bpd.read_gbq(f"{project_id}.{dataset_id}.{fact_flight_table}")
    flights_df["total_delay"] = flights_df["departure_delay"].fillna(0) + flights_df["arrival_delay"].fillna(0)
    flights_df["on_time_performance"] = (flights_df["total_delay"] <= 0).astype("Int64")
    flights_df.to_gbq(
        f"{project_id}.{dataset_id}.etl_step_1_delays_bigframes",
        if_exists="replace",
    )


def bigframes_step_2(project_id: str, dataset_id: str, dim_flight_table: str, dim_airport_table: str):
    import bigframes.pandas as bpd
    delays_df = bpd.read_gbq(f"{project_id}.{dataset_id}.etl_step_1_delays_bigframes")
    flights_df = bpd.read_gbq(f"{project_id}.{dataset_id}.{dim_flight_table}")
    airports_df = bpd.read_gbq(f"{project_id}.{dataset_id}.{dim_airport_table}")
    merged_df = delays_df.merge(flights_df, on='flight_key', how='inner')
    merged_df = merged_df.merge(airports_df, left_on='departure_airport_key', right_on='airport_key', how='inner')
    result_df = merged_df[["total_delay", "on_time_performance", "airport_name"]]
    result_df.to_gbq(
        f"{project_id}.{dataset_id}.etl_step_2_flight_delays_with_airports_bigframes",
        if_exists="replace",
    )


def bigframes_step_3(project_id: str, dataset_id: str):
    import bigframes.pandas as bpd
    df = bpd.read_gbq(f"{project_id}.{dataset_id}.etl_step_2_flight_delays_with_airports_bigframes")
    result_df = df.groupby('airport_name').agg({
        'total_delay': 'mean',
        'on_time_performance': 'mean'
    }).reset_index()
    result_df = result_df.rename(columns={
        'total_delay': 'average_total_delay',
        'on_time_performance': 'on_time_percentage'
    })
    result_df.to_gbq(
        f"{project_id}.{dataset_id}.etl_step_3_flight_delays_with_airports_bigframes",
        if_exists="replace",
    )


with models.DAG(
    "composer_airline_dag_bigframes",
    catchup=False,
    default_args=default_args,
    schedule_interval=datetime.timedelta(days=1),
) as dag:

    project_id = "velascoluis-dev-sandbox"
    dataset_id = "airline"
    base_tables = {
        "fact_flight_table": "fact_flight",
        "dim_flight_table": "dim_flight",
        "dim_airport_table": "dim_airport",
    }

    fact_flight_table = base_tables["fact_flight_table"]
    dim_flight_table = base_tables["dim_flight_table"]
    dim_airport_table = base_tables["dim_airport_table"]

    airline_etl_step_1 = PythonVirtualenvOperator(
        task_id="airline_etl_step_1",
        python_callable=bigframes_step_1,
        requirements=["bigframes"],
        op_kwargs={
            "project_id": project_id,
            "dataset_id": dataset_id,
            "fact_flight_table": fact_flight_table,
        },
    )

    airline_etl_step_2 = PythonVirtualenvOperator(
        task_id="airline_etl_step_2",
        python_callable=bigframes_step_2,
        requirements=["bigframes"],
        op_kwargs={
            "project_id": project_id,
            "dataset_id": dataset_id,
            "dim_flight_table": dim_flight_table,
            "dim_airport_table": dim_airport_table,
        },
    )

    airline_etl_step_3 = PythonVirtualenvOperator(
        task_id="airline_etl_step_3",
        python_callable=bigframes_step_3,
        requirements=["bigframes"],
        op_kwargs={
            "project_id": project_id,
            "dataset_id": dataset_id,
        },
    )

    airline_etl_step_1 >> airline_etl_step_2 >> airline_etl_step_3
