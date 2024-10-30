from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import argparse

def create_spark_session():
    return (SparkSession.builder
            .appName("airline_etl")
            .getOrCreate())

def step1_process_delays(spark, project_id, dataset_id):
    """Calculate total delays and on-time performance for each flight"""
    
    # Read the fact_flight table
    fact_flights = spark.read.format('bigquery') \
        .option('table', f'{project_id}.{dataset_id}.fact_flight') \
        .load()
    
    # Calculate delays and on-time performance
    delays_df = fact_flights.select(
        'flight_key',
        (F.coalesce(F.col('departure_delay'), F.lit(0)) + 
         F.coalesce(F.col('arrival_delay'), F.lit(0))).alias('total_delay'),
        F.when(
            (F.coalesce(F.col('departure_delay'), F.lit(0)) + 
             F.coalesce(F.col('arrival_delay'), F.lit(0))) <= 0,
            1
        ).otherwise(0).alias('on_time_performance')
    )
    
    # Write results to BigQuery
    delays_df.write.format('bigquery').option("writeMethod", "direct") \
        .option('table', f'{project_id}.{dataset_id}.etl_step_1_delays_sql') \
        .mode('overwrite') \
        .save()


def step2_join_airports(spark, project_id, dataset_id):
    """Join delay information with flight and airport data"""
    
    # Read necessary tables
    delays_df = spark.read.format('bigquery') \
        .option('table', f'{project_id}.{dataset_id}.etl_step_1_delays_sql') \
        .load()
    
    dim_flights = spark.read.format('bigquery') \
        .option('table', f'{project_id}.{dataset_id}.dim_flight') \
        .load()
    
    dim_airports = spark.read.format('bigquery') \
        .option('table', f'{project_id}.{dataset_id}.dim_airport') \
        .load()
    
    # Join tables
    result_df = delays_df.join(
        dim_flights,
        delays_df.flight_key == dim_flights.flight_key
    ).join(
        dim_airports,
        dim_flights.departure_airport_key == dim_airports.airport_key
    ).select(
        'total_delay',
        'on_time_performance',
        'airport_name'
    )
    
    # Write results to BigQuery
    result_df.write.format('bigquery').option("writeMethod", "direct") \
        .option('table', f'{project_id}.{dataset_id}.etl_step_2_flight_delays_with_airports_sql') \
        .mode('overwrite') \
        .save()

def step3_calculate_metrics(spark, project_id, dataset_id):
    """Calculate average delays and on-time performance by airport"""
    
    # Read the previous step's results
    flights_airports = spark.read.format('bigquery') \
        .option('table', f'{project_id}.{dataset_id}.etl_step_2_flight_delays_with_airports_sql') \
        .load()
    
    # Calculate metrics
    metrics_df = flights_airports.groupBy('airport_name').agg(
        F.avg('total_delay').alias('average_total_delay'),
        F.avg('on_time_performance').alias('on_time_percentage')
    )
    
    # Write final results to BigQuery
    metrics_df.write.format('bigquery').option("writeMethod", "direct") \
        .option('table', f'{project_id}.{dataset_id}.etl_step_3_flight_delays_with_airports_sql') \
        .mode('overwrite') \
        .save()



def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--project_id', required=True)
    parser.add_argument('--dataset_id', required=True)
    parser.add_argument('--step', type=int, required=True)
    
    args = parser.parse_args()
    
    spark = create_spark_session()
    
    # Execute the appropriate step based on the argument
    if args.step == 1:
        step1_process_delays(spark, args.project_id, args.dataset_id)
    elif args.step == 2:
        step2_join_airports(spark, args.project_id, args.dataset_id)
    elif args.step == 3:
        step3_calculate_metrics(spark, args.project_id, args.dataset_id)
    else:
        raise ValueError("Step must be 1, 2, or 3")
    
    spark.stop()

if __name__ == '__main__':
    main()
