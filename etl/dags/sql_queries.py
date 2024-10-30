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
