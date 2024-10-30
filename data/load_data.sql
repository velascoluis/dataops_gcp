-- dim_date
LOAD DATA INTO `velascoluis-dev-sandbox.airline.dim_date` (
    date_key INT64,
    date DATE,
    day INT64,
    month INT64,
    year INT64,
    day_of_week STRING,
    day_name STRING,
    month_name STRING,
    quarter INT64,
    year_month STRING,
    is_weekend BOOL
)
FROM FILES (
    format = 'CSV',
    skip_leading_rows=1,
    uris = ['gs://velascoluis-dev-sandbox-bucket/airline/dim_date.csv']
);

-- dim_time
LOAD DATA INTO `velascoluis-dev-sandbox.airline.dim_time` (
    time_key INT64,
    time_of_day TIME,
    hour INT64,
    minute INT64,
    second INT64,
    time_of_day_24 STRING
)
FROM FILES (
     skip_leading_rows=1,
    format = 'CSV',
    uris = ['gs://velascoluis-dev-sandbox-bucket/airline/dim_time.csv']
);

-- dim_airport
LOAD DATA INTO `velascoluis-dev-sandbox.airline.dim_airport` (
    airport_key INT64,
    airport_code STRING,
    airport_name STRING,
    city STRING,
    country STRING,
    latitude NUMERIC,
    longitude NUMERIC,
    time_zone STRING
)
FROM FILES (
     skip_leading_rows=1,
    format = 'CSV',
    uris = ['gs://velascoluis-dev-sandbox-bucket/airline/dim_airport.csv']
);

-- dim_aircraft
LOAD DATA INTO `velascoluis-dev-sandbox.airline.dim_aircraft` (
    aircraft_key INT64,
    aircraft_model STRING,
    manufacturer STRING,
    capacity INT64
)
FROM FILES (
     skip_leading_rows=1,
    format = 'CSV',
    uris = ['gs://velascoluis-dev-sandbox-bucket/airline/dim_aircraft.csv']
);

-- dim_customer
LOAD DATA INTO `velascoluis-dev-sandbox.airline.dim_customer` (
    customer_key INT64,
    customer_id STRING,
    first_name STRING,
    last_name STRING,
    email STRING,
    phone STRING,
    date_of_birth DATE,
    gender STRING,
    address STRING,
    city STRING,
    country STRING
)
FROM FILES (
     skip_leading_rows=1,
    format = 'CSV',
    uris = ['gs://velascoluis-dev-sandbox-bucket/airline/dim_customer.csv']
);

-- dim_flight
LOAD DATA INTO `velascoluis-dev-sandbox.airline.dim_flight` (
    flight_key INT64,
    flight_number STRING,
    departure_airport_key INT64,
    arrival_airport_key INT64,
    scheduled_departure_time_key INT64,
    scheduled_arrival_time_key INT64,
    aircraft_key INT64,
    distance NUMERIC
)
FROM FILES (
     skip_leading_rows=1,
    format = 'CSV',
    uris = ['gs://velascoluis-dev-sandbox-bucket/airline/dim_flight.csv']
);

-- fact_booking
LOAD DATA INTO `velascoluis-dev-sandbox.airline.fact_booking` (
    booking_key INT64,
    booking_date_key INT64,
    customer_key INT64,
    flight_key INT64,
    booking_channel STRING,
    booking_status STRING,
    number_of_tickets INT64,
    total_fare NUMERIC
)
FROM FILES (
     skip_leading_rows=1,
    format = 'CSV',
    uris = ['gs://velascoluis-dev-sandbox-bucket/airline/fact_booking.csv']
);

-- fact_flight
LOAD DATA INTO `velascoluis-dev-sandbox.airline.fact_flight` (
    flight_key INT64,
    actual_departure_time_key INT64,
    actual_arrival_time_key INT64,
    departure_delay INT64,
    arrival_delay INT64,
    is_cancelled BOOL,
    is_diverted BOOL,
    fuel_consumption NUMERIC,
    passenger_count INT64,
    baggage_count INT64
)
FROM FILES (
     skip_leading_rows=1,
    format = 'CSV',
    uris = ['gs://velascoluis-dev-sandbox-bucket/airline/fact_flight.csv']
);