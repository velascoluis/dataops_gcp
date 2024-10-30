--create schema
CREATE SCHEMA IF NOT EXISTS 
  airline OPTIONS(
    location="us-central1");


-- dim_date
CREATE OR REPLACE TABLE `velascoluis-dev-sandbox.airline.dim_date` (
    date_key INT64 PRIMARY KEY NOT ENFORCED,
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
);

-- dim_time
CREATE OR REPLACE TABLE `velascoluis-dev-sandbox.airline.dim_time` (
    time_key INT64 PRIMARY KEY NOT ENFORCED,,
    time_of_day TIME,
    hour INT64,
    minute INT64,
    second INT64,
    time_of_day_24 STRING
);

-- dim_airport
CREATE OR REPLACE TABLE `velascoluis-dev-sandbox.airline.dim_airport` (
    airport_key INT64 PRIMARY KEY NOT ENFORCED,
    airport_code STRING NOT NULL,
    airport_name STRING,
    city STRING,
    country STRING,
    latitude NUMERIC,
    longitude NUMERIC,
    time_zone STRING
);

-- dim_aircraft
CREATE OR REPLACE TABLE `velascoluis-dev-sandbox.airline.dim_aircraft` (
    aircraft_key INT64 PRIMARY KEY NOT ENFORCED,
    aircraft_model STRING,
    manufacturer STRING,
    capacity INT64
);

-- dim_customer
CREATE OR REPLACE TABLE `velascoluis-dev-sandbox.airline.dim_customer` (
    customer_key INT64 PRIMARY KEY NOT ENFORCED,
    customer_id STRING NOT NULL,
    first_name STRING,
    last_name STRING,
    email STRING,
    phone STRING,
    date_of_birth DATE,
    gender STRING,
    address STRING,
    city STRING,
    country STRING
);

-- dim_flight
CREATE OR REPLACE TABLE `velascoluis-dev-sandbox.airline.dim_flight` (
    flight_key INT64 PRIMARY KEY NOT ENFORCED,
    flight_number STRING NOT NULL,
    departure_airport_key INT64,
    arrival_airport_key INT64,
    scheduled_departure_time_key INT64,
    scheduled_arrival_time_key INT64,
    aircraft_key INT64,
    distance NUMERIC,
    FOREIGN KEY (departure_airport_key) REFERENCES `velascoluis-dev-sandbox.airline.dim_airport`(airport_key) NOT ENFORCED,
    FOREIGN KEY (arrival_airport_key) REFERENCES `velascoluis-dev-sandbox.airline.dim_airport`(airport_key) NOT ENFORCED,
    FOREIGN KEY (scheduled_departure_time_key) REFERENCES `velascoluis-dev-sandbox.airline.dim_time`(time_key) NOT ENFORCED,
    FOREIGN KEY (scheduled_arrival_time_key) REFERENCES `velascoluis-dev-sandbox.airline.dim_time`(time_key) NOT ENFORCED,
    FOREIGN KEY (aircraft_key) REFERENCES `velascoluis-dev-sandbox.airline.dim_aircraft`(aircraft_key) NOT ENFORCED,

);

-- fact_booking
CREATE OR REPLACE TABLE `velascoluis-dev-sandbox.airline.fact_booking` (
    booking_key INT64 PRIMARY KEY NOT ENFORCED,
    booking_date_key INT64,
    customer_key INT64,
    flight_key INT64,
    booking_channel STRING,
    booking_status STRING,
    number_of_tickets INT64,
    total_fare NUMERIC,
    FOREIGN KEY (booking_date_key) REFERENCES `velascoluis-dev-sandbox.airline.dim_date`(date_key) NOT ENFORCED,
    FOREIGN KEY (customer_key) REFERENCES `velascoluis-dev-sandbox.airline.dim_customer`(customer_key) NOT ENFORCED,
    FOREIGN KEY (flight_key) REFERENCES `velascoluis-dev-sandbox.airline.dim_flight`(flight_key) NOT ENFORCED

);

-- fact_flight
CREATE OR REPLACE TABLE `velascoluis-dev-sandbox.airline.fact_flight` (
    flight_key INT64,
    actual_departure_time_key INT64,
    actual_arrival_time_key INT64,
    departure_delay INT64,
    arrival_delay INT64,
    is_cancelled BOOL,
    is_diverted BOOL,
    fuel_consumption NUMERIC,
    passenger_count INT64,
    baggage_count INT64,
    FOREIGN KEY (flight_key) REFERENCES `velascoluis-dev-sandbox.airline.dim_flight`(flight_key) NOT ENFORCED,
    FOREIGN KEY (actual_departure_time_key) REFERENCES `velascoluis-dev-sandbox.airline.dim_time`(time_key) NOT ENFORCED,
    FOREIGN KEY (actual_arrival_time_key) REFERENCES `velascoluis-dev-sandbox.airline.dim_time`(time_key) NOT ENFORCED

);
