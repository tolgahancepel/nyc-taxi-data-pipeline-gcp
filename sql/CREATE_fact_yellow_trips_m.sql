/*
File: CREATE_fact_yellow_trips_m.sql
Author: Tolgahan Cepel
Email: tolgahan.cepel@gmail.com
Date: January 1, 2024
Description: Create DDL script for fact_yellow_trips_m table.
*/

CREATE TABLE fact_dataset.fact_yellow_trips_m (
    vendor_id INT64,
    rate_code_id FLOAT64,
    store_and_fwd_flag STRING,
    pu_location_id INT64,
    do_location_id INT64,
    payment_type INT64,
    passenger_count FLOAT64,
    trip_distance FLOAT64,
    fare_amount FLOAT64,
    extra FLOAT64,
    mta_tax FLOAT64,
    tip_amount FLOAT64,
    tolls_amount FLOAT64,
    improvement_surcharge FLOAT64,
    total_amount FLOAT64
);
