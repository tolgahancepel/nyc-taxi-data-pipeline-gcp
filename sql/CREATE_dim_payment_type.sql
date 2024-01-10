/*
File: CREATE_dim_payment_type.sql
Author: Tolgahan Cepel
Email: tolgahan.cepel@gmail.com
Date: January 1, 2024
Description: Create DDL script for dim_payment_type table.
*/

CREATE TABLE dim_dataset.dim_payment_type (
    payment_type_id INT64,
    payment_type_name STRING
);