/*
File: CREATE_dim_rate_code.sql
Author: Tolgahan Cepel
Email: tolgahan.cepel@gmail.com
Date: January 1, 2024
Description: Create DDL script for dim_rate_code table.
*/

CREATE TABLE dim_dataset.dim_rate_code (
    rate_code_id INT64,
    rate_code_name STRING
);