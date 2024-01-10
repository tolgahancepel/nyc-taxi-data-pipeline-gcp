/*
File: CREATE_dim_tpep_vendor.sql
Author: Tolgahan Cepel
Email: tolgahan.cepel@gmail.com
Date: January 1, 2024
Description: Create DDL script for dim_tpep_vendor table.
*/

CREATE TABLE dim_dataset.dim_tpep_vendor (
    vendor_id INT64,
    vendor_name STRING
);