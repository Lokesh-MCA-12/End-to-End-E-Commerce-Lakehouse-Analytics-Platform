-- DDL Script: dim_customer (SCD Type 2)
-- Platform: Fabric SQL Analytics Endpoint / T-SQL

CREATE TABLE dim_customer (
    customer_key VARCHAR(64) NOT NULL,
    customer_id VARCHAR(50) NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    full_name VARCHAR(200),
    email VARCHAR(255),
    gender VARCHAR(20),
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100),
    customer_segment VARCHAR(50),
    effective_date DATE NOT NULL,
    expiry_date DATE NOT NULL,
    is_current BOOLEAN NOT NULL
);
