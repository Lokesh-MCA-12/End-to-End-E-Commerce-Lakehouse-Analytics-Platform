-- DDL Script: dim_product
-- Platform: Fabric SQL Analytics Endpoint / T-SQL

CREATE TABLE dim_product (
    product_key VARCHAR(64) NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    category_id INT NOT NULL,
    category_name VARCHAR(100) NOT NULL,
    brand VARCHAR(100),
    unit_price DECIMAL(18,2) NOT NULL,
    cost_price DECIMAL(18,2) NOT NULL,
    supplier VARCHAR(150)
);
