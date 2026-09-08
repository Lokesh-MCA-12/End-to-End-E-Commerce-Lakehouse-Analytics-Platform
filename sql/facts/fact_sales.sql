-- DDL Script: fact_sales
-- Platform: Fabric SQL Analytics Endpoint / T-SQL

CREATE TABLE fact_sales (
    sales_key VARCHAR(64) NOT NULL,
    order_id VARCHAR(50) NOT NULL,
    customer_key VARCHAR(64) NOT NULL,
    product_key VARCHAR(64) NOT NULL,
    date_key INT NOT NULL,
    quantity INT NOT NULL,
    unit_price DECIMAL(18,2) NOT NULL,
    discount DECIMAL(18,2) NOT NULL,
    sales_amount DECIMAL(18,2) NOT NULL,
    cost_amount DECIMAL(18,2) NOT NULL,
    profit_amount DECIMAL(18,2) NOT NULL
);
