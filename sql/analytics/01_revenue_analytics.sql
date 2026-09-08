-- Business Query 01: Financial & Revenue Analytics
-- Platform: Fabric SQL Analytics Endpoint

-- 1. Total Cumulative Revenue, Orders, Profit & AOV
SELECT 
    SUM(sales_amount) AS total_revenue,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(sales_amount) / COUNT(DISTINCT order_id) AS average_order_value,
    SUM(profit_amount) AS total_profit,
    (SUM(profit_amount) / SUM(sales_amount)) * 100 AS overall_profit_margin_pct
FROM fact_sales;

-- 2. Monthly Revenue Trend & Growth Velocity
SELECT 
    d.year,
    d.month,
    d.month_name,
    SUM(f.sales_amount) AS monthly_revenue,
    SUM(f.profit_amount) AS monthly_profit,
    COUNT(DISTINCT f.order_id) AS monthly_orders
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;
