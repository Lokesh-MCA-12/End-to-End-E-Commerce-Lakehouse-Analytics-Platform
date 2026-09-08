-- Business Query 02: Product & Category Performance Analytics
-- Platform: Fabric SQL Analytics Endpoint

-- 1. Top 10 Products by Gross Revenue & Margin
SELECT 
    p.product_name,
    p.category_name,
    p.brand,
    SUM(f.quantity) AS total_units_sold,
    SUM(f.sales_amount) AS total_gross_revenue,
    SUM(f.profit_amount) AS total_net_profit,
    ROUND((SUM(f.profit_amount) / SUM(f.sales_amount)) * 100, 2) AS profit_margin_percentage
FROM fact_sales f
JOIN dim_product p ON f.product_key = p.product_key
GROUP BY p.product_name, p.category_name, p.brand
ORDER BY total_gross_revenue DESC;

-- 2. Category Performance Summary
SELECT 
    p.category_name,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.quantity) AS units_sold,
    SUM(f.sales_amount) AS category_revenue,
    SUM(f.profit_amount) AS category_profit
FROM fact_sales f
JOIN dim_product p ON f.product_key = p.product_key
GROUP BY p.category_name
ORDER BY category_revenue DESC;
