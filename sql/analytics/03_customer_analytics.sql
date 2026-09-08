-- Business Query 03: Customer Intelligence & Segmentation
-- Platform: Fabric SQL Analytics Endpoint

-- 1. High-Value Customer Leaderboard
SELECT 
    c.customer_id,
    c.full_name,
    c.email,
    c.city,
    c.country,
    c.customer_segment,
    COUNT(DISTINCT f.order_id) AS lifetime_orders,
    SUM(f.sales_amount) AS total_customer_spend,
    AVG(f.sales_amount) AS avg_order_spend
FROM fact_sales f
JOIN dim_customer c ON f.customer_key = c.customer_key
WHERE c.is_current = True
GROUP BY c.customer_id, c.full_name, c.email, c.city, c.country, c.customer_segment
ORDER BY total_customer_spend DESC;

-- 2. Revenue by Customer Segment
SELECT 
    c.customer_segment,
    COUNT(DISTINCT c.customer_id) AS total_customers,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.sales_amount) AS segment_revenue
FROM fact_sales f
JOIN dim_customer c ON f.customer_key = c.customer_key
WHERE c.is_current = True
GROUP BY c.customer_segment
ORDER BY segment_revenue DESC;
