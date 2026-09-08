-- Business Query 04: Operations, Returns & Payment Success Analytics
-- Platform: Fabric SQL Analytics Endpoint

-- 1. Order Status Breakdown
SELECT 
    order_status,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(total_amount) AS status_total_value
FROM silver_orders
GROUP BY order_status
ORDER BY total_orders DESC;

-- 2. Return Rate by Product Category
SELECT 
    p.category_name,
    COUNT(DISTINCT r.order_id) AS total_returns,
    SUM(r.refund_amount) AS total_refunded_amount
FROM fact_returns r
JOIN dim_product p ON r.product_key = p.product_key
GROUP BY p.category_name
ORDER BY total_refunded_amount DESC;
