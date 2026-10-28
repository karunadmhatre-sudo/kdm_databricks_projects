-- Databricks notebook source
CREATE OR REPLACE MATERIALIZED VIEW instacart.gold.product_performance
AS
SELECT 
    product_id,
    product_name,
    aisle_id,
    department_id,
    COUNT(order_id) AS total_times_ordered,
    COUNT(DISTINCT user_id) AS unique_customers_count,
    SUM(reordered) AS total_reorders,
    AVG(reordered) AS product_reorder_rate,
    AVG(add_to_cart_order) AS avg_add_to_cart_position
FROM instacart.gold.fact_order_items
GROUP BY product_id, product_name, aisle_id, department_id;