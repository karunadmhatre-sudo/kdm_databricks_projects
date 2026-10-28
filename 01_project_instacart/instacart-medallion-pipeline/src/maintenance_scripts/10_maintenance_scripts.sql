-- Databricks notebook source
-- =====================================================================
-- Maintenance Script: Apply Liquid Clustering to Silver & Gold Tables
-- Description: Run this script separately from the main ETL pipeline 
--              to cluster Delta tables for optimized analytical querying.
-- =====================================================================

-- 1. Switch to your target catalog and database (update as needed)
USE CATALOG instacart;
USE SCHEMA silver;

-- =====================================================================
-- SILVER LAYER CLUSTERING
-- =====================================================================

-- Cluster the heavy historical order-products transactional mapping table
ALTER TABLE instacart.silver.order_products_prior 
CLUSTER BY (order_id, product_id);

-- Cluster orders by user and timeline for customer behavior analysis
ALTER TABLE instacart.silver.orders 
CLUSTER BY (order_id, user_id);

-- =====================================================================
-- GOLD LAYER CLUSTERING
-- =====================================================================

-- Cluster user profiles by user_id for individual profile lookups
ALTER TABLE instacart.gold.fact_order_items 
CLUSTER BY (user_id, product_id);



-- =====================================================================
-- RUN OPTIMIZE (Triggers physical layout clustering on existing data)
-- =====================================================================
OPTIMIZE instacart.silver.order_products_prior;
OPTIMIZE instacart.silver.orders;
OPTIMIZE instacart.gold.fact_order_items;
