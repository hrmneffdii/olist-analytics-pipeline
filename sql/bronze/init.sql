-- create database for airflow metadata
CREATE DATABASE airflow_db;

-- create medallion schema
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- create bronze.orders table
CREATE TABLE IF NOT EXISTS bronze.orders (
    order_id VARCHAR(50),
    customer_id VARCHAR(50),
    order_status VARCHAR(20),
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP,
    ingested_at TIMESTAMP DEFAULT NOW()
);

-- create bronze.order_items table
CREATE TABLE IF NOT EXISTS bronze.order_items (
    order_id VARCHAR(50),
    order_item_id INTEGER,
    product_id VARCHAR(50),
    seller_id VARCHAR(50),
    shipping_limit_date TIMESTAMP,
    price DECIMAL(10,2),
    freight_value DECIMAL(10,2),
    ingested_at TIMESTAMP DEFAULT NOW()
);

-- create bronze.customers table
CREATE TABLE IF NOT EXISTS bronze.customers (
    customer_id VARCHAR(50),
    customer_unique_id VARCHAR(50),
    customer_zip_code_prefix VARCHAR(10),
    customer_city VARCHAR(100),
    customer_state VARCHAR(5),
    ingested_at TIMESTAMP DEFAULT NOW()
);

-- create bronze.products table
CREATE TABLE IF NOT EXISTS bronze.products (
    product_id VARCHAR(50),
    product_category_name VARCHAR(100),
    product_name_lenght INTEGER,
    product_description_lenght INTEGER,
    product_photos_qty INTEGER,
    product_weight_g DECIMAL(10,2),
    product_length_cm DECIMAL(10,2),
    product_height_cm DECIMAL(10,2),
    product_width_cm DECIMAL(10,2),
    ingested_at TIMESTAMP DEFAULT NOW()
);

-- create bronze.sellers table
CREATE TABLE IF NOT EXISTS bronze.sellers (
    seller_id VARCHAR(50),
    seller_zip_code_prefix VARCHAR(10),
    seller_city VARCHAR(100),
    seller_state VARCHAR(5),
    ingested_at TIMESTAMP DEFAULT NOW()
);

-- create bronze.order_payments table
CREATE TABLE IF NOT EXISTS bronze.order_payments (
    order_id VARCHAR(50),
    payment_sequential INTEGER,
    payment_type VARCHAR(30),
    payment_installments INTEGER,
    payment_value DECIMAL(10,2),
    ingested_at TIMESTAMP DEFAULT NOW()
);

-- create bronze.order_reviews table
CREATE TABLE IF NOT EXISTS bronze.order_reviews (
    review_id VARCHAR(50),
    order_id VARCHAR(50),
    review_score INTEGER,
    review_comment_title TEXT,
    review_comment_message TEXT,
    review_creation_date TIMESTAMP,
    review_answer_timestamp TIMESTAMP,
    ingested_at TIMESTAMP DEFAULT NOW()
);