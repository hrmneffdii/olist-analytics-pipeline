-- FACT ORDERS PIPELINE
-- Grain: 1 row = 1 order item

-- 1. CREATE STAGING TABLE


CREATE TABLE IF NOT EXISTS silver.stg_fact_orders (

    order_id                        TEXT,
    order_item_id                   INT,

    customer_id                     TEXT,
    product_id                      TEXT,
    seller_id                       TEXT,

    order_purchase_timestamp        TIMESTAMP,
    order_status                    TEXT,

    price                           NUMERIC(10,2),
    freight_value                   NUMERIC(10,2),
    payment_value                   NUMERIC(10,2),
    payment_type                    TEXT,

    review_score                    INT,

    is_late_delivery                BOOLEAN,
    delivery_days                   INT,

    ingested_at                     TIMESTAMP

);


-- RESET STAGING

TRUNCATE TABLE silver.stg_fact_orders;


-- 2. INCREMENTAL LOAD FROM BRONZE → STAGING

INSERT INTO silver.stg_fact_orders (

    order_id,
    order_item_id,
    customer_id,
    product_id,
    seller_id,
    order_purchase_timestamp,
    order_status,
    price,
    freight_value,
    payment_value,
    payment_type,
    review_score,
    is_late_delivery,
    delivery_days,
    ingested_at

)

WITH bronze_incremental AS (

    SELECT
        oi.order_id,
        oi.order_item_id,
        o.customer_id,
        oi.product_id,
        oi.seller_id,
        o.order_purchase_timestamp,
        o.order_status,
        oi.price,
        oi.freight_value,

        op.payment_value,
        op.payment_type,

        r.review_score,

        CASE
            WHEN o.order_delivered_customer_date > o.order_estimated_delivery_date
            THEN TRUE
            ELSE FALSE
        END AS is_late_delivery,

        DATE_PART(
            'day',
            o.order_delivered_customer_date - o.order_purchase_timestamp
        )::INT AS delivery_days,

        oi.ingested_at

    FROM bronze.order_items oi

    INNER JOIN bronze.orders o
        ON oi.order_id = o.order_id

    LEFT JOIN bronze.order_payments op
        ON oi.order_id = op.order_id
        AND op.payment_sequential = 1

    LEFT JOIN bronze.order_reviews r
        ON oi.order_id = r.order_id

    WHERE oi.ingested_at >= (
        SELECT COALESCE(MAX(ingested_at), TIMESTAMP '1900-01-01')
        FROM silver.stg_fact_orders
    )

)

SELECT
    order_id,
    order_item_id,
    customer_id,
    product_id,
    seller_id,
    order_purchase_timestamp,
    order_status,
    price,
    freight_value,
    payment_value,
    payment_type,
    review_score,
    is_late_delivery,
    delivery_days,
    ingested_at
FROM bronze_incremental;


-- 4. CREATE FACT TABLE

CREATE TABLE IF NOT EXISTS silver.fact_orders (

    order_item_key      BIGINT PRIMARY KEY,

    order_id            TEXT        NOT NULL,
    order_item_id       INT         NOT NULL,

    customer_key        BIGINT,
    product_key         BIGINT,
    seller_key          BIGINT,
    order_date_key      INT,

    order_status        TEXT,

    price               NUMERIC(10,2),
    freight_value       NUMERIC(10,2),
    payment_value       NUMERIC(10,2),
    payment_type        TEXT,

    review_score        INT,

    is_late_delivery    BOOLEAN,
    delivery_days       INT,

    ingested_at         TIMESTAMP

);


-- 5. CREATE SURROGATE KEY SEQUENCE

CREATE SEQUENCE IF NOT EXISTS silver.order_item_key_seq;


-- 6. INSERT INTO FACT TABLE WITH DIMENSION LOOKUP
-- Idempotent: pakai NOT EXISTS dengan natural key

INSERT INTO silver.fact_orders (

    order_item_key,
    order_id,
    order_item_id,
    customer_key,
    product_key,
    seller_key,
    order_date_key,
    order_status,
    price,
    freight_value,
    payment_value,
    payment_type,
    review_score,
    is_late_delivery,
    delivery_days,
    ingested_at

)

SELECT

    nextval('silver.order_item_key_seq'),

    f.order_id,
    f.order_item_id,

    c.customer_key,
    p.product_key,
    s.seller_key,
    d.date_key          AS order_date_key,

    f.order_status,
    f.price,
    f.freight_value,
    f.payment_value,
    f.payment_type,
    f.review_score,
    f.is_late_delivery,
    f.delivery_days,
    f.ingested_at

FROM silver.stg_fact_orders f

INNER JOIN silver.dim_customers c
    ON f.customer_id = c.customer_id

INNER JOIN silver.dim_products p
    ON f.product_id = p.product_id

INNER JOIN silver.dim_sellers s
    ON f.seller_id = s.seller_id

INNER JOIN silver.dim_time d
    ON f.order_purchase_timestamp::DATE = d.full_date

WHERE NOT EXISTS (
    SELECT 1
    FROM silver.fact_orders existing
    WHERE existing.order_id       = f.order_id
    AND   existing.order_item_id  = f.order_item_id
);


-- 7. INDEXES

CREATE INDEX IF NOT EXISTS idx_fact_orders_customer
    ON silver.fact_orders(customer_key);

CREATE INDEX IF NOT EXISTS idx_fact_orders_product
    ON silver.fact_orders(product_key);

CREATE INDEX IF NOT EXISTS idx_fact_orders_seller
    ON silver.fact_orders(seller_key);

CREATE INDEX IF NOT EXISTS idx_fact_orders_date
    ON silver.fact_orders(order_date_key);

CREATE INDEX IF NOT EXISTS idx_fact_orders_customer_date
    ON silver.fact_orders(customer_key, order_date_key);

CREATE INDEX IF NOT EXISTS idx_fact_orders_natural_key
    ON silver.fact_orders(order_id, order_item_id);