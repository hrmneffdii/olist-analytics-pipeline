WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

sellers AS (
    SELECT * FROM {{ ref('stg_sellers') }}
),

products AS (
    SELECT * FROM {{ ref('stg_products') }}
),

time AS (
    SELECT * FROM {{ ref('stg_time') }}
),

joined AS (
    SELECT
        -- Keys
        o.order_item_key,
        o.order_id,
        o.order_item_id,

        -- Customer info
        c.customer_id,
        c.customer_unique_id,
        c.customer_city,
        c.customer_state,

        -- Seller info
        s.seller_id,
        s.seller_city,
        s.seller_state,

        -- Product info
        p.product_id,
        p.product_category_name,

        -- Time info
        t.full_date          AS order_date,
        t.day_of_week,
        t.day_name,
        t.month,
        t.month_name,
        t.quarter,
        t.year,
        t.is_weekend,

        -- Metrics
        o.order_status,
        o.price,
        o.freight_value,
        o.payment_value,
        o.payment_type,
        o.review_score,
        o.is_late_delivery,
        o.delivery_days

    FROM orders o
    LEFT JOIN customers c   ON o.customer_key   = c.customer_key
    LEFT JOIN sellers s     ON o.seller_key      = s.seller_key
    LEFT JOIN products p    ON o.product_key     = p.product_key
    LEFT JOIN time t        ON o.order_date_key  = t.date_key
)

SELECT * FROM joined