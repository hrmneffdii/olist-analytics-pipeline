
  create view "admin_db"."gold"."int_customer_orders__dbt_tmp"
    
    
  as (
    WITH orders AS (
    SELECT * FROM "admin_db"."gold"."stg_orders"
),

customers AS (
    SELECT * FROM "admin_db"."gold"."stg_customers"
),

time AS (
    SELECT * FROM "admin_db"."gold"."stg_time"
),

customer_metrics AS (
    SELECT
        c.customer_key,
        c.customer_id,
        c.customer_unique_id,
        c.customer_city,
        c.customer_state,

        COUNT(o.order_id)      AS total_orders,
        ROUND(SUM(o.price), 2)          AS total_spent,
        ROUND(AVG(o.price), 2)          AS avg_order_value,
        MIN(t.full_date)                AS first_order_date,
        MAX(t.full_date)                AS last_order_date,

        -- Segment
        CASE
            WHEN COUNT(o.order_id) = 1     THEN 'One-time Buyer'
            WHEN COUNT(o.order_id) <= 3    THEN 'Occasional Buyer'
            ELSE 'Loyal Buyer'
        END AS customer_segment

    FROM orders o
    LEFT JOIN customers c   ON o.customer_key  = c.customer_key
    LEFT JOIN time t        ON o.order_date_key = t.date_key
    WHERE o.order_status NOT IN ('canceled', 'unavailable')
    GROUP BY
        c.customer_key,
        c.customer_id,
        c.customer_unique_id,
        c.customer_city,
        c.customer_state
)

SELECT * FROM customer_metrics
  );