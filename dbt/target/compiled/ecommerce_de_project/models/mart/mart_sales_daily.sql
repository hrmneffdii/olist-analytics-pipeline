WITH base AS (
    SELECT * FROM "admin_db"."gold"."int_order_items_joined"
    WHERE order_status NOT IN ('canceled', 'unavailable')
),

daily AS (
    SELECT
        order_date,
        year,
        month,
        month_name,
        quarter,
        day_name,
        is_weekend,

        COUNT(DISTINCT order_id)        AS total_orders,
        COUNT(order_item_key)           AS total_items,
        ROUND(SUM(price), 2)            AS total_revenue,
        ROUND(SUM(freight_value), 2)    AS total_freight,
        ROUND(SUM(payment_value), 2)    AS total_payment,
        ROUND(AVG(price), 2)            AS avg_order_value,
        ROUND(AVG(review_score), 2)     AS avg_review_score
    FROM base
    GROUP BY
        order_date, year, month,
        month_name, quarter,
        day_name, is_weekend
)

SELECT * FROM daily