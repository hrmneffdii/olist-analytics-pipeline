WITH base AS (
    SELECT * FROM {{ ref('int_order_items_joined') }}
    WHERE order_status NOT IN ('canceled', 'unavailable')
),

seller_metrics AS (
    SELECT
        seller_id,
        seller_city,
        seller_state,

        COUNT(DISTINCT order_id)            AS total_orders,
        COUNT(order_item_key)               AS total_items_sold,
        ROUND(SUM(price), 2)                AS total_revenue,
        ROUND(AVG(price), 2)                AS avg_order_value,
        ROUND(AVG(review_score), 2)         AS avg_review_score,

        SUM(CASE WHEN is_late_delivery
            THEN 1 ELSE 0 END)              AS total_late_deliveries,

        ROUND(
            SUM(CASE WHEN is_late_delivery
                THEN 1 ELSE 0 END)::NUMERIC
            / NULLIF(COUNT(*), 0) * 100
        , 2)                                AS late_delivery_pct,

        ROUND(AVG(delivery_days), 1)        AS avg_delivery_days

    FROM base
    GROUP BY seller_id, seller_city, seller_state
)

SELECT
    RANK() OVER (
        ORDER BY total_revenue DESC
    )                                       AS revenue_rank,
    *
FROM seller_metrics