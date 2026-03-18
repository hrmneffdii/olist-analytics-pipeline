WITH base AS (
    SELECT * FROM {{ ref('int_order_items_joined') }}
    WHERE order_status = 'delivered'
    AND delivery_days IS NOT NULL
),

state_metrics AS (
    SELECT
        customer_state,

        COUNT(DISTINCT order_id)            AS total_orders,
        ROUND(AVG(delivery_days), 1)        AS avg_delivery_days,

        SUM(CASE WHEN is_late_delivery
            THEN 1 ELSE 0 END)              AS total_late,

        ROUND(
            SUM(CASE WHEN is_late_delivery
                THEN 1 ELSE 0 END)::NUMERIC
            / NULLIF(COUNT(*), 0) * 100
        , 2)                                AS late_delivery_pct,

        ROUND(AVG(review_score), 2)         AS avg_review_score,
        ROUND(SUM(price), 2)                AS total_revenue,

        CASE
            WHEN ROUND(
                SUM(CASE WHEN is_late_delivery
                    THEN 1 ELSE 0 END)::NUMERIC
                / NULLIF(COUNT(*), 0) * 100
            , 2) >= 20  THEN 'Poor'
            WHEN ROUND(
                SUM(CASE WHEN is_late_delivery
                    THEN 1 ELSE 0 END)::NUMERIC
                / NULLIF(COUNT(*), 0) * 100
            , 2) >= 10  THEN 'Average'
            ELSE 'Good'
        END                                 AS delivery_performance

    FROM base
    GROUP BY customer_state
)

SELECT * FROM state_metrics
ORDER BY late_delivery_pct DESC