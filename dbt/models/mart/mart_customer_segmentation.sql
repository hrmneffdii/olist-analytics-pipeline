WITH base AS (
    SELECT * FROM {{ ref('int_customer_orders') }}
),

segmented AS (
    SELECT
        customer_segment,

        COUNT(*)                            AS total_customers,
        ROUND(
            COUNT(*)::NUMERIC
            / SUM(COUNT(*)) OVER () * 100
        , 2)                                AS pct_of_customers,

        ROUND(SUM(total_spent), 2)          AS total_revenue,
        ROUND(
            SUM(total_spent)
            / SUM(SUM(total_spent)) OVER () * 100
        , 2)                                AS pct_of_revenue,

        ROUND(AVG(total_spent), 2)          AS avg_spent_per_customer,
        ROUND(AVG(avg_order_value), 2)      AS avg_order_value,
        ROUND(AVG(total_orders), 1)         AS avg_orders_per_customer

    FROM base
    GROUP BY customer_segment
)

SELECT * FROM segmented
ORDER BY total_revenue DESC