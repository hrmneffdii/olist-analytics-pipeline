WITH base AS (
    SELECT * FROM "admin_db"."gold"."int_order_items_joined"
    WHERE order_status NOT IN ('canceled', 'unavailable')
    AND product_category_name IS NOT NULL
),

category_metrics AS (
    SELECT
        product_category_name,
        year,
        quarter,

        COUNT(DISTINCT order_id)        AS total_orders,
        COUNT(order_item_key)           AS total_items_sold,
        ROUND(SUM(price), 2)            AS total_revenue,
        ROUND(AVG(price), 2)            AS avg_price,
        ROUND(AVG(review_score), 2)     AS avg_review_score,

        DENSE_RANK() OVER (
            PARTITION BY year, quarter
            ORDER BY SUM(price) DESC
        )                               AS rank_in_quarter

    FROM base
    GROUP BY
        product_category_name,
        year,
        quarter
)

SELECT * FROM category_metrics