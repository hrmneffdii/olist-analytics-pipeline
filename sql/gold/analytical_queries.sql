-- =====================================================
-- QUERY 1: Month-over-Month Revenue Growth
-- 
-- "How about trend of revenue Olist monthly? How much percentage
--  it's growth comparing past month?"
-- =====================================================

WITH monthly_revenue AS (
    SELECT
        dt.year,
        dt.month,
        dt.month_name,
        SUM(fo.price + fo.freight_value)    AS total_revenue,
        COUNT(DISTINCT fo.order_id)         AS total_orders
    FROM silver.fact_orders fo
    JOIN silver.dim_time dt
        ON fo.order_date_key = dt.date_key
    WHERE fo.order_status NOT IN ('canceled', 'unavailable')
    GROUP BY dt.year, dt.month, dt.month_name
),

growth AS (
    SELECT
        year,
        month,
        month_name,
        total_revenue,
        total_orders,
        LAG(total_revenue) OVER (
            ORDER BY year, month
        ) AS revenue_past_month,

        ROUND(
            (total_revenue - LAG(total_revenue) OVER (ORDER BY year, month))
            / NULLIF(LAG(total_revenue) OVER (ORDER BY year, month), 0) * 100
        , 2) AS pct_growth
    FROM monthly_revenue
)

SELECT
    year,
    month,
    month_name,
    ROUND(total_revenue, 2)     AS total_revenue,
    total_orders,
    ROUND(revenue_past_month, 2) AS revenue_past_month,
    pct_growth                  AS pct_growth_vs_last_month
FROM growth
ORDER BY year, month;


-- =====================================================
-- QUERY 2: Top 10 Seller by Revenue 
-- 
-- "Who top 10 seller with highest revenue all time? 
--  How performace their delivery?"
-- =====================================================

WITH seller_performance AS (
    SELECT
        ds.seller_id,
        ds.seller_city,
        ds.seller_state,
        COUNT(DISTINCT fo.order_id)             AS total_orders,
        SUM(fo.price)                           AS total_revenue,
        ROUND(AVG(fo.review_score), 2)          AS avg_review_score,
        ROUND(
            SUM(CASE WHEN fo.is_late_delivery THEN 1 ELSE 0 END)::NUMERIC
            / NULLIF(COUNT(*), 0) * 100
        , 2)                                    AS late_delivery_pct
    FROM silver.fact_orders fo
    JOIN silver.dim_sellers ds
        ON fo.seller_key = ds.seller_key
    WHERE fo.order_status NOT IN ('canceled', 'unavailable')
    GROUP BY ds.seller_id, ds.seller_city, ds.seller_state
)

SELECT
    RANK() OVER (ORDER BY total_revenue DESC)   AS revenue_rank,
    seller_id,
    seller_city,
    seller_state,
    total_orders,
    ROUND(total_revenue, 2)                     AS total_revenue,
    avg_review_score,
    late_delivery_pct                           AS late_delivery_pct
FROM seller_performance
ORDER BY revenue_rank
LIMIT 10;


-- =====================================================
-- QUERY 3: Top 5 Product Category per Quarter
-- 
-- "Which product category that best seller in every quarter?
--  is a shifting trend between quarter?"
-- =====================================================

WITH category_quarterly AS (
    SELECT
        dt.year,
        dt.quarter,
        dp.product_category_name            AS category,
        COUNT(DISTINCT fo.order_id)         AS total_orders,
        ROUND(SUM(fo.price), 2)             AS total_revenue,
        DENSE_RANK() OVER (
            PARTITION BY dt.year, dt.quarter
            ORDER BY SUM(fo.price) DESC
        )                                   AS rank_in_quarter
    FROM silver.fact_orders fo
    JOIN silver.dim_products dp
        ON fo.product_key = dp.product_key
    JOIN silver.dim_time dt
        ON fo.order_date_key = dt.date_key
    WHERE fo.order_status NOT IN ('canceled', 'unavailable')
    AND dp.product_category_name IS NOT NULL
    GROUP BY dt.year, dt.quarter, dp.product_category_name
)

SELECT
    year,
    quarter,
    rank_in_quarter,
    category,
    total_orders,
    total_revenue
FROM category_quarterly
WHERE rank_in_quarter <= 5
ORDER BY year, quarter, rank_in_quarter;


-- =====================================================
-- QUERY 4: Customer Segmentation by Purchase Behavior
-- 
-- "How about distribution customer based on frequency buying?
--  How about comparation one-time buyer vs repeat buyer?"
-- =====================================================

WITH customer_orders AS (
    SELECT
        dc.customer_unique_id,
        dc.customer_state,
        COUNT(DISTINCT fo.order_id)         AS total_orders,
        ROUND(SUM(fo.price), 2)             AS total_spent,
        ROUND(AVG(fo.price), 2)             AS avg_order_value,
        MIN(dt.full_date)                   AS first_order_date,
        MAX(dt.full_date)                   AS last_order_date
    FROM silver.fact_orders fo
    JOIN silver.dim_customers dc
        ON fo.customer_key = dc.customer_key
    JOIN silver.dim_time dt
        ON fo.order_date_key = dt.date_key
    WHERE fo.order_status NOT IN ('canceled', 'unavailable')
    GROUP BY dc.customer_unique_id, dc.customer_state
),

segmented AS (
    SELECT
        *,
        CASE
            WHEN total_orders = 1 THEN 'One-time Buyer'
            WHEN total_orders BETWEEN 2 AND 3 THEN 'Occasional Buyer'
            WHEN total_orders >= 4 THEN 'Loyal Buyer'
        END AS segment
    FROM customer_orders
)

SELECT
    segment,
    COUNT(*)                                AS total_customers,
    ROUND(
        COUNT(*)::NUMERIC
        / SUM(COUNT(*)) OVER () * 100
    , 2)                                    AS pct_of_customers,
    ROUND(SUM(total_spent), 2)              AS total_revenue,
    ROUND(
        SUM(total_spent)
        / SUM(SUM(total_spent)) OVER () * 100
    , 2)                                    AS pct_of_revenue,
    ROUND(AVG(total_spent), 2)              AS avg_spent_per_customer,
    ROUND(AVG(avg_order_value), 2)          AS avg_order_value
FROM segmented
GROUP BY segment
ORDER BY total_revenue DESC;


-- =====================================================
-- QUERY 5: Delivery Performance Heatmap by Stat
-- 
-- "Which country that has a late delivery?
--  How average daily delivery per state? 
--  How about correlation between late delivery vs review score?"
-- =====================================================

WITH state_delivery AS (
    SELECT
        dc.customer_state,
        COUNT(*)                                AS total_orders,
        ROUND(AVG(fo.delivery_days), 1)         AS avg_delivery_days,
        ROUND(
            SUM(CASE WHEN fo.is_late_delivery THEN 1 ELSE 0 END)::NUMERIC
            / NULLIF(COUNT(*), 0) * 100
        , 2)                                    AS late_delivery_pct,
        ROUND(AVG(fo.review_score), 2)          AS avg_review_score,
        ROUND(SUM(fo.price), 2)                 AS total_revenue
    FROM silver.fact_orders fo
    JOIN silver.dim_customers dc
        ON fo.customer_key = dc.customer_key
    WHERE fo.order_status = 'delivered'
    AND fo.delivery_days IS NOT NULL
    GROUP BY dc.customer_state
)

SELECT
    customer_state,
    total_orders,
    avg_delivery_days,
    late_delivery_pct,
    avg_review_score,
    total_revenue,

    CASE
        WHEN late_delivery_pct >= 20 THEN 'Poor'
        WHEN late_delivery_pct BETWEEN 10 AND 19 THEN 'Average'
        WHEN late_delivery_pct < 10 THEN 'Good'
    END AS delivery_performance
FROM state_delivery
ORDER BY late_delivery_pct DESC;