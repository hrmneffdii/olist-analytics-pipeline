-- Test FAIL kalau ada revenue negatif di mart
-- Test fail if a negative revenue exists
SELECT
    order_date,
    total_revenue
FROM {{ ref('mart_sales_daily') }}
WHERE total_revenue < 0