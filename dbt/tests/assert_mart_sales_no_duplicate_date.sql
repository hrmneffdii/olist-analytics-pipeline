-- Test fail if a date duplicate exists in mart sales daily

SELECT
    order_date,
    COUNT(*) AS jumlah
FROM {{ ref('mart_sales_daily') }}
GROUP BY order_date
HAVING COUNT(*) > 1