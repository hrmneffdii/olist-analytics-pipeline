-- Test fail if a date duplicate exists in mart sales daily

SELECT
    order_date,
    COUNT(*) AS jumlah
FROM "admin_db"."gold"."mart_sales_daily"
GROUP BY order_date
HAVING COUNT(*) > 1