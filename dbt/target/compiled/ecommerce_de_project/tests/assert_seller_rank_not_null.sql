-- Test fail if seller doesn't have rank

SELECT
    seller_id,
    revenue_rank
FROM "admin_db"."gold"."mart_seller_performance"
WHERE revenue_rank IS NULL