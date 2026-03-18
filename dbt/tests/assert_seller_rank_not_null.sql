-- Test fail if seller doesn't have rank

SELECT
    seller_id,
    revenue_rank
FROM {{ ref('mart_seller_performance') }}
WHERE revenue_rank IS NULL