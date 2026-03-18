-- Test fail if there is exists a segment which is not defined

SELECT
    customer_segment,
    total_customers
FROM {{ ref('mart_customer_segmentation') }}
WHERE customer_segment NOT IN (
    'One-time Buyer',
    'Occasional Buyer',
    'Loyal Buyer'
)