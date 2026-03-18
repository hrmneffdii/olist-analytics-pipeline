WITH source AS (
    SELECT * FROM {{ source('silver', 'fact_orders') }}
)

SELECT
    order_item_key,
    order_id,
    order_item_id,
    customer_key,
    product_key,
    seller_key,
    order_date_key,
    order_status,
    COALESCE(price, 0)          AS price,
    COALESCE(freight_value, 0)  AS freight_value,
    COALESCE(payment_value, 0)  AS payment_value,
    payment_type,
    review_score,
    is_late_delivery,
    delivery_days,
    ingested_at
FROM source

