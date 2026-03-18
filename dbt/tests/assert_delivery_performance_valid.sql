-- Test fail if late delivery out of range 0-100

SELECT
    customer_state,
    late_delivery_pct
FROM {{ ref('mart_delivery_performance') }}
WHERE late_delivery_pct < 0
OR    late_delivery_pct > 100