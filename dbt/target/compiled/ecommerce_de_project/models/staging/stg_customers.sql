WITH source AS (
    SELECT * FROM "admin_db"."silver"."dim_customers"
)

SELECT 
    customer_key,
    customer_id,	
    customer_unique_id,	
    customer_zip_code_prefix,	
    customer_city,	
    customer_state,	
    ingested_at
from source