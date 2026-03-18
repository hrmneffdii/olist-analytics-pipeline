
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Test fail if there is exists a segment which is not defined

SELECT
    customer_segment,
    total_customers
FROM "admin_db"."gold"."mart_customer_segmentation"
WHERE customer_segment NOT IN (
    'One-time Buyer',
    'Occasional Buyer',
    'Loyal Buyer'
)
  
  
      
    ) dbt_internal_test