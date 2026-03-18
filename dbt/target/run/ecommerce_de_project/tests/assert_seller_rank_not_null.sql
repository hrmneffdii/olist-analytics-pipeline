
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Test fail if seller doesn't have rank

SELECT
    seller_id,
    revenue_rank
FROM "admin_db"."gold"."mart_seller_performance"
WHERE revenue_rank IS NULL
  
  
      
    ) dbt_internal_test