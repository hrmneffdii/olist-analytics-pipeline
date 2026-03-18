
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Test FAIL kalau ada revenue negatif di mart
-- Test fail if a negative revenue exists
SELECT
    order_date,
    total_revenue
FROM "admin_db"."gold"."mart_sales_daily"
WHERE total_revenue < 0
  
  
      
    ) dbt_internal_test