
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Test fail if a date duplicate exists in mart sales daily

SELECT
    order_date,
    COUNT(*) AS jumlah
FROM "admin_db"."gold"."mart_sales_daily"
GROUP BY order_date
HAVING COUNT(*) > 1
  
  
      
    ) dbt_internal_test