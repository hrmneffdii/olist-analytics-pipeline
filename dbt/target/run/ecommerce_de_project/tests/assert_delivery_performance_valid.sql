
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Test fail if late delivery out of range 0-100

SELECT
    customer_state,
    late_delivery_pct
FROM "admin_db"."gold"."mart_delivery_performance"
WHERE late_delivery_pct < 0
OR    late_delivery_pct > 100
  
  
      
    ) dbt_internal_test