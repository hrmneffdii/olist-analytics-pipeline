
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select late_delivery_pct
from "admin_db"."gold"."mart_seller_performance"
where late_delivery_pct is null



  
  
      
    ) dbt_internal_test