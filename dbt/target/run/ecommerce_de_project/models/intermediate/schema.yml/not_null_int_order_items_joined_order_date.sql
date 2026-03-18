
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select order_date
from "admin_db"."gold"."int_order_items_joined"
where order_date is null



  
  
      
    ) dbt_internal_test