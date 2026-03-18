
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select total_customers
from "admin_db"."gold"."mart_customer_segmentation"
where total_customers is null



  
  
      
    ) dbt_internal_test