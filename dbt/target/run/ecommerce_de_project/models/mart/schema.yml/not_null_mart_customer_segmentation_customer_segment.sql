
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select customer_segment
from "admin_db"."gold"."mart_customer_segmentation"
where customer_segment is null



  
  
      
    ) dbt_internal_test