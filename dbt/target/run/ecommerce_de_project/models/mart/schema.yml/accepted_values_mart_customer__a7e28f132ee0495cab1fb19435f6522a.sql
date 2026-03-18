
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        customer_segment as value_field,
        count(*) as n_records

    from "admin_db"."gold"."mart_customer_segmentation"
    group by customer_segment

)

select *
from all_values
where value_field not in (
    'One-time Buyer','Occasional Buyer','Loyal Buyer'
)



  
  
      
    ) dbt_internal_test