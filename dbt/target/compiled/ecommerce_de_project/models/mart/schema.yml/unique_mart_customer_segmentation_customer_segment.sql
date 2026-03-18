
    
    

select
    customer_segment as unique_field,
    count(*) as n_records

from "admin_db"."gold"."mart_customer_segmentation"
where customer_segment is not null
group by customer_segment
having count(*) > 1


