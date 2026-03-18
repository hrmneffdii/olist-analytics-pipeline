
    
    

select
    customer_key as unique_field,
    count(*) as n_records

from "admin_db"."gold"."int_customer_orders"
where customer_key is not null
group by customer_key
having count(*) > 1


