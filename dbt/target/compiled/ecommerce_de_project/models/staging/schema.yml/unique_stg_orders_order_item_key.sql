
    
    

select
    order_item_key as unique_field,
    count(*) as n_records

from "admin_db"."gold"."stg_orders"
where order_item_key is not null
group by order_item_key
having count(*) > 1


