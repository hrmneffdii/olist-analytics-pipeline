
    
    

select
    order_item_key as unique_field,
    count(*) as n_records

from "admin_db"."gold"."int_order_items_joined"
where order_item_key is not null
group by order_item_key
having count(*) > 1


