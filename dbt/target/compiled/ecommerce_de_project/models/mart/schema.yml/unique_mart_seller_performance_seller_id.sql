
    
    

select
    seller_id as unique_field,
    count(*) as n_records

from "admin_db"."gold"."mart_seller_performance"
where seller_id is not null
group by seller_id
having count(*) > 1


