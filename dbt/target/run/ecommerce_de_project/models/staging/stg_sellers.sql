
  create view "admin_db"."gold"."stg_sellers__dbt_tmp"
    
    
  as (
    WITH source AS (
    SELECT * FROM "admin_db"."silver"."dim_sellers"
)

SELECT * FROM source
  );