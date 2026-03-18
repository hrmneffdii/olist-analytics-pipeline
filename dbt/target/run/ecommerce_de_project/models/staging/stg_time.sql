
  create view "admin_db"."gold"."stg_time__dbt_tmp"
    
    
  as (
    WITH source AS (
    SELECT * FROM "admin_db"."silver"."dim_time"
)

SELECT * FROM source
  );