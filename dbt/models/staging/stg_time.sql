WITH source AS (
    SELECT * FROM {{ source('silver', 'dim_time') }}
)

SELECT * FROM source