WITH source AS (
    SELECT * FROM {{ source('silver', 'dim_sellers') }}
)

SELECT * FROM source