with source_data as(
    SELECT *
    FROM {{ source('data_raw', 'customers_raw') }}
)