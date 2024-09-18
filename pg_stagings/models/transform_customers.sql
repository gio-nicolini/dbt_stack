with source_data as(
    SELECT *
    FROM {{ source('data_raw', 'customers_raw') }}
)

with replace_name as (

    SELECT customer_id,
            name as full_name,
            email,
            address,
            city,
            country,
            phone_number,
            birthdate
    FROM source_data
)

SELECT *
FROM replace_name