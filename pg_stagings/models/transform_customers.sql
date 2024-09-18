with source_data as(
    SELECT *
    FROM {{ source('data_raw', 'customers_raw') }}
),

renamed_columns as (
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
FROM renamed_columns