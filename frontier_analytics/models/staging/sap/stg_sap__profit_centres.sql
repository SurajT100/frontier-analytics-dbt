with source as (

    select * from {{ source('sap', 'OPRC') }}

),

renamed as (

    select
        "PrcCode"   as profit_centre_code,
        "PrcName"   as profit_centre_name
    from source

)

select * from renamed
