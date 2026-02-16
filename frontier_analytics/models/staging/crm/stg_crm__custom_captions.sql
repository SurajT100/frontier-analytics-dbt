with source as (

    select * from {{ source('crm', 'Custom_Captions') }}

),

renamed as (

    select
        "Capt_Code"     as caption_code,
        "Capt_Family"   as caption_family,
        "Capt_US"       as caption_value
    from source

)

select * from renamed
