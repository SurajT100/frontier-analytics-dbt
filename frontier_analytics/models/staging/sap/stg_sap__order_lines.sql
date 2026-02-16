with source as (

    select * from {{ source('sap', 'RDR1') }}

),

renamed as (

    select
        "DocEntry"      as order_id,
        "LineNum"       as line_number,
        "ItemCode"      as item_code,
        "Dscription"    as item_description,
        "U_SSC"         as subcategory,
        "LineTotal"     as line_total,
        "Quantity"      as quantity
    from source

)

select * from renamed
