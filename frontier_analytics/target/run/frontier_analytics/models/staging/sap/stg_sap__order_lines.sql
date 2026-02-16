
  create view "FBS_DB"."dbt_dev_staging"."stg_sap__order_lines__dbt_tmp"
    
    
  as (
    with source as (

    select * from "FBS_DB"."SAP"."RDR1"

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
  );