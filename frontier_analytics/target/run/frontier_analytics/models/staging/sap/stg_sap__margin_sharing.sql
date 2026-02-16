
  create view "FBS_DB"."dbt_dev_staging"."stg_sap__margin_sharing__dbt_tmp"
    
    
  as (
    with source as (

    select * from "FBS_DB"."SAP"."@F_MARGINSHARING"

),

renamed as (

    select
        "Code"                  as code,
        "U_AVA_DocEntry"::integer as order_id,
        "U_AVA_DocNum"          as order_number,
        "U_AVA_EMPID"           as employee_id_code,
        "U_AVA_SUB"             as sbu_code,
        "U_AVA_SUBCATEGORY"     as subcategory,
        "U_AVA_BLOCK"           as block_code,
        "U_AVA_MARGIN"          as margin_raw,
        "U_AVA_FMargin"         as margin_final,
        "U_S_Orn"               as source_reference
    from source

)

select * from renamed
  );