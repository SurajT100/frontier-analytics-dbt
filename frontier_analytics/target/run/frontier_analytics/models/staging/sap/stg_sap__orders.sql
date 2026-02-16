
  create view "FBS_DB"."dbt_dev_staging"."stg_sap__orders__dbt_tmp"
    
    
  as (
    with source as (

    select * from "FBS_DB"."SAP"."ORDR"

),

renamed as (

    select
        "DocEntry"          as order_id,
        "DocNum"            as order_number,
        "DocDate"           as order_date,
        "CardName"          as customer_name,
        "CANCELED"          as is_cancelled,
        "U_Marginpl"::numeric as margin,
        "U_SBU"             as sbu_code,
        "U_Ordertype"       as order_type,
        "U_AVA_MARGINTYPE"  as margin_type,
        "U_SalesExecutive"  as sales_executive_code,
        "U_AVA_Block"       as block_code,
        "U_Cgroup"          as customer_group_code,
        "U_Ldcst"           as ld_cost,
        "Comments"          as comments,
        "U_AVA_MARGINTYPE"  as margin_sharing_type

    from source

)

select * from renamed
  );