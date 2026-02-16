
  create view "FBS_DB"."dbt_dev_staging"."stg_crm__companies__dbt_tmp"
    
    
  as (
    with source as (

    select * from "FBS_DB"."CRM"."Company"

),

renamed as (

    select
        "Comp_CompanyId"    as company_id,
        "Comp_Name"         as company_name,
        "Comp_Sector"       as sector_code,
        "Comp_PrimaryUserId" as primary_user_id,
        "comp_blocks"       as block_code
    from source

)

select * from renamed
  );