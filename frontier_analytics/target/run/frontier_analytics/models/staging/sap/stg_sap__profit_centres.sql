
  create view "FBS_DB"."dbt_dev_staging"."stg_sap__profit_centres__dbt_tmp"
    
    
  as (
    with source as (

    select * from "FBS_DB"."SAP"."OPRC"

),

renamed as (

    select
        "PrcCode"   as profit_centre_code,
        "PrcName"   as profit_centre_name
    from source

)

select * from renamed
  );