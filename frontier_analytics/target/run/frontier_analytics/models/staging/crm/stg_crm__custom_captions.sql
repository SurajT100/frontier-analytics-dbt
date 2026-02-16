
  create view "FBS_DB"."dbt_dev_staging"."stg_crm__custom_captions__dbt_tmp"
    
    
  as (
    with source as (

    select * from "FBS_DB"."CRM"."Custom_Captions"

),

renamed as (

    select
        "Capt_Code"     as caption_code,
        "Capt_Family"   as caption_family,
        "Capt_US"       as caption_value
    from source

)

select * from renamed
  );