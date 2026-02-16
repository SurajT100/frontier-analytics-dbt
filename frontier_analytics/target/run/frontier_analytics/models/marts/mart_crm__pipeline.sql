
  
    

  create  table "FBS_DB"."dbt_dev_marts"."mart_crm__pipeline__dbt_tmp"
  
  
    as
  
  (
    with opportunities as (

    select * from "FBS_DB"."dbt_dev_intermediate"."int_crm__opportunities_expanded"

),

account_type as (

    select * from "FBS_DB"."Excel_Data"."CRM_Account_Type_Mapping"

),

sbu_target as (

    select * from "FBS_DB"."Excel_Data"."SBU_Target"

),

final as (

    select distinct
        o.opportunity_id,
        o.company_id,
        o.open_date,
        o.open_month,
        o.close_date,
        o.close_month,
        o.assigned_user_name        as opportunity_assigned_user,
        o.assigned_employee_id      as empl_id,
        o.kam_name,
        o.kam_id,
        o.block_name                as block,
        o.stage,
        o.certainty,
        o.certainty_bucket,
        o.segment                   as category,
        o.company_name,
        o.vertical,
        o.sbu,
        o.oem,
        o.quantity,
        o.topline,
        o.bottomline,
        o.opportunity_age_days      as opportunity_age,
        o.opportunity_type          as tender,
        o.services_subcategory,
        o.solution_architect,
        o.is_mustwin,
        o.is_vp_visit,
        o.vp_visit_remark           as remark,
        o.created_by_name           as opportunity_created_by,
        o.updated_at                as last_modify_date,
        coalesce(at."Final Ac Type", 'OTHER') as final_ac_type,
        st."Region"                 as region,
        st."Block Head"             as block_head

    from opportunities o

    left join account_type at
        on o.segment::text = at."CRM"::text

    left join sbu_target st
        on o.block_name = st."Block"::text

)

select * from final
  );
  