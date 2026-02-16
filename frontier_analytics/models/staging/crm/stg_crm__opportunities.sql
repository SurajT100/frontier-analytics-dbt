with source as (

    select * from {{ source('crm', 'Opportunity') }}

),

renamed as (

    select
        "Oppo_OpportunityId"    as opportunity_id,
        "Oppo_PrimaryCompanyId" as company_id,
        "Oppo_AssignedUserId"   as assigned_user_id,
        "Oppo_CreatedBy"        as created_by_user_id,
        "Oppo_Opened"           as opened_at,
        "Oppo_TargetClose"      as target_close_date,
        "Oppo_UpdatedDate"      as updated_at,
        "Oppo_Stage"            as stage,
        "Oppo_Certainty"        as certainty,
        "Oppo_Forecast"         as forecast_topline,
        "Oppo_ChannelId"        as channel_id,
        "oppo_segment"          as segment,
        "oppo_qty"              as quantity,
        "oppo_oppotype"         as opportunity_type,
        "oppo_principle"        as oem_principle,
        "oppo_OEM1"             as oem_1,
        "oppo_OEM2"             as oem_2,
        "oppo_oem3"             as oem_3,
        "oppo_sbu1"             as sbu_1_id,
        "oppo_SBU2"             as sbu_2_id,
        "oppo_sbu3"             as sbu_3_id,

        "oppo_SA"               as solution_architect_code,
        "oppo_CRIMOFF"          as services_subcategory,
        "oppo_Mustwin"          as is_mustwin,
        "oppo_VPvisit"          as is_vp_visit,
        "oppo_VPVISITREMARK"    as vp_visit_remark,
        "oppo_bforecast"        as forecast_bottomline,
        "oppo_topline1"         as topline_1,
        "oppo_topline2"         as topline_2,
        "oppo_topline3"         as topline_3,
        "oppo_botline1"         as bottomline_1,
        "oppo_botline2"         as bottomline_2,
        "oppo_botline3"         as bottomline_3
    from source

)

select * from renamed
