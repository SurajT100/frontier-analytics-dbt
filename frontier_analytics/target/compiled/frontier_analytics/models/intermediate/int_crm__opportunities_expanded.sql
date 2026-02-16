with opportunities as (

    select * from "FBS_DB"."dbt_dev_staging"."stg_crm__opportunities"

),

users as (

    select * from "FBS_DB"."dbt_dev_staging"."stg_crm__users"

),

companies as (

    select * from "FBS_DB"."dbt_dev_staging"."stg_crm__companies"

),

captions as (

    select * from "FBS_DB"."dbt_dev_staging"."stg_crm__custom_captions"

),

-- Build arrays for SBU, OEM, topline, bottomline
-- This mirrors your original view's array construction
base as (

    select
        o.opportunity_id,
        o.company_id,
        o.assigned_user_id,
        o.created_by_user_id,
        o.opened_at,
        o.target_close_date,
        o.updated_at,
        o.stage,
        o.certainty,
        o.segment,
        o.quantity,
        o.opportunity_type,
        o.services_subcategory,
        o.is_mustwin,
        o.is_vp_visit,
        o.vp_visit_remark,

        -- Assigned user details
        assigned_user.full_name      as assigned_user_name,
        assigned_user.employee_id    as assigned_employee_id,

        -- Created by user
        created_user.full_name       as created_by_name,

        -- Company details
        c.company_name,
        c.sector_code,
        c.block_code                 as company_block_code,

        -- Block name from captions
        block_cap.caption_value      as block_name,

        -- Vertical from captions
        vertical_cap.caption_value   as vertical,

        -- Solution architect from captions
        sa_cap.caption_value         as solution_architect,

        -- KAM from primary user of company
        kam_user.full_name           as kam_name,
        kam_user.employee_id         as kam_id,

        -- Certainty bucket
        case
            when o.certainty <= 90 then '<90'
            when o.certainty > 90  then '>90'
            else o.certainty::text
        end as certainty_bucket,

        -- Opportunity age in days
        current_date - o.opened_at::date as opportunity_age_days,

        -- Arrays for multi-SBU and multi-OEM expansion
        array[
            sbu1.caption_value,
            sbu2.caption_value,
            sbu3.caption_value,
            sbu4.caption_value
        ] as sbu_array,

        array[
            oem0.caption_value,
            oem1.caption_value,
            oem2.caption_value,
            oem3.caption_value
        ] as oem_array,

        array[
            round(o.forecast_topline, 2),
            round(o.topline_1, 2),
            round(o.topline_2, 2),
            round(o.topline_3, 2)
        ] as topline_array,

        array[
            round(o.forecast_bottomline, 2),
            round(o.bottomline_1, 2),
            round(o.bottomline_2, 2),
            round(o.bottomline_3, 2)
        ] as bottomline_array

    from opportunities o

    -- Assigned user
    left join users assigned_user
        on o.assigned_user_id = assigned_user.user_id

    -- Created by user
    left join users created_user
        on o.created_by_user_id = created_user.user_id

    -- KAM from company primary user
    left join companies c
        on o.company_id = c.company_id
    left join users kam_user
        on c.primary_user_id = kam_user.user_id

    -- Block name
    left join captions block_cap
        on c.block_code::text = block_cap.caption_code::text
        and block_cap.caption_family = 'oppo_principle'

    -- Vertical
    left join captions vertical_cap
        on c.sector_code::text = vertical_cap.caption_code::text

    -- Solution architect
    left join captions sa_cap
        on o.solution_architect_code::text = sa_cap.caption_code::text

    -- SBU captions
    left join captions sbu1
        on o.sbu_1_id = sbu1.caption_code::integer
        and sbu1.caption_family = 'Channels'
    left join captions sbu2
        on o.sbu_2_id = sbu2.caption_code::integer
        and sbu2.caption_family = 'Channels'
    left join captions sbu3
        on o.sbu_3_id = sbu3.caption_code::integer
        and sbu3.caption_family = 'Channels'
    left join captions sbu4
        on o.assigned_user_id = sbu4.caption_code::integer
        and sbu4.caption_family = 'Channels'

    -- OEM captions
    left join captions oem0
        on o.oem_principle::text = oem0.caption_code::text
        and oem0.caption_family = 'oppo_principle'
    left join captions oem1
        on o.oem_1::text = oem1.caption_code::text
        and oem1.caption_family = 'oppo_principle'
    left join captions oem2
        on o.oem_2::text = oem2.caption_code::text
        and oem2.caption_family = 'oppo_principle'
    left join captions oem3
        on o.oem_3::text = oem3.caption_code::text
        and oem3.caption_family = 'oppo_principle'

    -- Financial year filter - dynamic current FY
    where o.target_close_date > case
        when extract(month from current_date) >= 4
            then (extract(year from current_date)::text || '-04-01')::date
        else ((extract(year from current_date) - 1)::text || '-04-01')::date
    end

),

-- Unnest arrays - one row per SBU/OEM combination
-- This is the CROSS JOIN LATERAL unnest logic from your original view
expanded as (

    select
        b.opportunity_id,
        b.company_id,
        b.assigned_user_id,
        b.assigned_user_name,
        b.assigned_employee_id,
        b.created_by_name,
        b.opened_at::date               as open_date,
        to_char(b.opened_at, 'FMMonth') as open_month,
        b.target_close_date::date       as close_date,
        to_char(b.target_close_date, 'FMMonth') as close_month,
        b.updated_at,
        b.stage,
        b.certainty,
        b.certainty_bucket,
        b.segment,
        b.quantity,
        b.opportunity_type,
        b.services_subcategory,
        b.is_mustwin,
        b.is_vp_visit,
        b.vp_visit_remark,
        b.company_name,
        b.vertical,
        b.block_name,
        b.solution_architect,
        b.kam_name,
        b.kam_id,
        b.opportunity_age_days,
        b.sbu_array[u.idx + 1]          as sbu,
        b.oem_array[u.idx + 1]          as oem,
        b.topline_array[u.idx + 1]      as topline,
        b.bottomline_array[u.idx + 1]   as bottomline

    from base b
    cross join lateral (
        select unnest(array[0,1,2,3]) as idx
    ) u

    -- Only keep rows where bottomline is non-zero
    where b.bottomline_array[u.idx + 1] <> 0

)

select distinct * from expanded