with margin_sharing as (

    select * from {{ ref('stg_sap__margin_sharing') }}

),

orders as (

    select * from {{ ref('stg_sap__orders') }}

),

order_lines as (

    select * from {{ ref('stg_sap__order_lines') }}

),

-- Replicate your original @F_MARGINSHARING logic
-- Final margin = margin_final if available, else clean margin_raw
margin_calculated as (

    select
        code,
        order_id,
        employee_id_code,
        sbu_code,
        subcategory,
        block_code,
        source_reference,
        margin_raw,
        margin_final,
        case
            when margin_final is null
                then regexp_replace(margin_raw::text, '[^0-9-]', '', 'g')::numeric
            else margin_final::numeric
        end as margin

    from margin_sharing

),

-- Join to ORDR to get order header details
joined as (

    select
        o.order_number,
        o.comments,
        mc.order_id,
        o.order_date,
        mc.margin,
        mc.sbu_code,
        o.order_type,
        o.margin_sharing_type,
        mc.employee_id_code,
        mc.block_code,
        o.customer_group_code,
        o.customer_name,
        o.ld_cost,
        lower(mc.subcategory) as subcategory,
        'Sharing' as sharing_type,
        row_number() over (partition by mc.order_id) as rn

    from margin_calculated mc
    left join orders o on o.order_id = mc.order_id
    left join order_lines l on l.order_id = mc.order_id

    where (
            coalesce(regexp_replace(mc.margin_raw::text, '[^0-9.-]', '', 'g')::numeric, 0) +
            coalesce(mc.margin_final::numeric, 0)
          ) <> 0
      and o.is_cancelled = 'N'
      and o.margin_sharing_type = 'S'
      and (lower(mc.subcategory) not like '%is%' or mc.subcategory is null)
      and (mc.sbu_code is null
           or mc.sbu_code not in ('POWER SERVICE', 'SPWR'))

),

-- Clean employee ID - same logic as not sharing
employee_id_cleaned as (

    select
        order_id,
        order_number,
        order_date,
        customer_name,
        margin,
        sbu_code,
        order_type,
        margin_sharing_type,
        block_code,
        customer_group_code,
        ld_cost,
        comments,
        subcategory,
        sharing_type,
        rn,
        case
            when left(replace(employee_id_code, 'E000', ''), 1) = '0'
                then right(replace(employee_id_code, 'E000', ''), 3)
            else replace(employee_id_code, 'E000', '')
        end as employee_id,
        employee_id_code as kam_profit_centre_code

    from joined
    where rn = 1

)

select * from employee_id_cleaned
