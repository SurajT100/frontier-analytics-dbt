with not_sharing as (

    select * from {{ ref('int_sap__orders_not_sharing') }}

),

sharing as (

    select * from {{ ref('int_sap__orders_sharing') }}

),

profit_centres as (

    select * from {{ ref('stg_sap__profit_centres') }}

),

-- Union both streams together with matching columns
combined as (

    select
        order_id,
        order_number,
        order_date,
        customer_name,
        margin,
        sbu_code,
        order_type,
        block_code,
        customer_group_code,
        ld_cost,
        comments,
        subcategory,
        sharing_type,
        employee_id,
        kam_profit_centre_code

    from not_sharing

    union all

    select
        order_id,
        order_number,
        order_date,
        customer_name,
        margin,
        sbu_code,
        order_type,
        block_code,
        customer_group_code,
        ld_cost,
        comments,
        subcategory,
        sharing_type,
        employee_id,
        kam_profit_centre_code

    from sharing

),

-- Join profit centres twice - once for block name, once for KAM name
-- This mirrors your original view joining OPRC twice with different aliases
enriched as (

    select
        c.order_id,
        c.order_number,
        c.order_date,
        c.customer_name,
        c.margin,
        c.sbu_code,
        c.order_type,
        c.ld_cost,
        c.comments,
        c.subcategory,
        c.sharing_type,
        c.employee_id,
        c.customer_group_code,
        block_pc.profit_centre_name   as block_name,
        kam_pc.profit_centre_name     as kam_name,

        -- Indian financial year quarter mapping
        case
            when extract(month from c.order_date) in (4,5,6)   then 'Q1'
            when extract(month from c.order_date) in (7,8,9)   then 'Q2'
            when extract(month from c.order_date) in (10,11,12) then 'Q3'
            when extract(month from c.order_date) in (1,2,3)   then 'Q4'
        end as financial_quarter,

        -- Month name mapping
        case extract(month from c.order_date)
            when 1  then 'January'
            when 2  then 'February'
            when 3  then 'March'
            when 4  then 'April'
            when 5  then 'May'
            when 6  then 'June'
            when 7  then 'July'
            when 8  then 'August'
            when 9  then 'September'
            when 10 then 'October'
            when 11 then 'November'
            when 12 then 'December'
        end as month_name,

        date_trunc('month', c.order_date) as order_month

    from combined c
    left join profit_centres block_pc
        on c.block_code = block_pc.profit_centre_code
    left join profit_centres kam_pc
        on c.kam_profit_centre_code = kam_pc.profit_centre_code

)

select * from enriched
