with orders as (

    select * from "FBS_DB"."dbt_dev_intermediate"."int_sap__orders_combined"

),

sbu_mapping as (

    select * from "FBS_DB"."Excel_Data"."SBU_Name_Master"

),

account_type as (

    select * from "FBS_DB"."Excel_Data"."SAP_Account_Type_Mapping"

),

sbu_target as (

    select * from "FBS_DB"."Excel_Data"."SBU_Target"

),

final as (

    select distinct
        o.order_number              as so_number,
        o.comments                  as orn,
        o.order_id,
        o.order_date,
        o.order_month,
        o.margin,
        coalesce(sbu."SBU Name", o.sbu_code) as sbu,
        o.kam_name,
        o.block_name                as block,
        o.order_type,
        o.subcategory,
        o.customer_name,
        o.sharing_type,
        o.financial_quarter,
        o.month_name,
        o.ld_cost,
        o.employee_id,
        at."Final Ac Type"          as account_type,
        st."Region"                 as region

    from orders o

    left join sbu_mapping sbu
        on o.sbu_code = sbu."PrcName"

    left join account_type at
        on o.customer_group_code = at."SAP"

    left join sbu_target st
        on o.block_name = st."Block"

    where o.order_date >= case
        when extract(month from current_date) >= 4
            then (extract(year from current_date)::text || '-04-01')::date
        else ((extract(year from current_date) - 1)::text || '-04-01')::date
    end

    order by o.order_number

)

select * from final