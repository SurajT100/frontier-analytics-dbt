
  create view "FBS_DB"."dbt_dev_intermediate"."int_sap__orders_not_sharing__dbt_tmp"
    
    
  as (
    with orders as (

    select * from "FBS_DB"."dbt_dev_staging"."stg_sap__orders"

),

order_lines as (

    select * from "FBS_DB"."dbt_dev_staging"."stg_sap__order_lines"

),

profit_centres as (

    select * from "FBS_DB"."dbt_dev_staging"."stg_sap__profit_centres"

),

-- Join orders to get one row per order (not per line)
-- rn = 1 ensures we take only the first line per order
-- This matches your original view logic exactly
orders_with_lines as (

    select
        o.order_id,
        o.order_number,
        o.order_date,
        o.customer_name,
        o.margin,
        o.sbu_code,
        o.order_type,
        o.margin_type,
        o.sales_executive_code,
        o.block_code,
        o.customer_group_code,
        o.ld_cost,
        o.comments,
        lower(l.subcategory) as subcategory,
        'Not Sharing' as sharing_type,
        row_number() over (partition by l.order_id) as rn

    from orders o
    join order_lines l on l.order_id = o.order_id

    -- Your original filters
    where o.margin <> 0
      and o.is_cancelled = 'N'
      and o.margin_sharing_type = 'NS'
      and (lower(l.subcategory) not like '%is%' or l.subcategory is null)
      and (o.sbu_code is null
           or o.sbu_code not in ('POWER SERVICE', 'SPWR'))

),

-- Clean employee ID - strip E000 prefix (your original CASE logic)
employee_id_cleaned as (

    select
        order_id,
        order_number,
        order_date,
        customer_name,
        margin,
        sbu_code,
        order_type,
        margin_type,
        block_code,
        customer_group_code,
        ld_cost,
        comments,
        subcategory,
        sharing_type,
        rn,
        case
            when left(replace(sales_executive_code, 'E000', ''), 1) = '0'
                then right(replace(sales_executive_code, 'E000', ''), 3)
            when sales_executive_code = 'DIR00001'
                then '1'
            else replace(sales_executive_code, 'E000', '')
        end as employee_id,
        sales_executive_code as kam_profit_centre_code

    from orders_with_lines
    where rn = 1

)

select * from employee_id_cleaned
  );