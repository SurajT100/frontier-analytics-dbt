
    
    

with all_values as (

    select
        financial_quarter as value_field,
        count(*) as n_records

    from "FBS_DB"."dbt_dev_marts"."mart_sap__sales_orders"
    group by financial_quarter

)

select *
from all_values
where value_field not in (
    'Q1','Q2','Q3','Q4'
)


