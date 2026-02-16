
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        sharing_type as value_field,
        count(*) as n_records

    from "FBS_DB"."dbt_dev_marts"."mart_sap__sales_orders"
    group by sharing_type

)

select *
from all_values
where value_field not in (
    'Sharing','Not Sharing'
)



  
  
      
    ) dbt_internal_test