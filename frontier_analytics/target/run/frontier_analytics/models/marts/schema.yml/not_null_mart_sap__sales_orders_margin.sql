
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select margin
from "FBS_DB"."dbt_dev_marts"."mart_sap__sales_orders"
where margin is null



  
  
      
    ) dbt_internal_test