
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select so_number
from "FBS_DB"."dbt_dev_marts"."mart_sap__sales_orders"
where so_number is null



  
  
      
    ) dbt_internal_test