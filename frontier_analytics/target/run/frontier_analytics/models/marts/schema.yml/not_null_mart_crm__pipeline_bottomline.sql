
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select bottomline
from "FBS_DB"."dbt_dev_marts"."mart_crm__pipeline"
where bottomline is null



  
  
      
    ) dbt_internal_test