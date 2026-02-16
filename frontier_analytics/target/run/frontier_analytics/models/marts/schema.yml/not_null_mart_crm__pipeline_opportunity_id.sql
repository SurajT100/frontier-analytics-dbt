
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select opportunity_id
from "FBS_DB"."dbt_dev_marts"."mart_crm__pipeline"
where opportunity_id is null



  
  
      
    ) dbt_internal_test