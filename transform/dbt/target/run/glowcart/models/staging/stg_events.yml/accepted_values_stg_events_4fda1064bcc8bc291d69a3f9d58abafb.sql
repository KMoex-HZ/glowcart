
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        event_type as value_field,
        count(*) as n_records

    from "glowcart"."main"."stg_events"
    group by event_type

)

select *
from all_values
where value_field not in (
    'page_view','add_to_cart','checkout','payment_success','payment_failed'
)



  
  
      
    ) dbt_internal_test