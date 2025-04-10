select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    sessionId as unique_field,
    count(*) as n_records

from USER_DB_DOG.analytics.session_summary
where sessionId is not null
group by sessionId
having count(*) > 1



      
    ) dbt_internal_test