With source as (
 select * from {{ source('erp_prod', 'stocks') }}
)
select 

*,

current_timestamp() as ingestion_timestamp,
 




from source as stk