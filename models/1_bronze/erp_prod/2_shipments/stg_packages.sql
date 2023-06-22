With source as (
 select * from {{ source(var('erp_source'), 'packages') }}
)
select 

*,

current_timestamp() as ingestion_timestamp
 




from source as packages