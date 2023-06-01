With source as (
 select * from {{ source('1_source', 'packages') }}
)
select 

*,

current_timestamp() as ingestion_timestamp
 




from source as packages