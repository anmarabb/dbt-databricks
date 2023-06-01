With source as (
 select * from {{ source('erp', 'packing_lists') }}
)
select 
*,

current_timestamp() as ingestion_timestamp
 




from source as packlist