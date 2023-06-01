With source as (
 select * from {{ source('1_source', 'packing_lists') }}
)
select 
*,

current_timestamp() as ingestion_timestamp
 




from source as packlist