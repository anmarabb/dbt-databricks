With source as (
 select * from {{ source('1_source', 'additional_items_reports') }}
)
select 

*,

current_timestamp() as ingestion_timestamp,
 




from source as ad