With source as (
 select * from {{ source('erp', 'additional_items_reports') }}
)
select 

*,

current_timestamp() as ingestion_timestamp
 




from source as ad