With source as (
 select * from {{ source('erp', 'packing_box_items') }}
)
select 

*,

current_timestamp() as ingestion_timestamp
 




from source as packbox