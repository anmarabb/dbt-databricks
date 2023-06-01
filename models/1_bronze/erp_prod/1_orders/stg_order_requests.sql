With source as (
 select * from {{ source('erp', 'order_requests') }}
)
select 

*,

current_timestamp() as ingestion_timestamp
 




from source as orr