With source as (
 select * from {{ source('1_source', 'order_requests') }}
)
select 

*,

current_timestamp() as ingestion_timestamp,
 




from source as orr