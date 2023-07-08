


With source as (
 select 
* 
 from {{ source(var('erp_source'), 'orders') }} as o
)
select 

*,

current_timestamp() as ingestion_timestamp
 

from source
