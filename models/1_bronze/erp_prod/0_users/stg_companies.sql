With source as (
 select * from {{ source('erp', 'companies') }}
)
select 
*,

current_timestamp() as ingestion_timestamp




from source 