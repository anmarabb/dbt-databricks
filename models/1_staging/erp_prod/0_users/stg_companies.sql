With source as (
 select * from {{ source('erp_prod', 'companies') }}
)
select 
*,

current_timestamp() as ingestion_timestamp,




from source 