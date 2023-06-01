With source as (
 select * from {{ source('1_source', 'companies') }}
)
select 
*,

current_timestamp() as ingestion_timestamp




from source 