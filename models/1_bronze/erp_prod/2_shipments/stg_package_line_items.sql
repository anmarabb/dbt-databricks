With source as (
 select * from {{ source(var('erp_source'), 'package_line_items') }}
)
select 

*,

current_timestamp() as ingestion_timestamp
 




from source as packages_li