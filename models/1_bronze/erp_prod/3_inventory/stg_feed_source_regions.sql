With source as (
 select * from {{ source('1_source', 'feed_source_regions') }}
)
select 
*,

current_timestamp() as ingestion_timestamp
 



from source as fsr

