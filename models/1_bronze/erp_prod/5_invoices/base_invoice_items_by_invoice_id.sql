With source as (
 select 
 ii.invoice_header_id,
 max(ii.number) as number,
 max(i.number) as invoice_number,
 max(i.purchase_order_number) as purchase_order_number,
 max(li.order_number) as order_number,
 max(ii.source_type) as source_type,
 max(i.generation_type) as generation_type,
 max(ii.order_date) as order_date,
 max(ii.delivery_date) as delivery_date,
 max(i.invoice_header_printed_at) as invoice_header_printed_at,

 count(*) as items_count --number of items in a single order


 from {{ ref('stg_invoice_items') }} as ii
 left join {{ ref('stg_invoices') }} as i on ii.invoice_header_id = i.invoice_header_id
left join {{ ref('stg_line_items') }} as li on ii.line_item_id = li.line_item_id

 group by ii.invoice_header_id
)
select 

*,

current_timestamp() as ingestion_timestamp
 
from source 
