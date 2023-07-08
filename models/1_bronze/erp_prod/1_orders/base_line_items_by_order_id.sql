With source as (
 select 
 li.order_id,
 max(li.order_number) as order_number,
 max(completed_at) as order_date, --Date Completed
 max(customer_id) as customer_id,
 max(customer.name) as Customer,
 max(li.order_type) as order_type,
 sum(total_price_without_tax) as Subtotal,
 sum(total_tax) as Tax,
 sum(total_price_include_tax) as Total,


 count(li.line_item_id) as items_count --number of items in a single order
 
 from {{ ref('stg_line_items') }} as li
 left join {{ref('base_users')}} as customer on customer.id = li.customer_id

 group by li.order_id
)
select 

*,

current_timestamp() as ingestion_timestamp
 
from source 
