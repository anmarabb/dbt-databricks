with

source as ( 

 
select 

line_item_id,
line_item_type,
order_type,
Customer,
User,
returned_by, 

Supplier,
supplier_region as Origin,

product_name as Product,
product_crop as Crop,
product_category,
product_subcategory,

li.order_number,


li.quantity,
li.unit_price,
li.currency,
li.total_price_without_tax, -- (li.quantity * li.unit_price)



li.incidents_count,



current_timestamp() as insertion_timestamp, 


from {{ref('int_line_items')}} as li 
)

select * from source