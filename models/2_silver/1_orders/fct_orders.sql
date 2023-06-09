with

source as ( 

select

li.order_number,
max(record_type) as record_type,
max(record_type_details) as record_type_details,
max(li.Customer) as Customer,
max(li.order_id) as order_id,
sum(li.total_price_without_tax) as Gross_item_sales_amount, 
count(line_item_id) as items_count,


--Gross_item_sales_amount: refers to the total amount of revenue generated by the sale of an item, before any deductions or adjustments are made.
--Discounted_item_sales_amount: refers to the total revenue generated by the sale of an item at a discounted price.
--Net_item_sales_amount: refers to the total revenue generated by the sale of an item after discounts, taxes, and other adjustments have been applied.

--Base_price: typically refers to the original or standard price of the item before any discounts or promotions are applied.
--Item_discount_amount: refers to the amount of discount applied to an item, which is the difference between the base price and the discounted price.
--Item_tax_amount: refers to the amount of tax applied to an item.


--Total_price_without_tax: refers to the total amount of money a customer pays for a product or service, excluding any taxes that may apply.


--Unit_fob_price: refers to the cost of the product per unit at the factory, which includes the cost of manufacturing, packing, and transporting the goods to the port of departure (FOB stands for "free on board").
--Unit_landed_cost: refers to the cost of the product per unit after it has arrived at its destination, and includes additional costs such as transportation, customs fees, taxes, and insurance.
--Unit_price: is the price at which the product is sold to the customer, which can be based on a variety of factors including the cost of production, marketing and distribution expenses, competition, and desired profit margin.




current_timestamp() as insertion_timestamp 


from {{ref('int_line_items')}} as li 
group by li.order_number
)

select * from source