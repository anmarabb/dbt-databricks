With source as (

    
 select 
 
sh.* EXCEPT(ingestion_timestamp,master_shipment_id,departure_date),


msh.master_shipments_status,
msh.master_shipment_name as master_shipment,
msh.master_shipments_fulfillment_status,
msh.arrival_at,
msh.master_shipment_id,
msh.master_total_quantity,

msh.departure_date,
msh.arrival_date,



concat( "https://erp.floranow.com/master_shipments/", msh.master_shipment_id) as master_shipment_link,

shipments_suppliers.supplier_name as Supplier,
shipments_suppliers.supplier_region as Origin,
shipments_suppliers.account_manager,


w.warehouse_name as warehouse,
w.warehouse_country as Destination,

--w2.warehouse_name as warehouse2,


case when msh.arrival_at is not null then 1 else 0 end as shipments_received,
case when msh.arrival_at is null  then 'shipmnet_not_arrived' else 'shipmnet_arrived' end as shipmnet_arrival,



CASE
    WHEN months_between(to_date(msh.arrival_date), current_date()) > 1 THEN 'Wrong date'
    WHEN to_date(msh.arrival_date) = current_date() + INTERVAL 1 DAY THEN 'Tomorrow'
    WHEN to_date(msh.arrival_date) > current_date() THEN 'Future'
    WHEN to_date(msh.arrival_date) = current_date() - INTERVAL 1 DAY THEN 'Yesterday'
    WHEN to_date(msh.arrival_date) = current_date() THEN 'Today'
    WHEN months_between(current_date(), to_date(msh.arrival_date)) = 0 THEN 'Month To Date'
    WHEN months_between(current_date(), to_date(msh.arrival_date)) = 1 THEN 'Last Month'
    WHEN year(current_date()) = year(to_date(msh.arrival_date)) THEN 'Year To Date'
    ELSE 'Past'
  END AS select_arrival_date



from {{ ref('stg_shipments') }} as sh
left join  {{ ref('stg_master_shipments') }} as msh on sh.master_shipment_id = msh.master_shipment_id
left join  {{ ref('base_warehouses') }} as w on msh.warehouse_id = w.warehouse_id
--left join  {{ ref('base_warehouses') }} as w2 on sh.warehouse_id = w.warehouse_id

left join  {{ ref('base_suppliers') }} as shipments_suppliers on shipments_suppliers.supplier_id = sh.supplier_id
 
 
 
)
select 

*,

current_timestamp() as ingestion_timestamp
 




from source as sh

