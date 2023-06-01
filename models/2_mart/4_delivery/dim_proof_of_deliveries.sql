with

source as ( 
        
select     
dim_date,

proof_of_delivery_id,
order_date,
delivery_date,
source_type,
--ids_count,
pod_status,

Customer,
warehouse,
country,
financial_administration,
account_manager,


dispatched_by,
skipped_by,
moved_by,
split_by,

case 
    when pod_status = 'DISPATCHED' then concat('dispatched_by',': ',dispatched_by)
    when pod_status = 'SKIPPED' then concat('skipped_by',': ',skipped_by) 
    else null end as action_by,



item_count,



  CASE
    WHEN months_between(to_date(delivery_date), current_date()) > 1 THEN 'Wrong date'
    WHEN to_date(delivery_date) > current_date() THEN 'Future'
    WHEN to_date(delivery_date) = current_date() THEN 'Today'
    WHEN to_date(delivery_date) < current_date() - INTERVAL 1 DAY THEN 'Past'
    ELSE 'Past'
  END AS select_delivery_date,


    current_timestamp() as insertion_timestamp

from {{ ref('int_proof_of_deliveries')}} as pod



    )

select * from source