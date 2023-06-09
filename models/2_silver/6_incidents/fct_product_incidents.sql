-- fullfeled (added to loc, and genrate product_location recourd)
--

with

source as ( 

 
select 
    product_incident_id,
    line_item_id,

    created_at as incident_at,

    incident_quantity,
   
    
    incident_type,  --MISSING, EXTRA, DAMAGED, RETURNED
    stage,          --PACKING, RECEIVING, INVENTORY, DELIVERY, AFTER_RETURN

    reported_by,

    accountable_type,
    Accountable,
    customer,
    Supplier,


    


    --line_item
        
        


current_timestamp() as insertion_timestamp 


from {{ref('int_product_incidents')}} as pi 

)

select * from source

