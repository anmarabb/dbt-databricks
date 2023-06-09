With source as (
 select * from {{ source('erp', 'master_shipments') }}
)
select 

            --PK
                id as master_shipment_id,
            --FK
                warehouse_id,
                customer_id,



            --date
                created_at,
                updated_at,
               departure_date,
                canceled_at,
                deleted_at,
                CASE WHEN origin IN ('CO', 'NL') THEN date_add(departure_date, 1) ELSE departure_date END AS arrival_date,
                CASE WHEN YEAR(arrival_time) <> 202213 THEN arrival_time ELSE NULL END AS arrival_at,




                



            --dim
                destination,
                
                total_fob,
                customer_type,  

                status as master_shipments_status, --CANCELED, MISSING, PACKED, WAREHOUSED,CANCELED,DRAFT
                name as master_shipment_name,
                fulfillment as master_shipments_fulfillment_status, --UNACCOUNTED, PARTIAL, SUCCEED

                origin,
                order_sequence,
                note,

                freight_currency,
                master_invoice_currency,
                clearance_currency,
                cancellation_reason,
                case when msh.customer_id is not null then 'Bulk shipments' else null end as shipment_type,
                concat( "https://erp.floranow.com/master_shipments/", msh.id) as master_shipment_link,



            --fct
                total_quantity as master_total_quantity,
                clearance_cost,
                master_invoice_cost,
                freight_cost,
            
            







current_timestamp() as ingestion_timestamp




from source as msh