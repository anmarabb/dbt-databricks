WITH
    source AS
    (
        SELECT
            -- PK
            li.id AS line_item_id,
            -- FK
            li.invoice_id,
            li.order_id,
            li.order_payload_id,
            li.order_request_id,
            li.customer_id,
            li.customer_master_id,
            li.user_id,
            li.reseller_id,
            li.supplier_id,
            li.offer_id,
            li.feed_source_id,
            li.shipment_id,
            li.source_shipment_id,
            li.root_shipment_id,
            li.proof_of_delivery_id,
            li.supplier_product_id,
            li.source_line_item_id,
            li.parent_line_item_id,
            li.split_source_id,
            li.dispatched_by_id,
            li.canceled_by_id,
            li.returned_by_id,
            li.created_by_id,
            li.split_by_id,
            li.replace_for_id,
            li.source_invoice_id,
            -- dim
            -- date
            li.departure_date,
            li.delivery_date,
            li.created_at,
            li.completed_at,
            li.dispatched_at,
            li.delivered_at,
            li.deleted_at,
            li.canceled_at,
            li.split_at,
            li.returned_at,
            li.updated_at,
            li.fulfillment,
            li.location,
            li.state,
            li.creation_stage,
            li.ordering_stock_type,
            li.order_type,
            li.sales_unit,
            li.sales_unit_name,
            --li.permalink,
            li.sequence_number,
            li.barcode,
            li.number,
            li.variety_mask,
            li.product_mask,
            li.previous_moved_proof_of_deliveries,
            li.previous_split_proof_of_deliveries,
            li.previous_shipments,
            li.order_number,
            li.tags,
            li.pricing_type,
            li.landed_currency,
            li.fob_currency,
            li.currency,
            li.product_name,
            li.properties,
            li.categorization,
            li.stem_length,
            li.color,
            get_json_object(pn, '$.p1') AS spec_1,
            get_json_object(pn, '$.p2') AS spec_2,
            get_json_object(pn, '$.p3') AS spec_3,
            li.unit_landed_cost,
            li.unit_fob_price,
            li.unit_price,
            li.exchange_rate,
            li.total_price_without_tax,
            li.total_price_include_tax,
            li.total_tax,
            li.unit_shipment_cost,
            li.unit_additional_cost,
            li.quantity,
            li.fulfilled_quantity,
            li.received_quantity,
            li.inventory_quantity,
            li.missing_quantity,
            li.damaged_quantity,
            li.delivered_quantity,
            li.extra_quantity,
            li.returned_quantity,
            li.canceled_quantity,
            li.picked_quantity,
            li.replaced_quantity,
            li.splitted_quantity,
            li.warehoused_quantity,
            li.published_canceled_quantity,


        case 
            when li.source_line_item_id is null and li.parent_line_item_id is null and li.ordering_stock_type is null and li.reseller_id is not null then 'Reseller Purchase Order' --from reseller to feed the stock
            when li.source_line_item_id is null and li.parent_line_item_id is null and li.ordering_stock_type is null and li.reseller_id is null and li.pricing_type in ('FOB','CIF') then 'Customer Bulk Order'
            when li.source_line_item_id is null and li.parent_line_item_id is null and li.ordering_stock_type is null and li.reseller_id is null then 'Customer Shipment Order' --customer_direct_orders
            when li.source_line_item_id is null and li.ordering_stock_type = 'INVENTORY' and li.reseller_id is null and li.order_type = 'IN_SHOP' then 'Customer In Shop Order'
            when li.source_line_item_id is null and li.ordering_stock_type = 'INVENTORY' and li.reseller_id is null then 'Customer Inventory Order' --customer_inventory_orders
            when li.source_line_item_id is null and li.ordering_stock_type = 'FLYING' and li.reseller_id is null then 'Customer Fly Order' --customer_inventory_orders_flying
            when li.source_line_item_id is null and li.ordering_stock_type is not null and li.reseller_id is not null then 'stock2stock'
            when li.source_line_item_id is not null and li.order_type = 'EXTRA' then 'EXTRA'
            when li.source_line_item_id is not null and li.order_type = 'RETURN' then 'RETURN' 
            when li.source_line_item_id is not null and li.order_type = 'MOVEMENT' then 'MOVEMENT'
            else 'cheack_my_logic'
            end as record_type_details,

        case 
            when li.source_line_item_id is null and li.parent_line_item_id is null and li.ordering_stock_type is null and li.reseller_id is not null then 'Purchase Order'
            when li.source_line_item_id is null and li.parent_line_item_id is null and li.ordering_stock_type is null and li.reseller_id is null and li.pricing_type in ('FOB','CIF') then 'Customer Order'
            when li.source_line_item_id is null and li.parent_line_item_id is null and li.ordering_stock_type is null and li.reseller_id is null then 'Customer Order' --customer_direct_orders
            when li.source_line_item_id is null and li.ordering_stock_type is not null and li.reseller_id is null then 'Customer Order' --customer_inventory_orders
            when li.source_line_item_id is null and li.ordering_stock_type is not null and li.reseller_id is not null then 'System'
            when li.source_line_item_id is not null and li.order_type = 'EXTRA' then 'System'
            when li.source_line_item_id is not null and li.order_type = 'RETURN' then 'System' 
            when li.source_line_item_id is not null and li.order_type = 'MOVEMENT' then 'System'
            else 'cheack_my_logic'
            end as record_type,


            regexp_extract(permalink, r'/([^/]+)') as product_crop,
            regexp_extract(permalink, r'/(?:[^/]+)/([^/]+)') as product_category,
            regexp_extract(permalink, r'/(?:[^/]+/){2}([^/]+)') as product_subcategory
           
           from {{ source('erp', 'line_items') }} as li

        
    )

SELECT
    *,
    current_timestamp AS ingestion_timestamp
FROM source AS li
