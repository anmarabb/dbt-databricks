with

source as ( 

select

--invoice Items
    
    --fct

        MTD_sales,
        LMTD_sales,
        MTD_sales_last_year,
        m_1_sales,
        m_1_sales_last_year,
        YTD_sales,
        LYTD_sales,
        
        price_without_tax,
        price,
        total_tax,

        invoiced_quantity,


    --dim
        invoice_item_id,
        financial_administration,
        source_type, --ERP, Florisft
        invoice_item_type,
        invoice_item_status,
        Customer,
        debtor_number,
        company_name,
        account_manager,
        customer_type,
        user_category,
        product_name as Product,

        meta_supplier_name,
        meta_supplier_code,
        meta_supplier,
        
     --date
        order_date,
        delivery_date,
        deleted_at,




--Line Items

    --dim
        line_item_id,
        
        fulfillment_mode, --Inventory Orders (Stock-out), Shipment Orders
        order_status, ----Dispatched, Fulfilled Full Incident, Fulfilled Not Dispatched, Dispatched, Fulfilled Not Dispatched

        line_item_supplier,

    --fct
        ordered_quantity,
        fulfilled_quantity,




--invoice Header

    --dim
        invoice_header_id,
        invoice_header_status, --draft, open, printed, signed, closed, canceled, rejected, voided
        invoice_header_type, --credit note, invoice
        generation_type,
        record_type, --Invoice - MANUAL, Credit Note - MANUAL, Invoice - MANUAL
        record_type_details, --Customer Fly Order, Customer Inventory Order, Customer Shipment Order




    --date
        invoice_header_created_at,
        invoice_header_printed_at,


--stitsh
    Supplier,
    Origin,


current_timestamp() as insertion_timestamp 


from {{ref('int_invoice_items')}} as ii 
)

select * from source

--where invoice_type != 'credit note' and generation_type !='MANUAL'
