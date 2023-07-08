with

source as ( 

select

number as invoice_number,
financial_administration,
invoice_header_id,
invoice_header_printed_at,
invoice_header_status, --Draft,signed,Open,Printed,Closed,Canceled,Rejected,voided
invoice_header_type, --credit note, invoice
generation_type,
proof_of_delivery_id,

printed_by,
customer,
customer_id,
city,
user_category,
customer_type,
payment_term,
account_manager,
country,
debtor_number,
warehouse,
company_name,

--fct
remaining_amount,
paid_amount,
total_amount_without_tax,




--stg_invoice_items
    items_count,
    quantity,
    delivery_date,
    match_check,
    match_check_gap,
    total_cost,


current_timestamp() as insertion_timestamp 


from {{ref('int_invoices')}} as i
)

select * from source

