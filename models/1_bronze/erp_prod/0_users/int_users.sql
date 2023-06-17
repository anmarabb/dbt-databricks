with prep_orders as 

(

        select 
        customer_id,
        MAX(li.created_at) AS last_order_date,
        datediff(current_date(), to_date(cast(max(li.created_at) as string))) as days_since_last_order,

        CASE 
            WHEN datediff(current_date(), to_date(cast(max(li.created_at) as string))) <= 7 THEN 'active'
            WHEN datediff(current_date(), to_date(cast(max(li.created_at) as string))) > 7 AND datediff(current_date(), to_date(cast(max(li.created_at) as string))) <= 30 THEN 'inactive'
            WHEN datediff(current_date(), to_date(cast(max(li.created_at) as string))) > 30 THEN 'churned'
            ELSE 'churned'
        END as Account_Status

        from {{ ref('int_line_items') }} as li  
        GROUP BY
        customer_id

),

prerp_invoice as 

        (

select

customer_id,
datediff(current_date(), CAST(MAX(i.invoice_header_printed_at) AS DATE)) AS days_since_last_drop,

sum(case when i.invoice_header_printed_at is not null then i.remaining_amount else 0 end) as total_outstanding_balance --total_remaining_amount
from {{ ref('int_invoices') }} as i  
GROUP BY
customer_id


        )

select
*
from
prep_orders
left join prerp_invoice on prerp_invoice.customer_id = prep_orders.customer_id