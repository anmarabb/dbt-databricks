with orders as 

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

invoice as 

        (

        select
            customer_id,
            datediff(current_date(), CAST(MAX(i.invoice_header_printed_at) AS DATE)) AS days_since_last_drop,
            sum(case when i.invoice_header_printed_at is not null then i.remaining_amount else 0 end) as total_outstanding_balance --total_remaining_amount
        from {{ ref('int_invoices') }} as i  
        GROUP BY
        customer_id


        ),

invoice_items as
        (
        select
            customer_id,
            count(distinct ii.invoice_header_id) as total_order_count_per_customer,
            sum( ii.price_without_tax) as total_order_value_per_customer
        from {{ ref('int_invoice_items') }} as ii  
        GROUP BY
        customer_id
        )
select
u.id as customer_id,
o.last_order_date

from   {{ ref('base_users') }} as u 
left join orders as o on u.id = o.customer_id
left join invoice as i on u.id = i.customer_id
left join invoice_items as ii on u.id = ii.customer_id