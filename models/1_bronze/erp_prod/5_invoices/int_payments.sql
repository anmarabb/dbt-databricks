with

source as ( 
        
select     

py.*,

customer.name as customer,
customer2.name as customer2,

    current_timestamp() as insertion_timestamp

from {{ ref('stg_payments')}} as py
left join {{ ref('int_invoices') }} as i on py.invoice_id = i.invoice_header_id
left join {{ ref('base_users') }} as customer on customer.id = py.user_id

left join {{ ref('base_users') }} as customer2 on customer2.id = i.customer_id






    )

select * from source