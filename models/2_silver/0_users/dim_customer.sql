with

source as ( 
        
select     
    u.id as customer_id,
    u.name,
    
    u.city,
    u.state,
    row_country,
    country,

    u.debtor_number,
    u.account_type,
    u.customer_type,
    u.odoo_code,
    u.statement_type,

    accessible_warehouses,
    commercial_register,
    lpo_number,
    accessible_internal_stocks,
    order_block,
    u.has_all_warehouses_access,
    u.has_trade_access,
    u.allow_due_invoices,
    u.customized_invoice,
    u.with_stamp,

    current_timestamp() as insertion_timestamp 

from {{ ref('base_users')}} as u
--where u.account_type = 'External' and u.customer_type != 'reseller'

)

select * from source

