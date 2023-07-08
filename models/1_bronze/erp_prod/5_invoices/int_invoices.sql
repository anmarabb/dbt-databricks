with stg_invoice_items as

    (

        SELECT
                ii.invoice_id,
                count(distinct ii.id) as items_count,
                CASE WHEN max(date(i.printed_at)) > max(ii.delivery_date) then 'late_delivery' else 'on_time_delivery' End as otd_check,
                sum(ii.price_without_tax) price_without_tax,
                --sum(case when li.supplier_id IN (109,71) then ii.price_without_tax else 0 end) as express_revenue,
                --sum(case when li.supplier_id is null then ii.price_without_tax else 0 end) as manual_revenue,
                --sum(case when li.supplier_id not IN (109,71) and li.supplier_id is not null then ii.price_without_tax else 0 end) as NonExpress_revenue,
                sum(ii.quantity * li.unit_landed_cost)  as total_cost,
                --sum(ii.price_without_tax) - sum(ii.quantity * li.unit_landed_cost) as profit,
                sum(ii.quantity) as quantity,
                max(ii.delivery_date) as delivery_date
        from {{ source(var('erp_source'), 'invoice_items') }}  as ii 
        left join {{ source(var('erp_source'), 'invoices') }} as i on ii.invoice_id = i.id
        left join {{ source(var('erp_source'), 'line_items') }} as li on ii.line_item_id = li.id
        where ii.status = 'APPROVED' and ii.deleted_at is null
        group by ii.invoice_id

    )
        
select     

i.*,


    customer.city,
    customer.name as customer,
    printed_by.name as printed_by,
    customer.user_category,
    customer.customer_type,
    customer.payment_term,
    customer.account_manager,
    customer.country,
    customer.debtor_number,
    customer.warehouse,
    customer.company_name,


--stg_invoice_items
    stg_invoice_items.items_count,
   -- stg_invoice_items.otd_check ,
    --stg_invoice_items.price_without_tax as ii_price_without_tax ,
    --stg_invoice_items.express_revenue ,
   --stg_invoice_items.manual_revenue ,
    --stg_invoice_items.NonExpress_revenue ,
    stg_invoice_items.total_cost,
    --stg_invoice_items.profit,
    stg_invoice_items.quantity,
    stg_invoice_items.delivery_date,



(i.total_amount - i.total_tax) - stg_invoice_items.price_without_tax as match_check_gap,

i.total_amount - i.total_tax = stg_invoice_items.price_without_tax as match_check


from {{ ref('stg_invoices')}} as i
left join {{ ref('base_users') }} as customer on customer.id = i.customer_id
left join {{ ref('base_users') }} as printed_by on printed_by.id = i.printed_by_id

left join stg_invoice_items as stg_invoice_items on stg_invoice_items.invoice_id = i.invoice_header_id

