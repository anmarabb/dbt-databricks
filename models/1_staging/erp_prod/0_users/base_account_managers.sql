With source as (


select 

a.account_manager_type,
u.name as account_manager,
f.name as fin_market,

from {{ source('1_source', 'account_managers') }} as a
left join {{ source('1_source', 'users') }} as u on a.user_id = u.id
left join {{ source('1_source', 'financial_administrations') }} as f on f.id = u.financial_administration_id



)
select 

*,

current_timestamp() as ingestion_timestamp,

 




from source as ii