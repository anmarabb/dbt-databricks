WITH source AS (
  SELECT 
    a.account_manager_type,
    u.name AS account_manager,
    f.name AS fin_market
  FROM {{ source('1_source', 'account_managers') }} AS a
  LEFT JOIN {{ source('1_source', 'users') }} AS u ON a.user_id = u.id
  LEFT JOIN {{ source('1_source', 'financial_administrations') }} AS f ON f.id = u.financial_administration_id
)
SELECT 
  *,
  current_timestamp() AS ingestion_timestamp
FROM source AS ii
