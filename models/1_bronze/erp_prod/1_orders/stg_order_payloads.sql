WITH source AS (
  SELECT *,
         from_json(marketplace_request, 'user_id STRING, customer_id STRING, order_type STRING, tags ARRAY<STRING>, offer_id STRING') AS marketplace_request_parsed
  FROM {{ source(var('erp_source'), 'order_payloads') }}
)
SELECT 
  id AS order_payload_id,
  third_party_request,
  third_party_response,
  created_at,
  updated_at,
  status,
  response_code,
  meta_data,
  marketplace_request_parsed.user_id, 
  marketplace_request_parsed.customer_id, 
  marketplace_request_parsed.order_type, 
  marketplace_request_parsed.tags, 
  marketplace_request_parsed.offer_id,
  -- ...
  job_id,
  current_timestamp() AS ingestion_timestamp
FROM source AS opl
