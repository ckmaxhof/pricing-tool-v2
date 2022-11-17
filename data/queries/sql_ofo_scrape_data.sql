SELECT DISTINCT
  i.name AS item_name,
  i.price,
  i.currency_code,
  si.store_name,
  si.external_store_id,
  si.raw_tags AS categories,
  CASE WHEN CARDINALITY(si.raw_tags) > 0 THEN si.raw_tags[1] ELSE NULL END AS primary_cuisine,
  si.latitude,
  si.longitude,
  si.service_slug,
  DATE_TRUNC('MONTH', si.last_seen) AS month,
  'marketintel' AS source
  
FROM hudi_ingest.scratch.marketintel_item i
LEFT JOIN hudi_ingest.scratch.marketintel_store_info si
  ON si.external_store_id = i.external_store_id 
  AND si.collection = i.collection 
  AND si.service_slug = i.service_slug
  
WHERE 1=1
  AND si.collection = '{collection}'
  AND i.collection = '{collection}'
  AND i.name IS NOT NULL
  AND si.last_seen BETWEEN DATE('{begin_date}') AND DATE('{end_date}')
  AND i.service_slug = '{service_slug}'
  AND si.service_slug = '{service_slug}'
  
LIMIT 
  