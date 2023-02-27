SELECT DISTINCT
  DATE_TRUNC('MONTH', i.last_seen) AS month,
  i.item_id,
  i.name AS item_name,
  i.price,
  i.currency_code,
  si.store_name,
  si.external_store_id,
  si.raw_tags AS categories,
  CASE WHEN CARDINALITY(si.inferred_cuisine_tags) > 0 THEN si.inferred_cuisine_tags[1] ELSE NULL END AS primary_cuisine,
  si.latitude,
  si.longitude,
  i.service_slug,
  'marketintel' AS source
  
FROM iceberg.marketintel_raw.item_snapshots i
LEFT JOIN iceberg.marketintel_raw.storefront_snapshots si
  ON si.external_store_id = i.external_store_id AND si.super_region = '{super_region}' AND si.service_slug = '{service_slug}'
  
WHERE 1=1
    AND si.last_seen BETWEEN DATE('{begin_date}') AND DATE('{end_date}')
    AND i.super_region = '{super_region}'
    AND i.service_slug = '{service_slug}'
    
    AND i.name IS NOT NULL
    AND si.store_name IS NOT NULL