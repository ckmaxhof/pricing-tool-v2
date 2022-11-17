SELECT
  
  TRIM(normalized_item_name) AS item_name,
  CASE WHEN brand_name IS NULL THEN organization_name ELSE brand_name END AS brand_name,
  CASE WHEN prep_organization_id = 'ac56d23b-a6a2-4c49-8412-a0a0949fb5ef' THEN TRUE ELSE FALSE END AS is_ff_brand,
  facility_latitude  AS latitude,
  facility_longitude AS longitude,
  facility_timezone AS timezone,
  facility_country_code AS country_code,
  DATE_TRUNC('MONTH', DATE(day_partition)) AS month,
  COUNT(DISTINCT day_partition) AS number_of_days,
  ROUND(SUM(quantity), 2) AS total_qty_ordered,
  COUNT(DISTINCT order_id) AS total_orders,
  ROUND(SUM(quantity) / COUNT(DISTINCT day_partition), 2) AS quantity_per_day,
  ROUND(SUM(quantity) / COUNT(DISTINCT order_id), 2) AS quantity_per_order,
  ROUND(SUM(total) / SUM(quantity), 2) AS mean_price,
  ROUND(MAX(total/quantity), 2) AS max_price,
  ROUND(MAX(total/quantity), 2) AS min_price

FROM hudi_ingest.api_orders.customer_order_items 

WHERE 1=1
  AND day_partition BETWEEN CAST('{begin_date}' AS VARCHAR) AND CAST('{end_date}' AS VARCHAR)
  AND is_cancelled = FALSE
  AND facility_country_code = '{country_code}'
  AND is_parent = TRUE
  AND normalized_item_name IS NOT NULL
  AND total IS NOT NULL
  AND quantity IS NOT NULL
  
GROUP BY 1,2,3,4,5,6,7,8

LIMIT 10