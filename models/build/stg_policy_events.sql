{{ config(materialized = 'table') }}

WITH ranked_events AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY policy_id, event_type, product_type
      ORDER BY event_timestamp DESC
    ) AS row_num
  FROM {{ ref('raw_policy_events') }}
)

SELECT
  *,
  event_timestamp AS event_ts,
  FORMAT_DATE('%Y-%m', DATE(event_timestamp)) AS event_month
FROM ranked_events
WHERE row_num = 1

-- Assumption made that (policy_id, event_type, product_type) combination is the business key 
-- however needs to confirmed by the business
