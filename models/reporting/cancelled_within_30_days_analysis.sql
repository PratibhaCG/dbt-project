{{ config(
    materialized = 'view' 
) }}

SELECT COUNT(*) AS num_cancelled_within_30_days
FROM {{ ref('policy_lifecycle_enriched') }}
WHERE quote_to_bind_hr IS NOT NULL
  AND bind_to_cancel_hr IS NOT NULL
  AND bind_to_cancel_hr <= 30*24


