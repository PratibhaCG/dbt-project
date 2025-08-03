{{ config(
    materialized = 'view'  
) }}

SELECT COUNT(*) AS num_quoted_never_bound
FROM {{ ref('policy_lifecycle_enriched') }}
WHERE first_quote_ts IS NOT NULL 
    AND first_bind_ts IS NULL


