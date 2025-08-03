{{ config(
    materialized = 'view'  
) }}


SELECT event_month, 
AVG(quote_to_bind_hr) AS avg_quote_to_bind_hr
FROM {{ ref('policy_lifecycle_enriched') }}
WHERE quote_to_bind_hr IS NOT NULL
GROUP BY event_month
ORDER BY event_month


