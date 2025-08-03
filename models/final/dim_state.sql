{{ config(materialized='table') }}

SELECT DISTINCT
    state
FROM {{ ref('policy_lifecycle_enriched') }}
WHERE state IS NOT NULL