{{ config(materialized='table') }}

SELECT DISTINCT
    last_event_type
FROM {{ ref('policy_lifecycle_enriched') }}
