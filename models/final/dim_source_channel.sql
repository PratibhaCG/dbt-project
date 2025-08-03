{{ config(materialized='table') }}

SELECT DISTINCT
    source_channel
FROM {{ ref('policy_lifecycle_enriched') }}
