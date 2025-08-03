{{ config(materialized='table') }}

SELECT DISTINCT
    product_type
FROM {{ ref('policy_lifecycle_enriched') }}
WHERE product_type IS NOT NULL
