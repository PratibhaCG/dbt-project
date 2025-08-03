{{ config(materialized = 'table')}}

SELECT
    policy_id,
    event_type,
    CAST(event_timestamp AS TIMESTAMP) AS event_timestamp,
    source_channel,
    state,
    product_type,
    CAST(premium_amount AS FLOAT64) AS premium_amount
FROM
    {{ source('policy_events', 'policy_events_external_table') }}

