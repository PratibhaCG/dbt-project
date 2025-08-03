{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['policy_id', 'event_type', 'product_type'],
    partition_by = {
      "field": "event_month",
      "data_type": "string"
    },
    cluster_by = ["policy_id", "current_status"]
) }}

-- Event order mapping
WITH policy_event_order as (
    select 'quote' as event_type, 1 as event_order union all
    select 'bind' as event_type, 2 as event_order union all
    select 'cancel' as event_type, 3 as event_order
),

-- Attach event order and event position within each policy
curated_data as (
    select
        spe.*
        ,peo.event_order
        ,row_number() over (partition by spe.policy_id order by spe.event_timestamp nulls last) as policy_order
    from {{ ref('stg_policy_events') }} spe
    join policy_event_order peo
        on spe.event_type = peo.event_type
),

-- Exclude non-logical event sequences
filtered_curated AS (
  select *
  from curated_data
  where not (
    (policy_order = 1 and event_order != 1) -- First event is not 'quote'
      or (policy_order = 2 and event_order = 3) -- Second event is 'cancel'
  )
),

---Assumptions are the order of events is only quote -> bind -> cancel. 
---A quote can't be cancelled
---A requote comes with a new policy_id

-- Find first per event_type
firsts AS (
  SELECT
    policy_id,
    MIN(CASE WHEN event_type = 'quote' THEN event_ts END) AS first_quote_ts,
    MIN(CASE WHEN event_type = 'bind' THEN event_ts END) AS first_bind_ts,
    MIN(CASE WHEN event_type = 'cancel' THEN event_ts END) AS first_cancel_ts,
  FROM filtered_curated
  GROUP BY policy_id
),


last_evt AS (
  SELECT
    t.policy_id,
    t.event_type AS last_event_type,
    t.event_ts,
    source_channel,
    state,
    product_type,
    premium_amount,
    event_month
  FROM (
    SELECT 
      *,
      ROW_NUMBER() OVER (PARTITION BY policy_id ORDER BY event_ts DESC) AS rn
    FROM filtered_curated
  ) t
  WHERE t.rn = 1
),

final AS (
  SELECT
    f.policy_id,
    f.first_quote_ts,
    f.first_bind_ts,
    f.first_cancel_ts,
    l.last_event_type,
    l.event_ts,
    -- Status logic
    CASE
      WHEN l.last_event_type = 'cancel' THEN 'cancelled'
      WHEN l.last_event_type = 'bind' THEN 'bound'
      ELSE 'quoted'
    END AS current_status,

    -- Durations
    CASE
      WHEN f.first_quote_ts IS NOT NULL AND f.first_bind_ts IS NOT NULL
        THEN TIMESTAMP_DIFF(f.first_bind_ts, f.first_quote_ts, HOUR)
      ELSE NULL
    END AS quote_to_bind_hr,

    CASE
      WHEN f.first_bind_ts IS NOT NULL AND f.first_cancel_ts IS NOT NULL
        THEN TIMESTAMP_DIFF(f.first_cancel_ts, f.first_bind_ts, HOUR)
      ELSE NULL
    END AS bind_to_cancel_hr,
    l.event_month,
    l.source_channel,
    l.state,
    l.product_type,
    l.premium_amount
  FROM firsts f
  JOIN last_evt l ON f.policy_id = l.policy_id
)


SELECT * FROM final
