{{
  config(
    materialized = 'table'
  )
}}

/*
  labels_rul
  ──────────
  Computes Remaining Useful Life (RUL) for every (engine_id, cycle) row.

  Method: piecewise linear RUL (Heimes 2008)
    - For each engine, max_cycle = last observed cycle.
    - raw_rul = max_cycle - current_cycle
    - true_rul = LEAST(raw_rul, 125)   -- cap at 125 to avoid over-weighting
                                          healthy early cycles

  For test split: raw_rul is computed from the observed test window only.
  The true end-of-life offset for test engines is available in RUL_FD001.txt,
  but is applied during scoring (not here) to keep this model self-contained.

  failure_flag = 1 only at the last observed cycle (raw_rul = 0).
*/

with max_cycles as (
    select
        engine_id,
        dataset_split,
        max(cycle) as max_cycle
    from {{ source('warehouse', 'raw_sensor_readings') }}
    group by engine_id, dataset_split
),

rul_raw as (
    select
        r.engine_id,
        r.cycle,
        r.dataset_split,
        m.max_cycle,
        (m.max_cycle - r.cycle)                         as raw_rul,
        least((m.max_cycle - r.cycle), 125)             as true_rul,
        case when r.cycle = m.max_cycle then 1 else 0 end as failure_flag
    from {{ source('warehouse', 'raw_sensor_readings') }} r
    join max_cycles m
        on r.engine_id = m.engine_id
        and r.dataset_split = m.dataset_split
)

select
    engine_id,
    cycle,
    dataset_split,
    max_cycle,
    true_rul,
    failure_flag,
    now() as created_at
from rul_raw
