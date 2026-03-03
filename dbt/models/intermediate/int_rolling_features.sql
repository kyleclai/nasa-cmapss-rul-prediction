{{
  config(
    materialized = 'ephemeral'
  )
}}

/*
  int_rolling_features
  ────────────────────
  Computes rolling statistics over the 14 informative sensors:
    - rolling_mean / rolling_std at windows 5 and 20 cycles
    - lag delta (s - lag(s, 1))

  Window functions partition by (engine_id, dataset_split) and
  order by cycle to keep each engine's history independent.

  Note: PostgreSQL window frames default to ROWS BETWEEN UNBOUNDED PRECEDING
  AND CURRENT ROW, so we explicitly set the frame for rolling windows.
*/

{% set sensors = ['s2','s3','s4','s7','s8','s9','s11','s12','s13','s14','s15','s17','s20','s21'] %}

with base as (
    select * from {{ ref('stg_sensor_readings') }}
)

select
    engine_id,
    cycle,
    dataset_split,

    -- Raw sensor values
    {% for s in sensors %}
    {{ s }},
    {% endfor %}

    -- Rolling mean, window = 5
    {% for s in sensors %}
    avg({{ s }}) over (
        partition by engine_id, dataset_split
        order by cycle
        rows between 4 preceding and current row
    ) as rmean_{{ s }}_5,
    {% endfor %}

    -- Rolling std, window = 5
    {% for s in sensors %}
    stddev_pop({{ s }}) over (
        partition by engine_id, dataset_split
        order by cycle
        rows between 4 preceding and current row
    ) as rstd_{{ s }}_5,
    {% endfor %}

    -- Rolling mean, window = 20
    {% for s in sensors %}
    avg({{ s }}) over (
        partition by engine_id, dataset_split
        order by cycle
        rows between 19 preceding and current row
    ) as rmean_{{ s }}_20,
    {% endfor %}

    -- Rolling std, window = 20
    {% for s in sensors %}
    stddev_pop({{ s }}) over (
        partition by engine_id, dataset_split
        order by cycle
        rows between 19 preceding and current row
    ) as rstd_{{ s }}_20,
    {% endfor %}

    -- Lag delta: s - lag(s, 1)
    {% for s in sensors %}
    {{ s }} - lag({{ s }}, 1) over (
        partition by engine_id, dataset_split
        order by cycle
    ) as delta_{{ s }}{% if not loop.last %},{% endif %}
    {% endfor %}

from base
