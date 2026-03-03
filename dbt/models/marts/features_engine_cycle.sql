{{
  config(
    materialized = 'table'
  )
}}

/*
  features_engine_cycle
  ─────────────────────
  Final feature mart. Pulls from int_rolling_features (ephemeral CTE).
  This is the table ML training reads from.
*/

select
    engine_id,
    cycle,
    dataset_split,

    -- Raw informative sensors
    s2, s3, s4, s7, s8, s9, s11, s12, s13, s14, s15, s17, s20, s21,

    -- Rolling mean window=5
    rmean_s2_5, rmean_s3_5, rmean_s4_5, rmean_s7_5,
    rmean_s8_5, rmean_s9_5, rmean_s11_5, rmean_s12_5,
    rmean_s13_5, rmean_s14_5, rmean_s15_5, rmean_s17_5,
    rmean_s20_5, rmean_s21_5,

    -- Rolling std window=5
    rstd_s2_5, rstd_s3_5, rstd_s4_5, rstd_s7_5,
    rstd_s8_5, rstd_s9_5, rstd_s11_5, rstd_s12_5,
    rstd_s13_5, rstd_s14_5, rstd_s15_5, rstd_s17_5,
    rstd_s20_5, rstd_s21_5,

    -- Rolling mean window=20
    rmean_s2_20, rmean_s3_20, rmean_s4_20, rmean_s7_20,
    rmean_s8_20, rmean_s9_20, rmean_s11_20, rmean_s12_20,
    rmean_s13_20, rmean_s14_20, rmean_s15_20, rmean_s17_20,
    rmean_s20_20, rmean_s21_20,

    -- Rolling std window=20
    rstd_s2_20, rstd_s3_20, rstd_s4_20, rstd_s7_20,
    rstd_s8_20, rstd_s9_20, rstd_s11_20, rstd_s12_20,
    rstd_s13_20, rstd_s14_20, rstd_s15_20, rstd_s17_20,
    rstd_s20_20, rstd_s21_20,

    -- Lag deltas
    delta_s2, delta_s3, delta_s4, delta_s7,
    delta_s8, delta_s9, delta_s11, delta_s12,
    delta_s13, delta_s14, delta_s15, delta_s17,
    delta_s20, delta_s21,

    now() as created_at

from {{ ref('int_rolling_features') }}
