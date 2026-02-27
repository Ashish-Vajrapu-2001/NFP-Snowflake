{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='flight_id',
    merge_exclude_columns=['_silver_load_timestamp'],
    on_schema_change='append_new_columns',
    tags=['silver', 'aims']
) }}

WITH source AS (
    SELECT
        FLIGHT_ID,
        FLIGHT_NUMBER,
        FLIGHT_DATE,
        SERVICE_TYPE,
        ORIGIN,
        DESTINATION,
        SCHEDULED_DEP,
        SCHEDULED_ARR,
        ACTUAL_DEP,
        ACTUAL_ARR,
        AIRCRAFT_REG,
        AIRCRAFT_TYPE,
        BLOCK_HOURS,
        FLIGHT_HOURS,
        DISTANCE_KM,
        SEAT_CAPACITY,
        PAX_BOOKED,
        PAX_BOARDED,
        LOAD_FACTOR,
        CARGO_CAPACITY_KG,
        CARGO_BOOKED_KG,
        CARGO_LOADED_KG,
        FLIGHT_STATUS,
        CANCELLED_FLAG,
        DIVERTED_FLAG,
        _FIVETRAN_SYNCED,
        _FIVETRAN_DELETED
    FROM {{ source('bronze_aims', 'FLIGHT_LEG') }}

    {% if is_incremental() %}
        WHERE _FIVETRAN_SYNCED > (
            SELECT COALESCE(
                MAX(_bronze_sync_timestamp),
                '{{ var("min_date", "2000-01-01") }}'::TIMESTAMP_TZ
            )
            FROM {{ this }}
        )
        OR _FIVETRAN_SYNCED >= DATEADD(
            'day',
            -{{ var('silver_lookback_days', 3) }},
            CURRENT_TIMESTAMP()
        )
    {% endif %}

    {{ limit_dates_for_dev('FLIGHT_DATE') }}
),

active_records AS (
    SELECT *
    FROM source
    WHERE _FIVETRAN_DELETED = FALSE
       OR _FIVETRAN_DELETED IS NULL
),

deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY FLIGHT_ID
            ORDER BY _FIVETRAN_SYNCED DESC
        ) AS _rn
    FROM active_records
),

transformed AS (
    SELECT
        FLIGHT_ID,
        CAST(FLIGHT_NUMBER AS VARCHAR) AS FLIGHT_NUMBER,
        TRY_TO_DATE(FLIGHT_DATE) AS FLIGHT_DATE,
        SERVICE_TYPE,
        ORIGIN,
        DESTINATION,
        TRY_TO_TIMESTAMP_NTZ(SCHEDULED_DEP) AS SCHEDULED_DEP,
        TRY_TO_TIMESTAMP_NTZ(SCHEDULED_ARR) AS SCHEDULED_ARR,
        TRY_TO_TIMESTAMP_NTZ(ACTUAL_DEP) AS ACTUAL_DEP,
        TRY_TO_TIMESTAMP_NTZ(ACTUAL_ARR) AS ACTUAL_ARR,
        AIRCRAFT_REG,
        AIRCRAFT_TYPE,
        CAST(BLOCK_HOURS AS NUMBER(10,2)) AS BLOCK_HOURS,
        CAST(FLIGHT_HOURS AS NUMBER(10,2)) AS FLIGHT_HOURS,
        DISTANCE_KM,
        SEAT_CAPACITY,
        PAX_BOOKED,
        PAX_BOARDED,
        LOAD_FACTOR,
        CARGO_CAPACITY_KG,
        CARGO_BOOKED_KG,
        CARGO_LOADED_KG,
        FLIGHT_STATUS,
        CANCELLED_FLAG,
        DIVERTED_FLAG,
        _FIVETRAN_SYNCED          AS _bronze_sync_timestamp,
        CURRENT_TIMESTAMP()       AS _silver_load_timestamp,
        '{{ invocation_id }}'     AS _dbt_run_id,
        _FIVETRAN_DELETED         AS _is_deleted
    FROM deduped
    WHERE _rn = 1
)

SELECT * FROM transformed