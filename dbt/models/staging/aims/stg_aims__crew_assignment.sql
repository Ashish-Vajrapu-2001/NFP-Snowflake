{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='assignment_id',
    merge_exclude_columns=['_silver_load_timestamp'],
    on_schema_change='append_new_columns',
    tags=['silver', 'aims']
) }}

WITH source AS (
    SELECT
        ASSIGNMENT_ID,
        FLIGHT_ID,
        EMPLOYEE_ID,
        CREW_ROLE,
        CREW_TYPE,
        DUTY_START,
        DUTY_END,
        BLOCK_HOURS,
        HOURLY_RATE,
        CREW_COST,
        OVERTIME_FLAG,
        OVERTIME_HOURS,
        PER_DIEM,
        BASE_STATION,
        _FIVETRAN_SYNCED,
        _FIVETRAN_DELETED
    FROM {{ source('bronze_aims', 'CREW_ASSIGNMENT') }}

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
            PARTITION BY ASSIGNMENT_ID
            ORDER BY _FIVETRAN_SYNCED DESC
        ) AS _rn
    FROM active_records
),

transformed AS (
    SELECT
        ASSIGNMENT_ID,
        FLIGHT_ID,
        EMPLOYEE_ID,
        CREW_ROLE,
        CREW_TYPE,
        TRY_TO_TIMESTAMP_NTZ(DUTY_START) AS DUTY_START,
        TRY_TO_TIMESTAMP_NTZ(DUTY_END) AS DUTY_END,
        CAST(BLOCK_HOURS AS NUMBER(10,2)) AS BLOCK_HOURS,
        HOURLY_RATE,
        CREW_COST,
        OVERTIME_FLAG,
        OVERTIME_HOURS,
        PER_DIEM,
        BASE_STATION,
        _FIVETRAN_SYNCED          AS _bronze_sync_timestamp,
        CURRENT_TIMESTAMP()       AS _silver_load_timestamp,
        '{{ invocation_id }}'     AS _dbt_run_id,
        _FIVETRAN_DELETED         AS _is_deleted
    FROM deduped
    WHERE _rn = 1
)

SELECT * FROM transformed