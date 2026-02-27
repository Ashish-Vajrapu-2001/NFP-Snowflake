{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['ticket_number', 'coupon_number'],
    merge_exclude_columns=['_silver_load_timestamp'],
    on_schema_change='append_new_columns',
    tags=['silver', 'amadeus']
) }}

WITH source AS (
    SELECT
        TICKET_NUMBER,
        COUPON_NUMBER,
        PNR_LOCATOR,
        PAX_ID,
        FLIGHT_NUMBER,
        FLIGHT_DATE,
        TOTAL_AMOUNT,
        COMMISSION_AMOUNT,
        TICKET_STATUS,
        EXCHANGE_TICKET,
        _FIVETRAN_SYNCED,
        _FIVETRAN_DELETED
    FROM {{ source('bronze_amadeus', 'TICKET_COUPON') }}

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
    WHERE (_FIVETRAN_DELETED = FALSE OR _FIVETRAN_DELETED IS NULL)
      AND TICKET_STATUS = 'FLOWN'
      AND TICKET_STATUS NOT IN ('VOID', 'REFUNDED')
),

deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY TICKET_NUMBER, COUPON_NUMBER
            ORDER BY _FIVETRAN_SYNCED DESC
        ) AS _rn
    FROM active_records
),

transformed AS (
    SELECT
        TICKET_NUMBER,
        COUPON_NUMBER,
        PNR_LOCATOR,
        PAX_ID,
        FLIGHT_NUMBER,
        TRY_TO_DATE(FLIGHT_DATE) AS FLIGHT_DATE,
        TOTAL_AMOUNT,
        COMMISSION_AMOUNT,
        (TOTAL_AMOUNT - COALESCE(COMMISSION_AMOUNT, 0)) AS NET_PASSENGER_REVENUE,
        TICKET_STATUS,
        EXCHANGE_TICKET,
        _FIVETRAN_SYNCED          AS _bronze_sync_timestamp,
        CURRENT_TIMESTAMP()       AS _silver_load_timestamp,
        '{{ invocation_id }}'     AS _dbt_run_id,
        _FIVETRAN_DELETED         AS _is_deleted
    FROM deduped
    WHERE _rn = 1
)

SELECT * FROM transformed