# NFP Analytics - Silver Layer

This dbt project transforms raw Bronze data into the Silver layer for Net Flight Profitability analytics.

## Architecture
*   **Source:** Fivetran-loaded Bronze tables in Snowflake.
*   **Transformation:** dbt Core (Incremental models & Snapshots).
*   **Target:** Silver schemas in Snowflake (`SILVER_AIMS`, `SILVER_AMADEUS`, etc.).

## Models
*   **Staging (SCD1):** 10 incremental models cleaning and deduplicating operational data.
*   **Snapshots (SCD2):** 3 snapshots tracking history for fleet and reference data.

## Quick Start