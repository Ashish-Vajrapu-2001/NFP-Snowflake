# Silver Layer Deployment Guide

## Prerequisites
*   Python 3.8+
*   dbt-snowflake
*   Git access to `nfp_analytics_dbt`
*   Snowflake role `NFP_TRANSFORMER_ROLE`
*   Bronze tables populated by Fivetran

## Environment Variables
Set the following before running dbt:
*   `DBT_SNOWFLAKE_ACCOUNT`
*   `DBT_SNOWFLAKE_USER`
*   `DBT_SNOWFLAKE_PASSWORD`
*   `DBT_BRONZE_DB` (e.g., `NFP_BRONZE`)
*   `DBT_SILVER_DB` (e.g., `NFP_SILVER`)

## Deployment Steps

1.  **Install Dependencies**