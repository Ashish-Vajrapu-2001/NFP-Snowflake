-- ============================================================
-- SECTION 1: PII classification tag
-- Created in bronze_db — shared tag used across all layers for lineage
-- ============================================================
CREATE TAG IF NOT EXISTS NFP_BRONZE.TAGS.PII_TYPE
    ALLOWED_VALUES
        'EMAIL', 'PHONE', 'FIRST_NAME', 'LAST_NAME', 'FULL_NAME',
        'DOB', 'ADDRESS', 'POSTCODE', 'NATIONAL_ID', 'FINANCIAL', 'OTHER';

-- ============================================================
-- SECTION 2: Masking policies
-- Created in gold_db — fire ONLY when consumers query Gold tables
-- Bronze and Silver: tags set for lineage; masking policy does NOT fire here
-- Applying masking at Bronze causes transformer to read '***MASKED***' into
-- Silver permanently — this must never happen (RULE G1)
-- ============================================================
CREATE OR REPLACE MASKING POLICY NFP_GOLD.POLICIES.MASK_PII_STRING
    AS (val STRING) RETURNS STRING ->
    CASE
        WHEN IS_ROLE_IN_SESSION('NFP_COMPLIANCE_ROLE') THEN val
        WHEN IS_ROLE_IN_SESSION('NFP_ADMIN_ROLE')      THEN val
        ELSE '***MASKED***'
    END;

CREATE OR REPLACE MASKING POLICY NFP_GOLD.POLICIES.MASK_PII_DATE
    AS (val DATE) RETURNS DATE ->
    CASE
        WHEN IS_ROLE_IN_SESSION('NFP_COMPLIANCE_ROLE') THEN val
        WHEN IS_ROLE_IN_SESSION('NFP_ADMIN_ROLE')      THEN val
        ELSE DATE_FROM_PARTS(YEAR(val), 1, 1)
    END;

-- Link masking policies to PII tag (tag-based masking)
-- Policies fire automatically on Gold columns tagged with PII_TYPE
ALTER TAG NFP_BRONZE.TAGS.PII_TYPE
    SET MASKING POLICY NFP_GOLD.POLICIES.MASK_PII_STRING;

-- ============================================================
-- SECTION 3: Apply PII tags to Bronze columns
-- TAG ONLY — masking policy does NOT fire on Bronze (RULE G1)
-- transformer_role reads real data from Bronze into Silver dbt models
-- ============================================================

ALTER TABLE NFP_BRONZE.BRONZE_AMADEUS.PASSENGER_NAME_RECORD
    MODIFY COLUMN pax_first_name
    SET TAG NFP_BRONZE.TAGS.PII_TYPE = 'FIRST_NAME';

ALTER TABLE NFP_BRONZE.BRONZE_AMADEUS.PASSENGER_NAME_RECORD
    MODIFY COLUMN pax_last_name
    SET TAG NFP_BRONZE.TAGS.PII_TYPE = 'LAST_NAME';

ALTER TABLE NFP_BRONZE.BRONZE_AMADEUS.PASSENGER_NAME_RECORD
    MODIFY COLUMN date_of_birth
    SET TAG NFP_BRONZE.TAGS.PII_TYPE = 'DOB';

ALTER TABLE NFP_BRONZE.BRONZE_AMADEUS.PASSENGER_NAME_RECORD
    MODIFY COLUMN passport_number
    SET TAG NFP_BRONZE.TAGS.PII_TYPE = 'NATIONAL_ID';

-- ============================================================
-- NOTE: Silver PII tag application
-- Silver column tagging is handled in the Silver dbt project
-- using dbt meta tags and post-hooks.
-- Gold masking fires automatically via tag-based masking
-- when consumers query Gold views/tables.
-- ============================================================