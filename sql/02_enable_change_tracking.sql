-- ============================================================
-- Run this on the AZURE SQL SOURCE — not on Snowflake
-- Requires ALTER DATABASE permission on the source database
-- Only needed for sources where update_method = NATIVE_UPDATE
-- Sources using TELEPORT do not require Change Tracking
-- ============================================================

-- SOURCE: AIMS (SRC-003)
ALTER DATABASE AIMS
    SET CHANGE_TRACKING = ON
    (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON);

-- Enable per INCREMENTAL table — write every table, no abbreviations
ALTER TABLE AIMS.FLIGHT_LEG
    ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = ON);

-- SOURCE: EPICOR (SRC-004)
ALTER DATABASE EPICOR
    SET CHANGE_TRACKING = ON
    (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON);

-- Enable per INCREMENTAL table — write every table, no abbreviations
ALTER TABLE EPICOR.GL_JOURNAL_ENTRY
    ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = ON);

ALTER TABLE EPICOR.FUEL_INVOICE
    ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = ON);

ALTER TABLE EPICOR.FLIGHT_COST_ALLOCATION
    ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = ON);

-- Verification query (run after enabling)
SELECT t.name AS table_name,
       ct.is_track_columns_updated_on
FROM sys.tables t
JOIN sys.change_tracking_tables ct ON t.object_id = ct.object_id
ORDER BY t.name;