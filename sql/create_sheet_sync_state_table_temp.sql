CREATE TABLE IF NOT EXISTS `b25h01-ragic.ragic_backup.sheet_sync_state` (
    sheet_code STRING NOT NULL OPTIONS(description="Ragic Sheet Code (e.g., '10', '99')"),
    last_sync_timestamp TIMESTAMP NOT NULL OPTIONS(description="Last successful sync timestamp for this sheet (UTC)"),
    updated_at TIMESTAMP OPTIONS(description="Timestamp when this record was last updated")
)
PARTITION BY DATE(updated_at)
OPTIONS(
    description="Stores the last successful sync timestamp for each Ragic sheet, used for incremental backups."
);
