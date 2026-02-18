-- 007_schema_additions.sql
-- Adds timeout column to jobs, queue column to cron entries,
-- and max_retries column to DLQ entries.

-- Job timeout stored as nanoseconds (int64). 0 means no timeout.
ALTER TABLE dispatch_jobs ADD COLUMN IF NOT EXISTS timeout BIGINT NOT NULL DEFAULT 0;

-- Cron entry queue override. Empty string means use default queue.
ALTER TABLE dispatch_cron_entries ADD COLUMN IF NOT EXISTS queue TEXT NOT NULL DEFAULT '';

-- DLQ max_retries preserves the original job's retry budget for replay.
ALTER TABLE dispatch_dlq ADD COLUMN IF NOT EXISTS max_retries INT NOT NULL DEFAULT 3;
