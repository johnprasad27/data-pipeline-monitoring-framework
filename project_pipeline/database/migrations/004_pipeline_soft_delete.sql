-- =====================================================================
-- Migration 004: Pipeline Soft Delete and Tombstone Support
-- Purpose: Additive, idempotent column for tracking pipeline deletion
--          and preventing resurrection from static configurations.
-- =====================================================================

ALTER TABLE pipeline_registry
    ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP DEFAULT NULL;

CREATE INDEX IF NOT EXISTS idx_pipeline_registry_deleted_at
    ON pipeline_registry(deleted_at);
