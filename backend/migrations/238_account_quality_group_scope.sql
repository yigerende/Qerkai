-- Manual selections get a one-shot exception to the scheduled group scope.
ALTER TABLE account_quality_states
    ADD COLUMN IF NOT EXISTS manual_revision TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS question_requested_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS model_requested_at TIMESTAMPTZ;
