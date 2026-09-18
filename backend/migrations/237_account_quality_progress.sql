-- Two bounded runtime snapshots, shared by all service instances.
CREATE TABLE IF NOT EXISTS account_quality_batches (
    kind SMALLINT PRIMARY KEY CHECK (kind IN (0,1)),
    payload JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Restore the place of accounts whose first check was postponed by the other detector.
UPDATE account_quality_states SET question_next_at='epoch'::timestamptz
WHERE payload->'question'->>'checked_at' IS NULL;
UPDATE account_quality_states SET model_next_at='epoch'::timestamptz
WHERE payload->'model'->>'checked_at' IS NULL;
