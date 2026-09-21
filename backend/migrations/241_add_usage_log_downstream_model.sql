-- The model name actually emitted to the client, after response transformations.
-- Historical rows remain unknown; never infer them from current settings.
ALTER TABLE usage_logs ADD COLUMN IF NOT EXISTS downstream_model VARCHAR(200);
