-- NULL preserves unknown counts for historical rows and uninstrumented paths.
ALTER TABLE usage_logs
    ADD COLUMN IF NOT EXISTS openai_upstream_5xx_retry_count INTEGER;
