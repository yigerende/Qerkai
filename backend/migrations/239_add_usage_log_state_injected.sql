-- Historical and uninstrumented rows retain an unknown injection status.
ALTER TABLE usage_logs
    ADD COLUMN IF NOT EXISTS state_injected BOOLEAN;
