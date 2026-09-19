ALTER TABLE accounts ADD COLUMN IF NOT EXISTS scheduling_paused_at TIMESTAMPTZ;

-- Historical pause times are unknown. Start their continuous-pause clock now.
UPDATE accounts SET scheduling_paused_at = NOW()
WHERE schedulable IS FALSE AND scheduling_paused_at IS NULL AND deleted_at IS NULL;

CREATE OR REPLACE FUNCTION track_account_scheduling_pause() RETURNS TRIGGER AS $$
BEGIN
    IF NEW.schedulable IS FALSE THEN
        IF TG_OP = 'INSERT' THEN
            NEW.scheduling_paused_at := clock_timestamp();
        ELSIF OLD.schedulable IS DISTINCT FROM FALSE OR OLD.scheduling_paused_at IS NULL THEN
            NEW.scheduling_paused_at := clock_timestamp();
        ELSE
            NEW.scheduling_paused_at := OLD.scheduling_paused_at;
        END IF;
    ELSE
        NEW.scheduling_paused_at := NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS accounts_scheduling_pause ON accounts;
CREATE TRIGGER accounts_scheduling_pause
BEFORE INSERT OR UPDATE OF schedulable, scheduling_paused_at ON accounts
FOR EACH ROW EXECUTE FUNCTION track_account_scheduling_pause();

CREATE INDEX IF NOT EXISTS idx_accounts_scheduling_paused
ON accounts (id) WHERE schedulable IS FALSE AND deleted_at IS NULL;
