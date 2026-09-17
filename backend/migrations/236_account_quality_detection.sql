CREATE TABLE IF NOT EXISTS account_quality_states (
    account_id BIGINT PRIMARY KEY REFERENCES accounts(id) ON DELETE CASCADE,
    revision TEXT NOT NULL,
    version TEXT NOT NULL,
    next_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    question_next_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    model_next_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    payload JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_account_quality_due ON account_quality_states(next_at, account_id);
CREATE INDEX IF NOT EXISTS idx_account_quality_question_due ON account_quality_states(question_next_at, account_id);
CREATE INDEX IF NOT EXISTS idx_account_quality_model_due ON account_quality_states(model_next_at, account_id);
CREATE TABLE IF NOT EXISTS account_quality_history (
    id BIGSERIAL PRIMARY KEY,
    account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    payload JSONB NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_account_quality_history ON account_quality_history(account_id, id DESC);
