CREATE TABLE job_dead_letters (
    job_id UUID PRIMARY KEY REFERENCES jobs (id),
    payload JSONB NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    attempt_count INTEGER NOT NULL CHECK (attempt_count > 0),
    error TEXT NOT NULL,
    failed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX job_dead_letters_failed_at_idx
    ON job_dead_letters (failed_at);
