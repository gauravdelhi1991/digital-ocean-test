CREATE TABLE job_outbox (
    job_id UUID PRIMARY KEY REFERENCES jobs (id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    claimed_at TIMESTAMPTZ,
    published_at TIMESTAMPTZ,
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    last_error TEXT
);

CREATE INDEX job_outbox_pending_idx
    ON job_outbox (available_at, created_at)
    WHERE published_at IS NULL;
