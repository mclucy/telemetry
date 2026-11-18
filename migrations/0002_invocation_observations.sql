-- Invocation observation contexts and intake pressure counters.
-- Additive only: existing rows keep NULL version/reason and zero counters.

ALTER TABLE context_snapshots ADD COLUMN version INTEGER;
ALTER TABLE context_snapshots ADD COLUMN reason TEXT;

-- Denied delivery attempts per hourly bucket, not unique records. Existing
-- intake_budget TTL cleanup removes these rows with their bucket.
ALTER TABLE intake_budget ADD COLUMN limited_events INTEGER NOT NULL DEFAULT 0
    CHECK (limited_events >= 0);
ALTER TABLE intake_budget ADD COLUMN limited_reports INTEGER NOT NULL DEFAULT 0
    CHECK (limited_reports >= 0);
