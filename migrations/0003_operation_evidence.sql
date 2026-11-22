-- Operation evidence and uniform retention.
--
-- The command path becomes `operation`, the low-cardinality name aggregates
-- already use. The collection policy is removed from both record tables: a
-- record exists only when collection was permitted. Historical context
-- evidence is folded into the retained shapes before obsolete kinds are
-- dropped.

ALTER TABLE events RENAME COLUMN command TO operation;
ALTER TABLE events DROP COLUMN collection_policy;
ALTER TABLE reports DROP COLUMN collection_policy;

-- Invocation timeline ordered by client-observed time, then stable identity.
CREATE INDEX IF NOT EXISTS idx_events_occurred_at_id
    ON events (occurred_at DESC, id DESC);

-- Report/event traversal is scoped by installation and invocation.
CREATE INDEX IF NOT EXISTS idx_events_installation_invocation
    ON events (installation_id, invocation_id);
CREATE INDEX IF NOT EXISTS idx_reports_installation_invocation
    ON reports (installation_id, invocation_id);

-- Move the cumulative prompt wait onto the execution snapshot. The target and
-- source are validated with JSON functions so malformed or legacy rows are
-- skipped instead of failing the migration.
UPDATE context_snapshots
SET data = json_set(
        context_snapshots.data,
        '$.input_wait_ms',
        (
            SELECT json_extract(prompt.data, '$.wait_ms')
            FROM context_snapshots AS prompt
            WHERE prompt.event_id = context_snapshots.event_id
              AND prompt.context_type = 'prompt'
              AND prompt.status = 'present'
              AND CASE
                      WHEN json_valid(prompt.data)
                       AND json_type(prompt.data) = 'object'
                       AND json_type(prompt.data, '$.count') = 'integer'
                       AND json_extract(prompt.data, '$.count') > 0
                       AND json_type(prompt.data, '$.wait_ms') = 'integer'
                       AND json_extract(prompt.data, '$.wait_ms') BETWEEN 0 AND 604800000
                      THEN 1
                      ELSE 0
                  END
            ORDER BY prompt.ordinal ASC
            LIMIT 1
        )
    )
WHERE context_snapshots.context_type = 'execution'
  AND context_snapshots.status = 'present'
  AND CASE
          WHEN json_valid(context_snapshots.data)
           AND json_type(context_snapshots.data) = 'object'
          THEN json_type(context_snapshots.data, '$.input_wait_ms') IS NULL
          ELSE 0
      END
  AND EXISTS (
      SELECT 1
      FROM context_snapshots AS prompt
      WHERE prompt.event_id = context_snapshots.event_id
        AND prompt.context_type = 'prompt'
        AND prompt.status = 'present'
        AND CASE
                WHEN json_valid(prompt.data)
                 AND json_type(prompt.data) = 'object'
                 AND json_type(prompt.data, '$.count') = 'integer'
                 AND json_extract(prompt.data, '$.count') > 0
                 AND json_type(prompt.data, '$.wait_ms') = 'integer'
                 AND json_extract(prompt.data, '$.wait_ms') BETWEEN 0 AND 604800000
                THEN 1
                ELSE 0
            END
  );

-- Scale moved its file count onto execution.
UPDATE context_snapshots
SET data = json_set(
        context_snapshots.data,
        '$.files',
        (
            SELECT json_extract(scale.data, '$.files')
            FROM context_snapshots AS scale
            WHERE scale.event_id = context_snapshots.event_id
              AND scale.context_type = 'scale'
              AND scale.status = 'present'
              AND CASE
                      WHEN json_valid(scale.data)
                       AND json_type(scale.data) = 'object'
                       AND json_type(scale.data, '$.files') = 'integer'
                       AND json_extract(scale.data, '$.files') BETWEEN 0 AND 1000000
                      THEN 1
                      ELSE 0
                  END
            ORDER BY scale.ordinal ASC
            LIMIT 1
        )
    )
WHERE context_snapshots.context_type = 'execution'
  AND context_snapshots.status = 'present'
  AND CASE
          WHEN json_valid(context_snapshots.data)
           AND json_type(context_snapshots.data) = 'object'
          THEN json_type(context_snapshots.data, '$.files') IS NULL
          ELSE 0
      END
  AND EXISTS (
      SELECT 1
      FROM context_snapshots AS scale
      WHERE scale.event_id = context_snapshots.event_id
        AND scale.context_type = 'scale'
        AND scale.status = 'present'
        AND CASE
                WHEN json_valid(scale.data)
                 AND json_type(scale.data) = 'object'
                 AND json_type(scale.data, '$.files') = 'integer'
                 AND json_extract(scale.data, '$.files') BETWEEN 0 AND 1000000
                THEN 1
                ELSE 0
            END
  );

-- Scale moved its package count onto workspace.
UPDATE context_snapshots
SET data = json_set(
        context_snapshots.data,
        '$.packages',
        (
            SELECT json_extract(scale.data, '$.packages')
            FROM context_snapshots AS scale
            WHERE scale.event_id = context_snapshots.event_id
              AND scale.context_type = 'scale'
              AND scale.status = 'present'
              AND CASE
                      WHEN json_valid(scale.data)
                       AND json_type(scale.data) = 'object'
                       AND json_type(scale.data, '$.packages') = 'integer'
                       AND json_extract(scale.data, '$.packages') BETWEEN 0 AND 1000000
                      THEN 1
                      ELSE 0
                  END
            ORDER BY scale.ordinal ASC
            LIMIT 1
        )
    )
WHERE context_snapshots.context_type = 'workspace'
  AND context_snapshots.status = 'present'
  AND CASE
          WHEN json_valid(context_snapshots.data)
           AND json_type(context_snapshots.data) = 'object'
          THEN json_type(context_snapshots.data, '$.packages') IS NULL
          ELSE 0
      END
  AND EXISTS (
      SELECT 1
      FROM context_snapshots AS scale
      WHERE scale.event_id = context_snapshots.event_id
        AND scale.context_type = 'scale'
        AND scale.status = 'present'
        AND CASE
                WHEN json_valid(scale.data)
                 AND json_type(scale.data) = 'object'
                 AND json_type(scale.data, '$.packages') = 'integer'
                 AND json_extract(scale.data, '$.packages') BETWEEN 0 AND 1000000
                THEN 1
                ELSE 0
            END
  );

-- An absent workspace snapshot is upgraded when package evidence exists. The
-- base object keeps only the migrated count; failed snapshots stay failed and
-- present snapshots keep their runtime fields.
UPDATE context_snapshots
SET status = 'present',
    reason = NULL,
    data = json_set(
        '{}',
        '$.packages',
        (
            SELECT json_extract(scale.data, '$.packages')
            FROM context_snapshots AS scale
            WHERE scale.event_id = context_snapshots.event_id
              AND scale.context_type = 'scale'
              AND scale.status = 'present'
              AND CASE
                      WHEN json_valid(scale.data)
                       AND json_type(scale.data) = 'object'
                       AND json_type(scale.data, '$.packages') = 'integer'
                       AND json_extract(scale.data, '$.packages') BETWEEN 0 AND 1000000
                      THEN 1
                      ELSE 0
                  END
            ORDER BY scale.ordinal ASC
            LIMIT 1
        )
    )
WHERE context_snapshots.context_type = 'workspace'
  AND context_snapshots.status = 'absent'
  AND context_snapshots.data IS NULL
  AND EXISTS (
      SELECT 1
      FROM context_snapshots AS scale
      WHERE scale.event_id = context_snapshots.event_id
        AND scale.context_type = 'scale'
        AND scale.status = 'present'
        AND CASE
                WHEN json_valid(scale.data)
                 AND json_type(scale.data) = 'object'
                 AND json_type(scale.data, '$.packages') = 'integer'
                 AND json_extract(scale.data, '$.packages') BETWEEN 0 AND 1000000
                THEN 1
                ELSE 0
            END
  );

-- Values derived from parsed arguments or duration are removed from retained
-- snapshots. Historical invocation flags keep their evidence as `arguments`;
-- a positional count becomes bounded `arg-N` text slots. The array is capped
-- at 32 entries and malformed legacy shapes are skipped.
UPDATE context_snapshots
SET data = CASE
        WHEN CASE
                 WHEN json_type(context_snapshots.data, '$.positionals') = 'integer'
                  AND json_extract(context_snapshots.data, '$.positionals') > 0
                 THEN 1
                 ELSE 0
             END
          OR COALESCE(json_array_length(context_snapshots.data, '$.flags'), 0) > 0
        THEN json_set(
                 json_remove(
                     context_snapshots.data,
                     '$.output',
                     '$.offline',
                     '$.flags',
                     '$.positionals'
                 ),
                 '$.arguments',
                 json((
                     SELECT json_group_array(
                                json_object('name', name, 'kind', kind)
                                ORDER BY seq, name
                            )
                     FROM (
                         SELECT flag.key AS seq,
                                json_extract(flag.value, '$.name') AS name,
                                json_extract(flag.value, '$.kind') AS kind
                         FROM json_each(
                                  CASE
                                      WHEN json_type(context_snapshots.data, '$.flags') = 'array'
                                      THEN json_extract(context_snapshots.data, '$.flags')
                                      ELSE '[]'
                                  END
                              ) AS flag
                         WHERE CASE
                                   WHEN flag.type = 'object'
                                    AND json_type(flag.value, '$.name') = 'text'
                                    AND json_type(flag.value, '$.kind') = 'text'
                                    AND length(json_extract(flag.value, '$.name')) BETWEEN 1 AND 64
                                    AND json_extract(flag.value, '$.kind') IN
                                        ('boolean', 'number', 'selector', 'version', 'text', 'list')
                                   THEN 1
                                   ELSE 0
                               END
                           AND flag.key < 32
                         UNION ALL
                         SELECT 32 + slot.n,
                                'arg-' || slot.n,
                                'text'
                         FROM (
                             WITH RECURSIVE slots(n) AS (
                                 SELECT 1
                                 UNION ALL
                                 SELECT n + 1 FROM slots WHERE n < 32
                             )
                             SELECT n FROM slots
                         ) AS slot
                         WHERE CASE
                                   WHEN json_type(context_snapshots.data, '$.positionals') = 'integer'
                                    AND json_extract(context_snapshots.data, '$.positionals') > 0
                                    AND slot.n <= json_extract(context_snapshots.data, '$.positionals')
                                    AND slot.n <= 32 - COALESCE(
                                        json_array_length(context_snapshots.data, '$.flags'),
                                        0
                                    )
                                   THEN 1
                                   ELSE 0
                               END
                     )
                 ))
             )
        ELSE json_remove(
                 context_snapshots.data,
                 '$.output',
                 '$.offline',
                 '$.flags',
                 '$.positionals'
             )
    END
WHERE context_snapshots.context_type = 'invocation'
  AND context_snapshots.status = 'present'
  AND CASE
          WHEN json_valid(context_snapshots.data)
           AND json_type(context_snapshots.data) = 'object'
          THEN 1
          ELSE 0
      END;

UPDATE context_snapshots
SET data = json_remove(data, '$.slow')
WHERE context_type = 'execution'
  AND status = 'present'
  AND CASE
          WHEN json_valid(data) AND json_type(data) = 'object'
          THEN 1
          ELSE 0
      END;

-- These contexts were removed from the wire: their useful evidence now lives
-- in the retained invocation/execution/workspace snapshots.
DELETE FROM context_snapshots
WHERE context_type IN ('prompt', 'scale', 'warnings', 'collection', 'transport');
