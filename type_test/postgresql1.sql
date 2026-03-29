-- -*- mode: sql; indent-tabs-mode: nil; sql-product: postgres -*-
-- Test schema exercising PostgreSQL features.
-- Domain: Team project / task board management.

-- This file MUST be idempotent and applying it SHOULD NOT destroy existing data.

BEGIN;

CREATE TABLE IF NOT EXISTS schema_revisions (
  id integer PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
  name text NOT NULL UNIQUE,
  sequence_index integer NOT NULL UNIQUE CHECK (sequence_index >= 0),
  applied_at timestamptz NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION apply_revision(rev_name text, description text, seq_idx integer, command text)
RETURNS BOOLEAN AS $$
  BEGIN
    IF EXISTS (SELECT 1 FROM schema_revisions WHERE name = rev_name) THEN
      RAISE NOTICE 'Skipping revision `%`: %', rev_name, description;
      RETURN FALSE;
    END IF;
    RAISE NOTICE 'Applying revision `%`: %', rev_name, description;
    IF COALESCE(seq_idx <= MAX(sequence_index), FALSE) FROM schema_revisions THEN
      RAISE EXCEPTION 'Revision has sequence index %, but a revision with index % has already been applied at %: %',
        seq_idx,
        (SELECT MAX(sequence_index) FROM schema_revisions),
        (SELECT applied_at FROM schema_revisions ORDER BY sequence_index DESC LIMIT 1),
        (SELECT name FROM schema_revisions ORDER BY sequence_index DESC LIMIT 1);
    END IF;
    INSERT INTO schema_revisions (name, sequence_index) VALUES (rev_name, seq_idx);
    EXECUTE command;
    RETURN TRUE;
  END
$$ LANGUAGE plpgsql;


SELECT apply_revision('init', 'Initial setup', 0, $a$
DO $$ BEGIN
  CREATE TYPE TaskStatus AS ENUM (
    'Pending',
    'Active',
    'OnHold',
    'Resolved',
    'Archived',
    'Cancelled',
    'Review',
    'Reopened',
    'Draft',
    'Published'
  );
EXCEPTION
  WHEN duplicate_object THEN RAISE NOTICE 'type "TaskStatus" already exists, skipping';
END $$;

CREATE TABLE IF NOT EXISTS boards (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  created_at timestamptz NOT NULL DEFAULT now(),
  last_modified timestamptz NOT NULL DEFAULT now(),
  -- `change_count` tracks the height of the undo stack for this board.
  change_count integer NOT NULL DEFAULT 0 CHECK (change_count >= 0),
  is_open boolean NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS tasks (
  id integer PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
  board_id UUID NOT NULL REFERENCES boards ON DELETE CASCADE ON UPDATE CASCADE,
  sort_order integer NOT NULL,
  status TaskStatus NOT NULL,
  title text NOT NULL,
  extra jsonb NOT NULL,
  created_by text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  sys_period tstzrange NOT NULL DEFAULT tstzrange(now(), null),
  -- `change_index` is the board-wide undo stack height at the time this
  -- version of the row was created.
  change_index integer NOT NULL CHECK (change_index >= 0),
  is_active boolean NOT NULL DEFAULT TRUE,
  -- `coord_x` / `coord_y` store the item's position in the board canvas.
  coord_x double precision NOT NULL,
  coord_y double precision NOT NULL,
  -- Simplified coordinates rounded to 4 decimal places for cheaper
  -- spatial queries at lower zoom levels.
  coord_x_4dp double precision NOT NULL GENERATED ALWAYS AS (round(coord_x::numeric, 4)::double precision) STORED
);

CREATE INDEX IF NOT EXISTS tasks_board_id_idx ON tasks USING hash (board_id);

DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'tasks' AND column_name = 'coord_x') THEN
    CREATE INDEX IF NOT EXISTS tasks_coords_idx ON tasks (board_id, coord_x, coord_y);
  END IF;
END $$;

CREATE OR REPLACE FUNCTION versioning() RETURNS trigger AS $$
  DECLARE
    history_table regclass := TG_ARGV[0];
    old_sys_period tstzrange := tstzrange(lower(OLD.sys_period), now());
    new_sys_period tstzrange := tstzrange(now(), null);
  BEGIN
    OLD.sys_period = old_sys_period;
    IF (TG_OP = 'UPDATE') THEN
      NEW.sys_period = new_sys_period;
    END IF;
    EXECUTE format('INSERT INTO %I VALUES ($1.*);', history_table)
      USING OLD;
    RETURN COALESCE(NEW, OLD);
  END
$$ LANGUAGE plpgsql;

CREATE TABLE IF NOT EXISTS tasks_history (
  LIKE tasks,
  UNIQUE (id, sys_period)
);

CREATE INDEX IF NOT EXISTS tasks_undo_lookup_idx ON tasks (
  board_id,
  change_index
);

CREATE INDEX IF NOT EXISTS tasks_history_undo_lookup_idx ON tasks_history (
  board_id,
  change_index,
  upper(sys_period),
  id
);

CREATE OR REPLACE TRIGGER history_trigger
  BEFORE UPDATE OR DELETE ON tasks
  FOR EACH ROW
  EXECUTE FUNCTION versioning('tasks_history');

CREATE OR REPLACE FUNCTION update_last_modified() RETURNS trigger AS $$
  BEGIN
    NEW.last_modified = now();
    RETURN COALESCE(NEW, OLD);
  END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER boards_last_modified
  BEFORE UPDATE ON boards
  FOR EACH ROW
  EXECUTE FUNCTION update_last_modified();

$a$);


-- Add a tag-definition store
SELECT apply_revision('tag_store', 'Add tag definition storage table', 1, $a$

CREATE TABLE IF NOT EXISTS tag_definitions (
  id integer PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
  created_at timestamptz NOT NULL DEFAULT now(),
  last_modified timestamptz NOT NULL DEFAULT now(),
  name text NOT NULL,
  UNIQUE (name)
);
CREATE INDEX IF NOT EXISTS tag_definitions_name_idx ON tag_definitions USING hash (name);

CREATE OR REPLACE TRIGGER tag_definitions_last_modified
  BEFORE UPDATE ON tag_definitions
  FOR EACH ROW
  EXECUTE FUNCTION update_last_modified();

$a$);


-- Restructure tasks to use a single properties column
SELECT apply_revision('properties_column', 'Consolidate task fields into a properties jsonb column', 2, $a$

DROP TRIGGER IF EXISTS history_trigger ON tasks;

CREATE OR REPLACE FUNCTION versioning() RETURNS trigger AS $$
  DECLARE
    live_table regclass := TG_ARGV[0];
    history_table regclass := TG_ARGV[1];
    old_sys_period tstzrange := tstzrange(lower(OLD.sys_period), now());
    new_sys_period tstzrange := tstzrange(now(), null);
    -- Compute the column names for `OLD` as `tasks` and `tasks_history` may
    -- have different column orders due to subsequent ALTER TABLE statements.
    old_column_order text[] := array_agg(col) FROM (
      SELECT quote_ident(column_name) AS col FROM information_schema.columns
        WHERE table_name = live_table::text ORDER BY ordinal_position
    ) AS cols;
    query_string text := format('INSERT INTO %I (%s) VALUES ($1.*);', history_table, array_to_string(old_column_order, ','));
  BEGIN
    OLD.sys_period = old_sys_period;
    IF (TG_OP = 'UPDATE') THEN
      NEW.sys_period = new_sys_period;
    END IF;
    EXECUTE query_string USING OLD;
    RETURN COALESCE(NEW, OLD);
  END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER history_trigger
  BEFORE UPDATE OR DELETE ON tasks
  FOR EACH ROW
  EXECUTE FUNCTION versioning('tasks', 'tasks_history');

ALTER TABLE ONLY tasks
  ADD COLUMN IF NOT EXISTS properties jsonb;

DO $$ BEGIN
  ALTER TABLE ONLY tasks
    ALTER COLUMN properties SET DATA TYPE jsonb USING json_build_object(
      'title', title,
      'extra', extra,
      'sort_order', sort_order,
      'created_by', created_by,
      'status', status
    ),
    ALTER COLUMN properties SET NOT NULL;
EXCEPTION
  WHEN undefined_column THEN RAISE NOTICE 'dependent columns have already been dropped, skipping initializing "properties"';
END $$;

ALTER TABLE ONLY tasks
  DROP COLUMN IF EXISTS title,
  DROP COLUMN IF EXISTS extra,
  DROP COLUMN IF EXISTS sort_order,
  DROP COLUMN IF EXISTS created_by,
  DROP COLUMN IF EXISTS status;

ALTER TABLE ONLY tasks
  ADD COLUMN IF NOT EXISTS sort_order integer NOT NULL GENERATED ALWAYS AS ((properties ->> 'sort_order')::integer) STORED,
  ADD COLUMN IF NOT EXISTS created_by text NOT NULL GENERATED ALWAYS AS ((properties ->> 'created_by')) STORED,
  ADD COLUMN IF NOT EXISTS status text NOT NULL GENERATED ALWAYS AS ((properties ->> 'status')) STORED;

ALTER TABLE ONLY tasks_history
  ADD COLUMN IF NOT EXISTS properties jsonb;

DO $$ BEGIN
  ALTER TABLE ONLY tasks_history
    ALTER COLUMN properties SET DATA TYPE jsonb USING json_build_object(
      'title', title,
      'extra', extra,
      'sort_order', sort_order,
      'created_by', created_by,
      'status', status
    );
EXCEPTION
  WHEN undefined_column THEN RAISE NOTICE 'dependent columns have already been dropped, skipping initializing "properties"';
END $$;

ALTER TABLE ONLY tasks_history
  DROP COLUMN IF EXISTS title,
  DROP COLUMN IF EXISTS extra,
  ALTER COLUMN status TYPE text USING status::text;

$a$);


-- Add board-wide properties record
SELECT apply_revision('board_properties', 'Add board-wide properties record', 3, $a$

DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'tasks' AND column_name = 'coord_x') THEN
    INSERT INTO tasks (board_id, coord_x, coord_y, properties, change_index)
    SELECT
      boards.id,
      0.0,
      0.0,
      '{
        "status": "__BoardProperties__",
        "created_by": "system",
        "sort_order": 0,
        "boardProperties": {
          "boardType": "Standard"
        }
      }'::jsonb,
      0
    FROM boards
    WHERE NOT EXISTS (
      SELECT 1 FROM tasks
      WHERE tasks.status = '__BoardProperties__'
      AND boards.id = tasks.board_id
    );
  END IF;
END $$;

$a$);


-- Add NOT NULL constraint to tasks_history.properties
SELECT apply_revision('history_properties_notnull', 'Add missing NOT NULL on tasks_history.properties', 4, $a$

ALTER TABLE ONLY tasks_history
  ALTER COLUMN properties SET NOT NULL;

$a$);


-- Move coordinates into their own table
SELECT apply_revision('positions_table', 'Move coordinate data into a dedicated positions table', 5, $a$

CREATE TABLE IF NOT EXISTS positions (
  id integer PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
  board_id UUID NOT NULL REFERENCES boards ON DELETE CASCADE ON UPDATE CASCADE,
  coord_x double precision NOT NULL,
  coord_y double precision NOT NULL,
  coord_x_4dp double precision NOT NULL GENERATED ALWAYS AS (round(coord_x::numeric, 4)::double precision) STORED
);

CREATE INDEX IF NOT EXISTS positions_ids_idx ON positions (board_id, id);

DO $$ BEGIN
  IF
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'tasks' AND column_name = 'coord_x')
    AND COUNT(*) = 0 FROM positions
  THEN
    ALTER TABLE ONLY tasks
      ADD COLUMN IF NOT EXISTS position_id integer NULL REFERENCES positions,
      ADD COLUMN IF NOT EXISTS bounding_box box NULL;
    ALTER TABLE ONLY tasks_history
      ADD COLUMN IF NOT EXISTS position_id integer NULL,
      ADD COLUMN IF NOT EXISTS bounding_box box NULL;

    INSERT INTO positions (board_id, coord_x, coord_y)
    SELECT DISTINCT t.board_id, t.coord_x, t.coord_y
    FROM tasks AS t
    UNION
    SELECT DISTINCT th.board_id, th.coord_x, th.coord_y
    FROM tasks_history AS th;

    UPDATE tasks
    SET
      position_id = positions.id,
      bounding_box = box(point(tasks.coord_x, tasks.coord_y), point(tasks.coord_x, tasks.coord_y))
    FROM positions
    WHERE tasks.board_id = positions.board_id
    AND tasks.coord_x = positions.coord_x
    AND tasks.coord_y = positions.coord_y;
    UPDATE tasks
    SET bounding_box = box(point(0, 0), point(0, 0))
    WHERE tasks.status = '__BoardProperties__';

    UPDATE tasks_history
    SET
      position_id = positions.id,
      bounding_box = box(point(tasks_history.coord_x, tasks_history.coord_y), point(tasks_history.coord_x, tasks_history.coord_y))
    FROM positions
    WHERE tasks_history.board_id = positions.board_id
    AND tasks_history.coord_x = positions.coord_x
    AND tasks_history.coord_y = positions.coord_y;
    UPDATE tasks_history
    SET bounding_box = box(point(0, 0), point(0, 0))
    WHERE tasks_history.status = '__BoardProperties__';

    ALTER TABLE ONLY tasks
      ALTER COLUMN position_id SET NOT NULL,
      ALTER COLUMN bounding_box SET NOT NULL,
      DROP COLUMN IF EXISTS coord_x_4dp,
      DROP COLUMN IF EXISTS coord_x,
      DROP COLUMN IF EXISTS coord_y;
    ALTER TABLE ONLY tasks_history
      ALTER COLUMN position_id SET NOT NULL,
      ALTER COLUMN bounding_box SET NOT NULL,
      DROP COLUMN IF EXISTS coord_x_4dp,
      DROP COLUMN IF EXISTS coord_x,
      DROP COLUMN IF EXISTS coord_y;

    CREATE INDEX IF NOT EXISTS tasks_bounding_box_idx ON tasks (board_id, bounding_box);
  END IF;
END $$;

$a$);


-- Add indices for task filtering
SELECT apply_revision('filter_indices', 'Add indices for task filtering', 6, $a$

CREATE INDEX tasks_status_filter_idx ON tasks (board_id, is_active, status);
CREATE INDEX tasks_id_filter_idx ON tasks (board_id, is_active, id);

$a$);


-- Support undo barriers via min_change_count
SELECT apply_revision('add_min_change_count', 'Support undo barriers via min_change_count', 7, $a$

ALTER TABLE ONLY boards
  ADD COLUMN min_change_count integer NOT NULL DEFAULT 0,
  ADD CONSTRAINT change_min_check CHECK (change_count >= min_change_count);

$a$);


-- Normalize property field names to camelCase
SELECT apply_revision('camelcase_fields', 'Normalize property field names to camelCase', 8, $a$

-- Temporarily disable history_trigger while bulk-updating tasks
ALTER TABLE ONLY tasks
  DISABLE TRIGGER history_trigger;

-- Add new camelCase fields alongside the old snake_case ones
UPDATE tasks
SET properties = jsonb_build_object(
  'sortOrder', sort_order,
  'createdBy', created_by,
  'taskStatus', status
) || properties;

UPDATE tasks_history
SET properties = jsonb_build_object(
  'sortOrder', sort_order,
  'createdBy', created_by,
  'taskStatus', status
) || properties;

-- Re-create generated columns under camelCase keys (drop-and-add required)

DROP INDEX tasks_status_filter_idx;

ALTER TABLE ONLY tasks
  DROP COLUMN sort_order,
  DROP COLUMN created_by,
  DROP COLUMN status;
ALTER TABLE ONLY tasks
  ADD COLUMN sort_order integer NOT NULL GENERATED ALWAYS AS ((properties ->> 'sortOrder')::integer) STORED,
  ADD COLUMN created_by text NOT NULL GENERATED ALWAYS AS ((properties ->> 'createdBy')) STORED,
  ADD COLUMN status text NOT NULL GENERATED ALWAYS AS ((properties ->> 'taskStatus')) STORED;

CREATE INDEX tasks_status_filter_idx ON tasks (board_id, is_active, status);

-- Remove old snake_case fields from properties
UPDATE tasks
SET properties = properties - 'sort_order' - 'created_by' - 'status';

UPDATE tasks_history
SET properties = properties - 'sort_order' - 'created_by' - 'status';

-- Fix casing in nested board-properties record
UPDATE tasks
SET properties = jsonb_set(properties, '{boardProperties, boardType}', properties #> '{board_properties, board_type}') #- '{board_properties, board_type}'
WHERE status = '__BoardProperties__'
AND properties #> '{boardProperties, boardType}' IS NULL;

UPDATE tasks_history
SET properties = jsonb_set(properties, '{boardProperties, boardType}', properties #> '{board_properties, board_type}') #- '{board_properties, board_type}'
WHERE status = '__BoardProperties__'
AND properties #> '{boardProperties, boardType}' IS NULL;

ALTER TABLE ONLY tasks
  ENABLE TRIGGER history_trigger;

$a$);


-- Remove the deprecated TaskStatus enum and add GC indices
SELECT apply_revision('cleanup_enum_and_gc_indices', 'Remove unused TaskStatus enum, add GC indices', 9, $a$

-- TaskStatus is no longer used; the status column is now a plain text
-- generated column backed by properties jsonb.
DROP TYPE TaskStatus;

-- Foreign-key columns on tasks benefit from indices when checking FK
-- constraints during DELETE on the referenced table (positions).
CREATE INDEX tasks_position_id_idx ON tasks (position_id);
CREATE INDEX tasks_history_position_id_idx ON tasks_history (position_id);

-- Restrict history deletes: a position row must not be removed while
-- reachable undo history still refers to it.
ALTER TABLE ONLY tasks_history
  ADD CONSTRAINT tasks_history_position_id_fkey FOREIGN KEY (position_id) REFERENCES positions (id) ON UPDATE CASCADE;

-- Hard-deletes during GC should not fire the history trigger.
CREATE OR REPLACE TRIGGER history_trigger
  BEFORE UPDATE ON tasks
  FOR EACH ROW
  EXECUTE FUNCTION versioning('tasks', 'tasks_history');

$a$);


-- Add binary snapshot table
SELECT apply_revision('snapshots_table', 'Add snapshots table for binary blobs', 10, $a$

CREATE TABLE IF NOT EXISTS snapshots (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  content bytea NOT NULL,
  recorded_at timestamptz NOT NULL DEFAULT now()
);

$a$);


-- Store a derived display_kind column on tasks
SELECT apply_revision('display_kind_column', 'Store display_kind derived from properties', 12, $a$

ALTER TABLE ONLY tasks
  DISABLE TRIGGER history_trigger;

ALTER TABLE ONLY tasks
  ADD COLUMN display_kind text NULL;
ALTER TABLE ONLY tasks_history
  ADD COLUMN display_kind text NULL;
COMMENT ON COLUMN tasks.display_kind IS 'Cached display kind from properties. Must be kept in sync manually with the matching positions row.';

UPDATE tasks
SET display_kind = properties ->> 'displayKind';
UPDATE tasks_history
SET display_kind = properties ->> 'displayKind';

CREATE INDEX tasks_display_kind_filter_idx ON tasks (board_id, is_active, display_kind);

ALTER TABLE ONLY tasks
  ALTER COLUMN display_kind SET NOT NULL;
ALTER TABLE ONLY tasks_history
  ALTER COLUMN display_kind SET NOT NULL;

ALTER TABLE ONLY tasks
  ENABLE TRIGGER history_trigger;

$a$);


-- Support per-board coordinate systems
SELECT apply_revision('coordinate_systems', 'Add per-board coordinate system support', 13, $a$

ALTER TABLE ONLY tasks
  DISABLE TRIGGER history_trigger;

CREATE TABLE coordinate_systems (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  board_id UUID NOT NULL REFERENCES boards ON DELETE CASCADE ON UPDATE CASCADE,
  -- position_id is only used during this migration and will be dropped before commit
  position_id integer NOT NULL REFERENCES positions,
  origin_x double precision NOT NULL,
  origin_y double precision NOT NULL,
  scale double precision NOT NULL DEFAULT 1.0
);
COMMENT ON COLUMN coordinate_systems.scale IS 'Scale factor relative to the base unit. Set by the client at row-creation time. See also: unit_to_scale().';

CREATE INDEX coordinate_systems_ids_idx ON coordinate_systems (board_id, id);
CREATE INDEX coordinate_systems_position_id_idx ON coordinate_systems (position_id);

ALTER TABLE boards
  ADD COLUMN epsg_code integer NOT NULL DEFAULT 4326;
ALTER TABLE boards
  ALTER COLUMN epsg_code DROP DEFAULT;

INSERT INTO coordinate_systems (board_id, position_id, origin_x, origin_y, scale)
SELECT positions.board_id,
  positions.id,
  positions.coord_x,
  positions.coord_y,
  1.0
FROM positions;

ALTER TABLE tasks
  ADD COLUMN coord_system_id uuid NULL;
ALTER TABLE tasks_history
  ADD COLUMN coord_system_id uuid NULL;

UPDATE tasks
SET coord_system_id = coordinate_systems.id
FROM coordinate_systems
WHERE coordinate_systems.position_id = tasks.position_id;
UPDATE tasks_history
SET coord_system_id = coordinate_systems.id
FROM coordinate_systems
WHERE coordinate_systems.position_id = tasks_history.position_id;

DROP INDEX tasks_bounding_box_idx;
DROP INDEX tasks_position_id_idx;
DROP INDEX tasks_history_position_id_idx;

ALTER TABLE tasks
  ALTER COLUMN coord_system_id SET NOT NULL,
  DROP COLUMN position_id,
  ADD FOREIGN KEY (coord_system_id) REFERENCES coordinate_systems,
  ALTER COLUMN bounding_box SET DATA TYPE box USING bounding_box;
ALTER TABLE tasks_history
  ALTER COLUMN coord_system_id SET NOT NULL,
  DROP COLUMN position_id,
  ADD FOREIGN KEY (coord_system_id) REFERENCES coordinate_systems,
  ALTER COLUMN bounding_box SET DATA TYPE box USING bounding_box;

CREATE INDEX tasks_bounding_box_idx ON tasks (board_id, is_active, bounding_box);
CREATE INDEX tasks_coord_system_id_idx ON tasks (coord_system_id);
CREATE INDEX tasks_history_coord_system_id_idx ON tasks_history (coord_system_id);

DROP INDEX coordinate_systems_position_id_idx;
ALTER TABLE coordinate_systems
  DROP COLUMN position_id;

ALTER TABLE ONLY tasks
  ENABLE TRIGGER history_trigger;

DROP TABLE positions;

-- Returns approximately how many native units equal one metre for the
-- given EPSG code. Used by clients when computing simplification tolerances.
CREATE FUNCTION unit_to_metre(epsg integer) RETURNS double precision AS $$
DECLARE
  proj_info text;
  uses_metres bool;
  to_metre double precision;
BEGIN
  -- In a full deployment this would query the spatial_ref_sys catalogue table.
  proj_info := '';
  uses_metres := proj_info LIKE '%+units=m' OR proj_info LIKE '%+units=m %';
  to_metre := (regexp_match(proj_info, '(\+to_meter=)([^ ]+)'))[2];
  IF uses_metres THEN
    RETURN 1.0;
  ELSIF to_metre IS NOT NULL THEN
    RETURN 1.0 / to_metre;
  ELSE
    RETURN 1.0;
  END IF;
END;
$$ STABLE STRICT PARALLEL SAFE LANGUAGE plpgsql;

CREATE FUNCTION apply_scale(val double precision, scale_factor double precision DEFAULT 1.0) RETURNS double precision AS $$
DECLARE
  result double precision;
BEGIN
  IF scale_factor IS NULL OR scale_factor = 0.0 THEN
    RAISE EXCEPTION 'scale_factor must be non-null and non-zero, got: %', scale_factor;
  END IF;
  result := val / scale_factor;
  IF result IS NULL THEN
    RAISE WARNING 'apply_scale returned NULL for val=%, scale_factor=%', val, scale_factor;
    BEGIN
      RETURN val;
    EXCEPTION
      WHEN numeric_value_out_of_range THEN
        RAISE WARNING 'Numeric overflow in apply_scale fallback, returning 0';
        RETURN 0.0;
    END;
  END IF;
  RETURN result;
END;
$$ STABLE STRICT PARALLEL SAFE LANGUAGE plpgsql;

$a$);


COMMIT;
