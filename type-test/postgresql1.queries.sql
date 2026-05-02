-- Example queries for postgresql1 schema (task board, PostgreSQL dialect).
-- Arguments use $N placeholders.

-- query: get_board
SELECT id, created_at, last_modified, change_count, is_open
FROM boards
WHERE id = $1;

-- query: insert_board
INSERT INTO boards (change_count, is_open, epsg_code)
VALUES ($1, $2, $3)
RETURNING id, created_at;

-- query: update_board_change_count
UPDATE boards
SET change_count = $1
WHERE id = $2;

-- query: delete_board
DELETE FROM boards
WHERE id = $1;

-- query: list_tasks_for_board
SELECT id, board_id, properties, change_index, is_active, sort_order, created_by, status
FROM tasks
WHERE board_id = $1 AND is_active = $2;

-- query: insert_task
INSERT INTO tasks (board_id, properties, change_index, display_kind, coord_system_id)
VALUES ($1, $2, $3, $4, $5)
RETURNING id, created_at;

-- query: update_task_properties
UPDATE tasks
SET properties = $1
WHERE id = $2 AND board_id = $3;

-- query: get_tag_by_name
SELECT id, name, last_modified
FROM tag_definitions
WHERE name = $1;

-- query: insert_tag_definition
INSERT INTO tag_definitions (name)
VALUES ($1)
RETURNING id, created_at;

-- query: list_schema_revisions
SELECT id, name, sequence_index, applied_at
FROM schema_revisions
ORDER BY sequence_index;

-- query: get_schema_revision_by_name
SELECT id, name, sequence_index, applied_at
FROM schema_revisions
WHERE name = $1;
