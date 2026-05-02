-- Example queries for postgresql2 schema (media archives, PostgreSQL dialect).
-- Arguments use $N placeholders.

-- query: get_media_archive
SELECT id, path, status, creation_time, total_blob_bytes, config
FROM media_archives
WHERE id = $1;

-- query: insert_media_archive
INSERT INTO media_archives (path, status, total_blob_bytes)
VALUES ($1, $2, $3)
RETURNING id, creation_time;

-- query: update_media_archive_status
UPDATE media_archives
SET status = $1
WHERE id = $2;

-- query: get_blob_by_id
SELECT id, hash_a, hash_b, stored_bytes, tier, complete, creation_time
FROM blobs
WHERE id = $1;

-- query: insert_blob
INSERT INTO blobs (hash_a, hash_b, stored_bytes, tier, complete)
VALUES ($1, $2, $3, $4, $5)
RETURNING id, creation_time;

-- query: list_blobs_for_archive
SELECT b.id, b.stored_bytes, b.tier, b.complete, ab.metadata
FROM blobs b
JOIN archive_blobs ab ON ab.blob_id = b.id
WHERE ab.archive_id = $1;

-- query: list_pending_local_files
SELECT id, dir, filename, size, upload_status
FROM local_files
WHERE upload_status != $1 AND NOT deleted;

-- query: insert_render_queue
INSERT INTO render_queue (path)
VALUES ($1)
RETURNING id;

-- query: get_track_by_identifier
SELECT id, provider_id, path, identifier, content_type, title, license_type, size, upload_status, deleted
FROM tracks
WHERE identifier = $1;

-- query: insert_processing_job
INSERT INTO processing_jobs (hash_a, hash_b)
VALUES ($1, $2)
RETURNING id, last_used;
