-- -*- mode: sql; indent-tabs-mode: nil; sql-product: postgres -*-

-- This file MUST be idempotent and applying it SHOULD NOT destroy existing data.

BEGIN;
CREATE TABLE IF NOT EXISTS worker_checkpoint (
    payload bytea NOT NULL
);
ALTER TABLE worker_checkpoint ADD COLUMN IF NOT EXISTS chunk bigint NOT NULL DEFAULT 0;
ALTER TABLE worker_checkpoint ADD COLUMN IF NOT EXISTS final boolean NOT NULL DEFAULT true;
COMMIT;

BEGIN;
CREATE TABLE IF NOT EXISTS media_archives (
    id bigint NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    path text,
    status smallint NOT NULL,
    creation_time timestamptz NOT NULL DEFAULT now(),
    previous_id bigint,
    manifest bytea DEFAULT NULL,
    total_blob_bytes bigint NOT NULL,
    CONSTRAINT fk_media_archives__previous
      FOREIGN KEY(previous_id)
      REFERENCES media_archives(id)
      ON DELETE SET NULL
);
CREATE INDEX IF NOT EXISTS idx_media_archives__path ON media_archives(path);
DROP INDEX IF EXISTS idx_media_archives__path;
CREATE UNIQUE INDEX IF NOT EXISTS idx_media_archives__path2 ON media_archives(path text_pattern_ops);
ALTER TABLE media_archives ADD COLUMN IF NOT EXISTS config text DEFAULT NULL;
ALTER TABLE media_archives ADD COLUMN IF NOT EXISTS head_blob_id bigint DEFAULT NULL;
COMMIT;

BEGIN;
CREATE TABLE IF NOT EXISTS archive_updates (
    id bigint NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    path text NOT NULL,
    update_time timestamptz NOT NULL DEFAULT now()
);
COMMIT;

BEGIN;
CREATE TABLE IF NOT EXISTS storage_tiers (
    id smallint NOT NULL UNIQUE,
    description text NOT NULL
);
COMMIT;

BEGIN;
CREATE TABLE IF NOT EXISTS blobs (
    id bigint NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    hash_a bigint,
    hash_b bigint,
    stored_bytes bigint NOT NULL,
    tier smallint NOT NULL,
    complete boolean NOT NULL,
    creation_time timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_blobs__hash ON blobs(hash_a, hash_b) WHERE complete;
CREATE INDEX IF NOT EXISTS idx_blobs__not_complete_creation_time ON blobs(creation_time) WHERE NOT complete;
ALTER TABLE blobs
    ALTER COLUMN hash_a DROP NOT NULL,
    ALTER COLUMN hash_b DROP NOT NULL;
COMMIT;


BEGIN;
CREATE TABLE IF NOT EXISTS blobs_delete (
    id bigint NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    blob_id bigint NOT NULL,
    hash_a bigint,
    hash_b bigint,
    stored_bytes bigint NOT NULL,
    tier smallint NOT NULL
);
ALTER TABLE blobs_delete
    ALTER COLUMN hash_a DROP NOT NULL,
    ALTER COLUMN hash_b DROP NOT NULL;
COMMIT;

BEGIN;
CREATE TABLE IF NOT EXISTS archive_blobs (
    archive_id bigint NOT NULL,
    blob_id bigint NOT NULL,
    metadata bytea NOT NULL,
    CONSTRAINT fk_archive_blobs__blob
        FOREIGN KEY(blob_id)
        REFERENCES blobs(id)
        ON DELETE RESTRICT,
    CONSTRAINT fk_archive_blobs__archive
        FOREIGN KEY(archive_id)
        REFERENCES media_archives(id)
        ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_archive_blobs__archive_id ON archive_blobs(archive_id);
DROP INDEX IF EXISTS idx_archive_blobs__blob_id;
CREATE UNIQUE INDEX IF NOT EXISTS idx_archive_blobs__blob_id2 ON archive_blobs(blob_id, archive_id, metadata);
COMMIT;

BEGIN;
CREATE TABLE IF NOT EXISTS processing_jobs (
    id bigint NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    hash_a bigint NOT NULL,
    hash_b bigint NOT NULL,
    last_used timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_processing_jobs__hash ON processing_jobs(hash_a, hash_b);
CREATE INDEX IF NOT EXISTS idx_processing_jobs__last_used ON processing_jobs(last_used);
COMMIT;

BEGIN;
CREATE TABLE IF NOT EXISTS job_blobs (
    job_id bigint NOT NULL,
    blob_id bigint NOT NULL,
    direction smallint NOT NULL,
    metadata bytea NOT NULL,
    CONSTRAINT fk_job_blobs__blob
        FOREIGN KEY(blob_id)
        REFERENCES blobs(id)
        ON DELETE RESTRICT,
    CONSTRAINT fk_job_blobs__job
        FOREIGN KEY(job_id)
        REFERENCES processing_jobs(id)
        ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_job_blobs__job_id ON job_blobs(job_id);
CREATE INDEX IF NOT EXISTS idx_job_blobs__blob_id ON job_blobs(blob_id);
COMMIT;


BEGIN;
CREATE TABLE IF NOT EXISTS local_files (
    id bigint NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    dir text NOT NULL,
    filename text,
    size bigint NOT NULL,
    upload_status smallint DEFAULT 0 NOT NULL,
    deleted boolean NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_local_files__dir ON local_files(dir);
CREATE INDEX IF NOT EXISTS idx_local_files__upload_status ON local_files (upload_status) WHERE upload_status != 2;
COMMIT;

BEGIN;
CREATE TABLE IF NOT EXISTS ingest_queue (
    id bigint NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    dir text NOT NULL
);
COMMIT;

BEGIN;
CREATE TABLE IF NOT EXISTS render_queue (
    id bigint NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    path text NOT NULL
);
ALTER TABLE render_queue ADD COLUMN IF NOT EXISTS inprogress boolean DEFAULT false;
DROP INDEX IF EXISTS render_queue_path2;
CREATE INDEX IF NOT EXISTS render_queue_path ON render_queue (path);
COMMIT;

BEGIN;
CREATE TABLE IF NOT EXISTS tracks (
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    provider_id int,
    path text NOT NULL,
    identifier text NOT NULL,
    content_type text NOT NULL,
    title text NOT NULL,
    license_type int NOT NULL,
    manifest_ctime_nsec bigint NOT NULL,
    file_mtime_nsec bigint NOT NULL,
    source_file_name text,
    size bigint NOT NULL,
    upload_status smallint DEFAULT 0 NOT NULL,
    deleted boolean DEFAULT false NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_tracks_identifier ON tracks (identifier);
DROP INDEX IF EXISTS idx_tracks_path;
CREATE UNIQUE INDEX IF NOT EXISTS idx_tracks_path2 ON tracks (path text_pattern_ops);
CREATE INDEX IF NOT EXISTS idx_tracks_provider_id ON tracks (provider_id);
CREATE INDEX IF NOT EXISTS idx_tracks_upload_status ON tracks (upload_status) WHERE upload_status != 2;
COMMIT;

BEGIN;
-- This table contains supplemental tracks derived from the captions extension.
-- For the delivery backend and frontend they behave like any other track but
-- internally they are backed by the caption data of the track referenced by track_id.
CREATE TABLE IF NOT EXISTS caption_tracks (
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    track_id int NOT NULL,
    identifier text NOT NULL,
    content_type text NOT NULL,
    upload_status smallint DEFAULT 0 NOT NULL,
    deleted boolean DEFAULT false NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_caption_tracks_identifier ON caption_tracks (identifier);
CREATE INDEX IF NOT EXISTS idx_caption_tracks_track_id ON caption_tracks (track_id);
CREATE INDEX IF NOT EXISTS idx_caption_tracks_upload_status ON caption_tracks (upload_status) WHERE upload_status != 2;
COMMIT;

BEGIN;
CREATE TABLE IF NOT EXISTS prediction_models (
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    module text NOT NULL,
    schema_version bigint NOT NULL,
    parameters text NOT NULL,
    ctime bigint NOT NULL,
    model bytea NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS prediction_models_key ON prediction_models (module, schema_version, parameters);
COMMIT;

BEGIN;

CREATE OR REPLACE FUNCTION fn_ingest_queue_trigger() RETURNS trigger AS $psql$
BEGIN
  PERFORM pg_notify('ingest_queue', NULL);
  RETURN new;
END;$psql$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER ingest_queue_trigger
    AFTER INSERT ON ingest_queue
    EXECUTE FUNCTION fn_ingest_queue_trigger();

GRANT INSERT ON ingest_queue TO ingestworker;

GRANT SELECT ON media_archives TO ingestworker;

COMMIT;


BEGIN;

CREATE OR REPLACE FUNCTION fn_render_queue_trigger() RETURNS trigger AS $psql$
BEGIN
  PERFORM pg_notify('render_queue', NULL);
  RETURN new;
END;$psql$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER render_queue_trigger
    AFTER INSERT ON render_queue
    EXECUTE FUNCTION fn_render_queue_trigger();

GRANT INSERT ON render_queue TO ingestworker;

COMMIT;
