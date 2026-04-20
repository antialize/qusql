-- -*- mode: sql; indent-tabs-mode: nil; sql-product: postgres -*-
-- Notes schema for the qusql-sqlx-type-notes example.
--
-- The schema is intentionally minimal: a single CREATE TABLE IF NOT EXISTS
-- statement is all that is needed to bootstrap an empty database.
--
-- For a more complete migration pattern that tracks applied revisions and
-- supports incremental schema evolution without touching existing data, see
-- examples/qusql-sqlx-type-books/sqlx-type-schema.sql.

CREATE TABLE IF NOT EXISTS notes (
    id         integer     PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    title      text        NOT NULL,
    body       text,
    pinned     boolean     NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT now()
);
