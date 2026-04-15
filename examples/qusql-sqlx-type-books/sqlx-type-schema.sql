-- -*- mode: sql; indent-tabs-mode: nil; sql-product: postgres -*-
-- Library catalog schema - example for qusql-sqlx-type.
-- Domain: authors, books, loans, and reviews.
--
-- This file is idempotent: running it against a live database skips every
-- revision that has already been applied and leaves existing data untouched.
-- Running it against a fresh, empty database bootstraps the full schema in a
-- single transaction.

BEGIN;

-- ---------------------------------------------------------------------------
-- Migration infrastructure
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS schema_revisions (
    id             integer     PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    name           text        NOT NULL UNIQUE,
    sequence_index integer     NOT NULL UNIQUE CHECK (sequence_index >= 0),
    applied_at     timestamptz NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION apply_revision(
    rev_name    text,
    description text,
    seq_idx     integer,
    command     text
) RETURNS BOOLEAN AS $$
  BEGIN
    IF EXISTS (SELECT 1 FROM schema_revisions WHERE name = rev_name) THEN
      RAISE NOTICE 'Skipping revision "%": %', rev_name, description;
      RETURN FALSE;
    END IF;
    RAISE NOTICE 'Applying revision "%": %', rev_name, description;
    IF COALESCE(seq_idx <= MAX(sequence_index), FALSE) FROM schema_revisions THEN
      RAISE EXCEPTION
        'Out-of-order revision: % (seq %) - last applied was % (seq %)',
        rev_name, seq_idx,
        (SELECT name           FROM schema_revisions ORDER BY sequence_index DESC LIMIT 1),
        (SELECT sequence_index FROM schema_revisions ORDER BY sequence_index DESC LIMIT 1);
    END IF;
    INSERT INTO schema_revisions (name, sequence_index) VALUES (rev_name, seq_idx);
    EXECUTE command;
    RETURN TRUE;
  END
$$ LANGUAGE plpgsql;


-- ---------------------------------------------------------------------------
-- Revision 0 - initial schema: authors, books, loans
-- ---------------------------------------------------------------------------

SELECT apply_revision('init', 'Initial schema: authors, books, loans', 0, $rev$

DO $$ BEGIN
  CREATE TYPE Genre AS ENUM (
    'Fiction',
    'NonFiction',
    'Science',
    'History',
    'Biography',
    'Children'
  );
EXCEPTION
  WHEN duplicate_object THEN RAISE NOTICE 'type "Genre" already exists, skipping';
END $$;

CREATE TABLE IF NOT EXISTS authors (
    id         uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    name       text        NOT NULL,
    email      text        NOT NULL UNIQUE,
    bio        text,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS books (
    id           integer PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    author_id    uuid    NOT NULL REFERENCES authors ON DELETE RESTRICT ON UPDATE CASCADE,
    title        text    NOT NULL,
    isbn         text    NOT NULL UNIQUE,
    published_on date    NOT NULL,
    genre        Genre   NOT NULL,
    total_copies integer NOT NULL DEFAULT 1 CHECK (total_copies >= 0)
);

CREATE INDEX IF NOT EXISTS books_author_idx ON books (author_id);
CREATE INDEX IF NOT EXISTS books_genre_idx  ON books (genre);

-- A loan tracks a single copy of a book checked out to a borrower.
-- returned_at is NULL while the copy is still on loan.
CREATE TABLE IF NOT EXISTS loans (
    id            integer     PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    book_id       integer     NOT NULL REFERENCES books ON DELETE RESTRICT ON UPDATE CASCADE,
    borrower_name text        NOT NULL,
    borrowed_at   timestamptz NOT NULL DEFAULT now(),
    due_date      date        NOT NULL,
    returned_at   timestamptz
);

CREATE INDEX IF NOT EXISTS loans_book_idx   ON loans (book_id);
CREATE INDEX IF NOT EXISTS loans_active_idx ON loans (book_id) WHERE returned_at IS NULL;

$rev$);


-- ---------------------------------------------------------------------------
-- Revision 1 - add reviews
-- ---------------------------------------------------------------------------

SELECT apply_revision('add_reviews', 'Add book reviews table', 1, $rev$

CREATE TABLE IF NOT EXISTS reviews (
    id            integer     PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    book_id       integer     NOT NULL REFERENCES books ON DELETE CASCADE ON UPDATE CASCADE,
    reviewer_name text        NOT NULL,
    -- 1 = poor ... 5 = excellent
    rating        integer     NOT NULL CHECK (rating BETWEEN 1 AND 5),
    body          text,
    reviewed_at   timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS reviews_book_idx ON reviews (book_id);

$rev$);


COMMIT;
