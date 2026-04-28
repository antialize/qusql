-- sql-product: postgres
-- Schema for qusql-sqlx-type-test: exercises integer and other type mappings.
CREATE TABLE IF NOT EXISTS type_test_items (
    id          integer     PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    big_id      bigint      NOT NULL DEFAULT 0,
    small_id    smallint    NOT NULL DEFAULT 0,
    name        text        NOT NULL DEFAULT '',
    score       integer     NOT NULL DEFAULT 0,
    active      boolean     NOT NULL DEFAULT true,
    ratio       float8      NOT NULL DEFAULT 0.0
);

CREATE SEQUENCE IF NOT EXISTS type_test_seq;
