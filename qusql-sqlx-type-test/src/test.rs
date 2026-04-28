use qusql_sqlx_type::query;
use sqlx::PgPool;

async fn setup(pool: &PgPool) {
    let schema = include_str!("../sqlx-type-schema.sql");
    sqlx::raw_sql(schema)
        .execute(pool)
        .await
        .expect("schema setup failed");
    sqlx::query("DELETE FROM type_test_items")
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("SELECT setval('type_test_seq', 1, false)")
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO type_test_items (big_id, small_id, name, score, active, ratio) \
         VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(100i64)
    .bind(3i16)
    .bind("alpha")
    .bind(42i32)
    .bind(true)
    .bind(1.5f64)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO type_test_items (big_id, small_id, name, score, active, ratio) \
         VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(200i64)
    .bind(7i16)
    .bind("beta")
    .bind(10i32)
    .bind(false)
    .bind(2.5f64)
    .execute(pool)
    .await
    .unwrap();
}

/// Verify that columns declared as `integer` in the schema come back as `i32`.
#[sqlx::test]
async fn test_integer_column_is_i32(pool: PgPool) {
    setup(&pool).await;
    let row = query!("SELECT id, score FROM type_test_items ORDER BY id LIMIT 1")
        .fetch_one(&pool)
        .await
        .unwrap();
    let _: i32 = row.id;
    let _: i32 = row.score;
}

/// Verify that `bigint` columns come back as `i64`.
#[sqlx::test]
async fn test_bigint_column_is_i64(pool: PgPool) {
    setup(&pool).await;
    let row = query!("SELECT big_id FROM type_test_items ORDER BY id LIMIT 1")
        .fetch_one(&pool)
        .await
        .unwrap();
    let _: i64 = row.big_id;
}

/// Verify that `smallint` columns come back as `i16`.
#[sqlx::test]
async fn test_smallint_column_is_i16(pool: PgPool) {
    setup(&pool).await;
    let row = query!("SELECT small_id FROM type_test_items ORDER BY id LIMIT 1")
        .fetch_one(&pool)
        .await
        .unwrap();
    let _: i16 = row.small_id;
}

/// `count(*)` must return `i64` (PostgreSQL bigint).
#[sqlx::test]
async fn test_count_is_i64(pool: PgPool) {
    setup(&pool).await;
    let row = query!("SELECT count(*) AS n FROM type_test_items")
        .fetch_one(&pool)
        .await
        .unwrap();
    let n: i64 = row.n;
    assert_eq!(n, 2);
}

/// `count(*) + 4`: integer literal 4 is INT4 in PG, but count() is INT8, so
/// the result is INT8 (i64).
#[sqlx::test]
async fn test_count_plus_literal_is_i64(pool: PgPool) {
    setup(&pool).await;
    let row = query!("SELECT count(*) + 4 AS n FROM type_test_items")
        .fetch_one(&pool)
        .await
        .unwrap();
    let n: i64 = row.n;
    assert_eq!(n, 6);
}

/// Integer literal `4` is INT4 in PostgreSQL.
#[sqlx::test]
async fn test_integer_literal_is_i32(pool: PgPool) {
    setup(&pool).await;
    let row = query!("SELECT 4::integer AS n")
        .fetch_one(&pool)
        .await
        .unwrap();
    let _: i32 = row.n;
}

/// `inet_server_port()` returns INT4 in PostgreSQL — must decode as `i32`.
#[sqlx::test]
async fn test_inet_server_port_is_i32(pool: PgPool) {
    let row = query!("SELECT inet_server_port() AS port")
        .fetch_one(&pool)
        .await
        .unwrap();
    // Port is nullable (NULL when connected via Unix socket).
    let _: Option<i32> = row.port;
}

/// `pg_backend_pid()` returns INT4 (nullable per the qusql type system).
#[sqlx::test]
async fn test_pg_backend_pid_is_i32(pool: PgPool) {
    let row = query!("SELECT pg_backend_pid() AS pid")
        .fetch_one(&pool)
        .await
        .unwrap();
    let pid: Option<i32> = row.pid;
    assert!(pid.unwrap() > 0);
}

/// `nextval()` / `lastval()` return INT8 (i64).
#[sqlx::test]
async fn test_sequence_functions_are_i64(pool: PgPool) {
    setup(&pool).await;
    let row = query!("SELECT nextval('type_test_seq') AS v")
        .fetch_one(&pool)
        .await
        .unwrap();
    let v: i64 = row.v;
    assert_eq!(v, 1);

    let row2 = query!("SELECT lastval() AS v")
        .fetch_one(&pool)
        .await
        .unwrap();
    let _: Option<i64> = row2.v;
}

/// `character_length()` returns INT4 (i32) in PostgreSQL.
#[sqlx::test]
async fn test_character_length_is_i32(pool: PgPool) {
    setup(&pool).await;
    let row =
        query!("SELECT character_length(name) AS len FROM type_test_items ORDER BY id LIMIT 1")
            .fetch_one(&pool)
            .await
            .unwrap();
    let len: i32 = row.len;
    assert_eq!(len, 5); // "alpha"
}

/// `score + 1`: INT4 + INT4 literal → stays INT4 (i32).
#[sqlx::test]
async fn test_int4_arithmetic_stays_i32(pool: PgPool) {
    setup(&pool).await;
    let row = query!("SELECT score + 1 AS v FROM type_test_items ORDER BY id LIMIT 1")
        .fetch_one(&pool)
        .await
        .unwrap();
    let _: i32 = row.v;
}
