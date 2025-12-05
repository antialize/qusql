use anyhow::Result;

use std::{
    borrow::Cow,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::Duration,
};

use qusql_mysql::{ConnectionOptions, Executor, ExecutorExt, Pool, PoolOptions};

const ADDR: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1235);
const OPTS: ConnectionOptions<'static> = ConnectionOptions {
    address: ADDR,
    user: Cow::Borrowed("root"),
    password: Cow::Borrowed("test"),
    database: Cow::Borrowed("test"),
};

struct Timings {
    setup: Duration,
    insert: Duration,
    select_all: Duration,
    select_stream: Duration,
    select_one: Duration,
}

const CNT: i64 = 400000;
const ITR: i64 = 100;

async fn sqly_test() -> Result<Timings> {
    let start = std::time::Instant::now();

    let pool = Pool::connect(
        OPTS,
        PoolOptions {
            max_connections: 2,
            ..Default::default()
        },
    )
    .await?;

    {
        let mut conn = pool.acquire().await?;

        let mut tr = conn.begin().await?;
        tr.execute("DROP TABLE IF EXISTS db_bench_1", ()).await?;
        tr.execute(
            "CREATE TABLE db_bench_1 (
            id BIGINT NOT NULL AUTO_INCREMENT,
            v INT NOT NULL,
            t TEXT NOT NULL,
            PRIMARY KEY (id)
            )",
            (),
        )
        .await?;
        tr.commit().await?;
    }

    let setup_done = std::time::Instant::now();

    {
        let mut conn = pool.acquire().await?;
        let mut tr: qusql_mysql::Transaction<'_> = conn.begin().await?;
        for cnt in 0..CNT {
            tr.execute(
                "INSERT INTO db_bench_1 (v, t) VALUES (?, ?)",
                (cnt, "this is a string that is somewhat long"),
            )
            .await?;
        }
        tr.commit().await?;
    }

    let insert_done = std::time::Instant::now();

    {
        let mut conn = pool.acquire().await?;
        let mut sum = 0;
        for _ in 0..ITR {
            let values: Vec<(i64, i32, &str)> = conn
                .fetch_all("SELECT id, v, t FROM db_bench_1", ())
                .await?;
            for (id, _, _) in values {
                sum += id;
            }
        }
        assert_eq!(sum, CNT * (CNT + 1) / 2 * ITR);
    }

    let select_all_done = std::time::Instant::now();

    {
        let mut conn = pool.acquire().await?;
        let mut sum = 0;
        for _ in 0..ITR {
            let mut iter = conn.fetch("SELECT id, v, t FROM db_bench_1", ()).await?;
            while let Some(row) = iter.next().await? {
                let (id, _, _): (i64, i32, &str) = row.read()?;
                sum += id;
            }
        }
        assert_eq!(sum, CNT * (CNT + 1) / 2 * ITR);
    }
    let select_stream_done = std::time::Instant::now();

    {
        let mut conn = pool.acquire().await?;
        let mut sum = 0;
        for id in 0..CNT {
            let (_, v, _): (i64, i32, &str) = conn
                .fetch_one("SELECT id, v, t FROM db_bench_1 WHERE id=?", (id + 1,))
                .await?;
            sum += v as i64;
        }
        assert_eq!(sum, CNT * (CNT - 1) / 2);
    }
    let select_one_done = std::time::Instant::now();

    {
        let mut conn = pool.acquire().await?;
        conn.execute("DROP TABLE IF EXISTS db_bench_1", ()).await?;
    }

    Ok(Timings {
        setup: setup_done.duration_since(start),
        insert: insert_done.duration_since(setup_done),
        select_all: select_all_done.duration_since(insert_done),
        select_stream: select_stream_done.duration_since(select_all_done),
        select_one: select_one_done.duration_since(select_stream_done),
    })
}

async fn sqlx_test() -> Result<Timings> {
    use futures_util::TryStreamExt;
    use sqlx::{Executor, Row};

    let start = std::time::Instant::now();

    let pool = sqlx::mysql::MySqlPoolOptions::new()
        .max_connections(2)
        .connect_with(
            sqlx::mysql::MySqlConnectOptions::new()
                .database("test")
                .username("root")
                .password("test")
                .port(1235)
                .host("127.0.0.1"),
        )
        .await?;

    {
        let mut tr = pool.begin().await?;
        tr.execute("DROP TABLE IF EXISTS db_bench_1").await?;
        tr.execute(
            "CREATE TABLE db_bench_1 (
            id BIGINT NOT NULL AUTO_INCREMENT,
            v INT NOT NULL,
            t TEXT NOT NULL,
            PRIMARY KEY (id)
            )",
        )
        .await?;
        tr.commit().await?;
    }

    let setup_done = std::time::Instant::now();

    {
        let mut tr = pool.begin().await?;
        for cnt in 0..CNT {
            sqlx::query("INSERT INTO db_bench_1 (v, t) VALUES (?, ?)")
                .bind(cnt)
                .bind("this is a string that is somewhat long")
                .execute(&mut *tr)
                .await?;
        }
        tr.commit().await?;
    }

    let insert_done = std::time::Instant::now();

    {
        let mut sum = 0;
        for _ in 0..ITR {
            let values: Vec<(i64, i32, String)> = sqlx::query("SELECT id, v, t FROM db_bench_1")
                .map(|r: sqlx::mysql::MySqlRow| (r.get(0), r.get(1), r.get(2)))
                .fetch_all(&pool)
                .await?;
            for (id, _, _) in values {
                sum += id;
            }
        }
        assert_eq!(sum, CNT * (CNT + 1) / 2 * ITR);
    }

    let select_all_done = std::time::Instant::now();

    {
        let mut sum = 0;
        for _ in 0..ITR {
            let mut iter = sqlx::query("SELECT id, v, t FROM db_bench_1").fetch(&pool);
            while let Some(row) = iter.try_next().await? {
                let (id, _, _): (i64, i32, &str) = (row.get(0), row.get(1), row.get(2));
                sum += id;
            }
        }
        assert_eq!(sum, CNT * (CNT + 1) / 2 * ITR);
    }
    let select_stream_done = std::time::Instant::now();

    {
        let mut sum = 0;
        for id in 0..CNT {
            let (_, v, _): (i64, i32, String) =
                sqlx::query("SELECT id, v, t FROM db_bench_1 WHERE id=?")
                    .bind(id + 1)
                    .map(|r: sqlx::mysql::MySqlRow| (r.get(0), r.get(1), r.get(2)))
                    .fetch_one(&pool)
                    .await?;
            sum += v as i64;
        }
        assert_eq!(sum, CNT * (CNT - 1) / 2);
    }
    let select_one_done = std::time::Instant::now();

    {
        sqlx::query("DROP TABLE IF EXISTS db_bench_1")
            .execute(&pool)
            .await?;
    }

    Ok(Timings {
        setup: setup_done.duration_since(start),
        insert: insert_done.duration_since(setup_done),
        select_all: select_all_done.duration_since(insert_done),
        select_stream: select_stream_done.duration_since(select_all_done),
        select_one: select_one_done.duration_since(select_stream_done),
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    let sqlx_times = sqlx_test().await?;
    let sqly_times = sqly_test().await?;

    println!("Test              Sqly time     Sqlx time");
    println!("-----------------------------------------");
    println!(
        "Setup         {:10.3} ms {:10.3} ms",
        sqly_times.setup.as_secs_f64() * 1000.0,
        sqlx_times.setup.as_secs_f64() * 1000.0
    );
    println!(
        "Insert        {:10.3} ms {:10.3} ms",
        sqly_times.insert.as_secs_f64() * 1000.0,
        sqlx_times.insert.as_secs_f64() * 1000.0
    );
    println!(
        "Select all    {:10.3} ms {:10.3} ms",
        sqly_times.select_all.as_secs_f64() * 1000.0,
        sqlx_times.select_all.as_secs_f64() * 1000.0
    );
    println!(
        "Select stream {:10.3} ms {:10.3} ms",
        sqly_times.select_stream.as_secs_f64() * 1000.0,
        sqlx_times.select_stream.as_secs_f64() * 1000.0
    );
    println!(
        "Select one    {:10.3} ms {:10.3} ms",
        sqly_times.select_one.as_secs_f64() * 1000.0,
        sqlx_times.select_one.as_secs_f64() * 1000.0
    );

    Ok(())
}
