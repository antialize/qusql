///! To use this db test framework start a mysql db as follows:
///! podman volume create --ignore --opt type=tmpfs --opt device=tmpfs dbstore
///! podman run --replace --name test_db --rm -e MYSQL_ROOT_PASSWORD=test -e MYSQL_DATABASE=test \
///!     --network=host -v dbstore:/var/lib/mysql  docker.io/mariadb:10.5 \
///!     --port 1235 --innodb-flush-method=nosync --innodb-buffer-pool-size=200M
use std::{fmt::Debug, time::Duration};

use qusql_mysql::{
    connection::{Connection, ConnectionErrorContent, ConnectionOptions, Executor, ExecutorExt},
    plain_types::{Bit, Date, DateTime, Decimal, Json, Time, Timestamp, Year},
    pool::{Pool, PoolOptions},
};

fn opts() -> ConnectionOptions<'static> {
    ConnectionOptions::new()
        .address("127.0.0.1:1235")
        .unwrap()
        .user("root")
        .password("test")
        .database("test")
}

struct Error(Box<dyn std::error::Error + Send>);

impl Debug for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.0, f)
    }
}

impl<E: std::error::Error + 'static + Send> From<E> for Error {
    fn from(value: E) -> Self {
        Error(Box::new(value))
    }
}

#[tokio::test]
async fn test_connection() -> Result<(), Error> {
    let mut conn =
        tokio::time::timeout(Duration::from_secs(2), Connection::connect(&opts())).await??;

    // Create a test database with all handled types
    let r = conn
        .query("DROP TABLE IF EXISTS db_test")
        .await?
        .execute()
        .await?;
    assert_eq!(r.affected_rows(), 0);
    assert_eq!(r.last_insert_id(), 0);

    let r = conn
        .query(
            "CREATE TABLE db_test (
        id BIGINT NOT NULL AUTO_INCREMENT,
        u8n TINYINT UNSIGNED NOT NULL DEFAULT 0,
        u8 TINYINT UNSIGNED DEFAULT 0,
        i8n TINYINT NOT NULL DEFAULT 0,
        i8 TINYINT DEFAULT 0,
        u16n SMALLINT UNSIGNED NOT NULL DEFAULT 0,
        u16 SMALLINT UNSIGNED DEFAULT 0,
        i16n SMALLINT NOT NULL DEFAULT 0,
        i16 SMALLINT DEFAULT 0,
        u32n INT UNSIGNED NOT NULL DEFAULT 0,
        u32 INT UNSIGNED DEFAULT 0,
        i32n INT NOT NULL DEFAULT 0,
        i32 INT DEFAULT 0,
        u64n BIGINT UNSIGNED NOT NULL DEFAULT 0,
        u64 BIGINT UNSIGNED DEFAULT 0,
        i64n BIGINT NOT NULL DEFAULT 0,
        i64 BIGINT DEFAULT 0,
        bn TINYINT(1) UNSIGNED NOT NULL DEFAULT false,
        b TINYINT(1) UNSIGNED DEFAULT false,
        f32n FLOAT NOT NULL DEFAULT 0,
        f32 FLOAT DEFAULT 0,
        f64n DOUBLE NOT NULL DEFAULT 0,
        f64 DOUBLE DEFAULT 0,
        cn VARCHAR(10) NOT NULL DEFAULT '',
        c VARCHAR(10) DEFAULT '',
        tn TEXT NOT NULL DEFAULT '',
        t TEXT DEFAULT '',
        bln BLOB NOT NULL DEFAULT '',
        bl BLOB DEFAULT '',
        dn DECIMAL(20,5) NOT NULL DEFAULT 0,
        d DECIMAL(20,5) DEFAULT 0,
        bin BIT(4) NOT NULL DEFAULT 0,
        bi BIT(4) DEFAULT 0,
        jn JSON NOT NULL DEFAULT '{}',
        j JSON DEFAULT '{}',
        dan DATE NOT NULL DEFAULT '2025-01-01',
        da DATE DEFAULT '2025-01-01',
        yn YEAR NOT NULL DEFAULT '2025',
        y YEAR DEFAULT '2025',
        tin TIME NOT NULL DEFAULT '0:00:00',
        ti TIME DEFAULT '0:00:00',
        dtn DATETIME NOT NULL DEFAULT '2025-01-01 0:00:00',
        dt DATETIME DEFAULT '2025-01-01 0:00:00',
        tsn TIMESTAMP NOT NULL DEFAULT '2025-01-01 0:00:00',
        ts TIMESTAMP DEFAULT '2025-01-01 0:00:00',
        ban BINARY(3) NOT NULL DEFAULT '',
        ba BINARY(3) DEFAULT '',
        vban VARBINARY(3) NOT NULL DEFAULT '',
        vba VARBINARY(3) DEFAULT '',
        en ENUM('a', 'b', 'c') NOT NULL DEFAULT 'a',
        e ENUM('a', 'b', 'c') DEFAULT 'a',
        sn SET('a', 'b', 'c') NOT NULL DEFAULT '',
        s SET('a', 'b', 'c') DEFAULT '',
        PRIMARY KEY (id)
        )",
        )
        .await?
        .execute()
        .await?;
    assert_eq!(r.affected_rows(), 0);
    assert_eq!(r.last_insert_id(), 0);

    // Check that we can bind parameters of all types
    let r = conn
        .query(
            "INSERT INTO `db_test` (
        `u8n`, `u8`, `i8n`, `i8`,
        `u16n`, `u16`, `i16n`, `i16`,
        `u32n`, `u32`, `i32n`, `i32`,
        `u64n`, `u64`, `i64n`, `i64`,
        `bn`, `b`, `f32n`, `f32`, `f64n`, `f64`,
        `cn`, `c`, `tn`, `t`, `bln`, `bl`,
        `dn`, `d`, `bin`, `bi`, `jn`, `j`,
        `dan`, `da`, `yn`, `y`, `tin`,`ti`,
        `dtn`, `dt`, `tsn`, `ts`,
        `ban`, `ba`, `vban`, `vba`,
        `en`, `e`, `sn`, `s`
        )
        VALUES (
        ?, ?, ?, ?,
        ?, ?, ?, ?,
        ?, ?, ?, ?,
        ?, ?, ?, ?,
        ?, ?, ?, ?, ?, ?,
        ?, ?, ?, ?, ?, ?,
        ?, ?, ?, ?, ?, ?,
        ?, ?, ?, ?, ?, ?,
        ?, ?, ?, ?,
        ?, ?, ?, ?,
        ?, ?, ?, ?
        )",
        )
        .await?;

    const U8_VALUE: u8 = u8::MAX - 7;
    const I8_VALUE: i8 = i8::MIN + 7;
    const U16_VALUE: u16 = u16::MAX - 7;
    const I16_VALUE: i16 = i16::MIN + 7;
    const U32_VALUE: u32 = u32::MAX - 7;
    const I32_VALUE: i32 = i32::MIN + 7;
    const U64_VALUE: u64 = u64::MAX - 7;
    const I64_VALUE: i64 = i64::MIN + 7;
    const BOOL_VALUE: bool = true;
    const F32_VALUE: f32 = f32::MAX;
    const F64_VALUE: f64 = f64::MIN;
    const V_VALUE: &'static str = "VVALUE";
    const T_VALUE: &'static str = "TVALUE";
    const B_VALUE: &'static [u8] = b"BVALUE";
    const D_VALUE: &'static Decimal = &Decimal::new("12445.11234");
    const BI_VALUE: &'static Bit = Bit::new(b"\x05");
    const J_VALUE: &'static Json = Json::new("{\"Hello\": \"world\"}");
    const DA_VALUE: Date = Date {
        year: 1984,
        month: 2,
        day: 2,
    };
    const Y_VALUE: Year = Year::new(1984);
    const TI_VALUE: Time = Time {
        positive: true,
        days: 2,
        hours: 1,
        minutes: 2,
        seconds: 44,
        microseconds: 0,
    };
    const DT_VALUE: DateTime = DateTime {
        year: 1984,
        month: 2,
        day: 2,
        hour: 1,
        minute: 2,
        second: 3,
        msec: 0,
    };
    const TS_VALUE: Timestamp = Timestamp {
        year: 1984,
        month: 2,
        day: 2,
        hour: 1,
        minute: 2,
        second: 3,
        msec: 0,
    };
    const BA_VALUE: &'static [u8] = b"HAT";
    const E_VALUE: &'static str = "b";
    const S_VALUE: &'static str = "b,c";

    let r = r
        // `u8n`, `u8`, `i8n`, `i8`,
        .bind(&U8_VALUE)?
        .bind(&Option::<u8>::None)?
        .bind(&I8_VALUE)?
        .bind(&Option::<i8>::None)?
        // `u16n`, `u16`, `i16n`, `i16`,
        .bind(&U16_VALUE)?
        .bind(&Option::<u16>::None)?
        .bind(&I16_VALUE)?
        .bind(&Option::<i16>::None)?
        // `u32n`, `u32`, `i32n`, `i32`,
        .bind(&U32_VALUE)?
        .bind(&Option::<u32>::None)?
        .bind(&I32_VALUE)?
        .bind(&Option::<i32>::None)?
        // `u64n`, `u64`, `i64n`, `i64`,
        .bind(&U64_VALUE)?
        .bind(&Option::<u64>::None)?
        .bind(&I64_VALUE)?
        .bind(&Option::<i64>::None)?
        // `bn`, `b`, `f32n`, `f32`, `f64n`, `f64`,
        .bind(&BOOL_VALUE)?
        .bind(&Option::<bool>::None)?
        .bind(&F32_VALUE)?
        .bind(&Option::<f32>::None)?
        .bind(&F64_VALUE)?
        .bind(&Option::<f64>::None)?
        // `cn`, `c`, `tn`, `t`, `bln`, `bl`,
        .bind(&V_VALUE)?
        .bind(&Option::<&str>::None)?
        .bind(&T_VALUE)?
        .bind(&Option::<&str>::None)?
        .bind(&B_VALUE)?
        .bind(&Option::<&[u8]>::None)?
        // `dn`, `d`, `bin`, `bi`, `jn`, `j`,
        .bind(&D_VALUE)?
        .bind(&Option::<&Decimal>::None)?
        .bind(&BI_VALUE)?
        .bind(&Option::<&[u8]>::None)?
        .bind(&J_VALUE)?
        .bind(&Option::<&Json>::None)?
        // `dan`, `da`, `yn`, `y`, `tin`,`ti`,
        .bind(&DA_VALUE)?
        .bind(&Option::<Date>::None)?
        .bind(&Y_VALUE)?
        .bind(&Option::<Year>::None)?
        .bind(&TI_VALUE)?
        .bind(&Option::<Time>::None)?
        // `dtn`, `dt`, `tsn`, `ts`,
        .bind(&DT_VALUE)?
        .bind(&Option::<DateTime>::None)?
        .bind(&TS_VALUE)?
        .bind(&Option::<Timestamp>::None)?
        // `ban`, `ba`, `vban`, `vba`,
        .bind(&BA_VALUE)?
        .bind(&Option::<&[u8]>::None)?
        .bind(&BA_VALUE)?
        .bind(&Option::<&[u8]>::None)?
        // `en`, `e`, `sn`, `s`,
        .bind(&E_VALUE)?
        .bind(&Option::<&str>::None)?
        .bind(&S_VALUE)?
        .bind(&Option::<&str>::None)?
        .execute()
        .await?;

    assert_eq!(r.last_insert_id(), 1);
    assert_eq!(r.affected_rows(), 1);

    // Check that we inserted values correctly and that we can decode them
    let mut rows = conn
        .query(
            "SELECT id,
            u8n, u8, i8n, i8,
            u16n, u16, i16n, i16,
            u32n, u32, i32n, i32,
            u64n, u64, i64n, i64,
            `bn`, `b`, `f32n`, `f32`, `f64n`, `f64`,
            `cn`, `c`, `tn`, `t`, `bln`, `bl`,
            `dn`, `d`, `bin`, `bi`, `jn`, `j`,
            `dan`, `da`, `yn`, `y`, `tin`,`ti`,
            `dtn`, `dt`, `tsn`, `ts`,
            `ban`, `ba`, `vban`, `vba`,
            `en`, `e`, `sn`, `s`
            FROM db_test",
        )
        .await?
        .fetch()
        .await?;
    while let Some(row) = rows.next().await? {
        let mut p = row.parse();

        assert_eq!(p.next::<i64>()?, 1);

        // u8n, u8, i8n, i8,
        assert_eq!(p.next::<u8>()?, U8_VALUE);
        assert_eq!(p.next::<Option<u8>>()?, None);
        assert_eq!(p.next::<i8>()?, I8_VALUE);
        assert_eq!(p.next::<Option<i8>>()?, None);

        // u16n, u16, i16n, i16,
        assert_eq!(p.next::<u16>()?, U16_VALUE);
        assert_eq!(p.next::<Option<u16>>()?, None);
        assert_eq!(p.next::<i16>()?, I16_VALUE);
        assert_eq!(p.next::<Option<i16>>()?, None);

        // u32n, u32, i32n, i32,
        assert_eq!(p.next::<u32>()?, U32_VALUE);
        assert_eq!(p.next::<Option<u32>>()?, None);
        assert_eq!(p.next::<i32>()?, I32_VALUE);
        assert_eq!(p.next::<Option<i32>>()?, None);

        // u64n, u64, i64n, i64,
        assert_eq!(p.next::<u64>()?, U64_VALUE);
        assert_eq!(p.next::<Option<u64>>()?, None);
        assert_eq!(p.next::<i64>()?, I64_VALUE);
        assert_eq!(p.next::<Option<i64>>()?, None);

        // `bn`, `b`, `f32n`, `f32`, `f64n`, `f64`,
        assert_eq!(p.next::<bool>()?, BOOL_VALUE);
        assert_eq!(p.next::<Option<bool>>()?, None);
        assert_eq!(p.next::<f32>()?, F32_VALUE);
        assert_eq!(p.next::<Option<f32>>()?, None);
        assert_eq!(p.next::<f64>()?, F64_VALUE);
        assert_eq!(p.next::<Option<f64>>()?, None);

        // `cn`, `c`, `tn`, `t`, `bln`, `bl`,
        assert_eq!(p.next::<&str>()?, V_VALUE);
        assert_eq!(p.next::<Option<&str>>()?, None);
        assert_eq!(p.next::<&str>()?, T_VALUE);
        assert_eq!(p.next::<Option<&str>>()?, None);
        assert_eq!(p.next::<&[u8]>()?, B_VALUE);
        assert_eq!(p.next::<Option<&[u8]>>()?, None);

        // `dn`, `d`, `bin`, `bi`, `jn`, `j`
        assert_eq!(p.next::<&Decimal>()?, D_VALUE);
        assert_eq!(p.next::<Option<&Decimal>>()?, None);
        assert_eq!(p.next::<&Bit>()?, BI_VALUE);
        assert_eq!(p.next::<Option<&Bit>>()?, None);
        assert_eq!(p.next::<&Json>()?, J_VALUE);
        assert_eq!(p.next::<Option<&Json>>()?, None);

        // `dan`, `da`, `yn`, `y`, `tin`,`ti`,
        assert_eq!(p.next::<Date>()?, DA_VALUE);
        assert_eq!(p.next::<Option<Date>>()?, None);
        assert_eq!(p.next::<Year>()?, Y_VALUE);
        assert_eq!(p.next::<Option<Year>>()?, None);
        assert_eq!(p.next::<Time>()?, TI_VALUE);
        assert_eq!(p.next::<Option<Time>>()?, None);

        // `dtn`, `dt`, `tsn`, `ts`,
        assert_eq!(p.next::<DateTime>()?, DT_VALUE);
        assert_eq!(p.next::<Option<DateTime>>()?, None);
        assert_eq!(p.next::<Timestamp>()?, TS_VALUE);
        p.next::<Option<Timestamp>>()?;

        //`ban`, `ba`, `vban`, `vba`,
        assert_eq!(p.next::<&[u8]>()?, BA_VALUE);
        assert_eq!(p.next::<Option<&[u8]>>()?, None);
        assert_eq!(p.next::<&[u8]>()?, BA_VALUE);
        assert_eq!(p.next::<Option<&[u8]>>()?, None);

        //`en`, `e`, `sn`, `s`
        assert_eq!(p.next::<&str>()?, E_VALUE);
        assert_eq!(p.next::<Option<&str>>()?, None);
        assert_eq!(p.next::<&str>()?, S_VALUE);
        assert_eq!(p.next::<Option<&str>>()?, None);

        assert!(p.get_next_column_info().is_none())
    }

    std::mem::drop(conn);
    Ok(())
}

#[tokio::test]
async fn drop_cancel() -> Result<(), Error> {
    // Ensure that the drop/cleanup functionally works
    let mut conn =
        tokio::time::timeout(Duration::from_secs(2), Connection::connect(&opts())).await??;
    conn.execute("DROP TABLE IF EXISTS db_test3", ()).await?;
    conn.execute(
        "CREATE TABLE db_test3 (
        id BIGINT NOT NULL AUTO_INCREMENT,
        v INT NOT NULL,
        PRIMARY KEY (id)
        )",
        (),
    )
    .await?;

    // ********************************************************************************
    // Test dropping of normal queries, checking that cancel can recover the connection
    // ********************************************************************************

    for v in 0..50 {
        conn.execute("INSERT INTO db_test3 (v) VALUES (?)", (v,))
            .await?;
    }

    // Make sure this statement is prepared
    for (idx, (id, v)) in conn
        .fetch_all::<(i64, i32)>("SELECT id, v FROM db_test3", ())
        .await?
        .iter()
        .enumerate()
    {
        assert_eq!(*id, (idx + 1) as i64);
        assert_eq!(*v, idx as i32);
    }

    for c in 0.. {
        conn.set_cancel_count(None);
        conn.cleanup().await?;
        conn.set_cancel_count(Some(c));
        let t: Result<Vec<(i64, i32)>, _> = conn.fetch_all("SELECT id, v FROM db_test3", ()).await;
        match t {
            Err(e) if matches!(e.content(), ConnectionErrorContent::TestCancelled) => (),
            Ok(v) => {
                for (idx, (id, v)) in v.iter().enumerate() {
                    assert_eq!(*id, (idx + 1) as i64);
                    assert_eq!(*v, idx as i32);
                }
                break;
            }
            Err(e) => return Err(e.into()),
        }
    }

    // ********************************************************************************
    // Test dropping of preparing statements
    // ********************************************************************************
    for c in 0.. {
        conn.set_cancel_count(None);
        conn.cleanup().await?;

        conn.set_cancel_count(Some(c));
        let q = format!("DELETE FROM db_test3 WHERE id={}", c + 1000);
        match conn.execute(q, ()).await {
            Err(e) if matches!(e.content(), ConnectionErrorContent::TestCancelled) => {
                let q = format!("DELETE FROM db_test3 WHERE id={}", c + 2000);
                conn.set_cancel_count(None);
                conn.execute(q, ()).await?;
            }
            Ok(_) => break,
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}

#[tokio::test]
async fn test_transaction() -> Result<(), Error> {
    let mut conn =
        tokio::time::timeout(Duration::from_secs(2), Connection::connect(&opts())).await??;

    let mut tr = conn.begin().await?;
    tr.execute("DROP TABLE IF EXISTS db_test2", ()).await?;
    tr.execute(
        "CREATE TABLE db_test2 (
        id BIGINT NOT NULL AUTO_INCREMENT,
        v INT NOT NULL,
        t TEXT NOT NULL,
        PRIMARY KEY (id)
        )",
        (),
    )
    .await?;
    tr.commit().await?;

    let mut tr = conn.begin().await?;
    tr.execute(
        "INSERT INTO `db_test2` (v, t) VALUES (?, ?)",
        (42u32, "hello"),
    )
    .await?;
    tr.commit().await?;

    let _: (i64, i32, &str) = conn.fetch_one("SELECT id, v, t FROM db_test2", ()).await?;

    let mut tr = conn.begin().await?;
    tr.execute("DELETE FROM `db_test2`", ()).await?;
    tr.rollback().await?;

    let _: (i64, i32, &str) = conn.fetch_one("SELECT id, v, t FROM db_test2", ()).await?;

    Ok(())
}

#[tokio::test]
async fn drop_cancel_transaction() -> Result<(), Error> {
    let mut conn =
        tokio::time::timeout(Duration::from_secs(2), Connection::connect(&opts())).await??;

    let mut tr = conn.begin().await?;
    tr.execute("DROP TABLE IF EXISTS db_test4", ()).await?;
    tr.execute(
        "CREATE TABLE db_test4 (
        id BIGINT NOT NULL AUTO_INCREMENT,
        v INT NOT NULL,
        t TEXT NOT NULL,
        PRIMARY KEY (id)
        )",
        (),
    )
    .await?;
    tr.commit().await?;

    let mut commit_attempted = false;
    // Test dropping of a transaction at all event points
    for c in 0.. {
        conn.set_cancel_count(None);
        conn.cleanup().await?;

        let (cnt,): (i64,) = conn.fetch_one("SELECT COUNT(*) FROM db_test4", ()).await?;
        if cnt == 1 && commit_attempted {
            break;
        }
        assert_eq!(cnt, 0);

        // Tests are repeated due to caching of prepared queries
        conn.set_cancel_count(Some(c / 3));

        let mut tr = match conn.begin().await {
            Err(e) if matches!(e.content(), ConnectionErrorContent::TestCancelled) => {
                continue;
            }
            Ok(tr) => tr,
            Err(e) => return Err(e.into()),
        };

        match tr
            .execute("INSERT INTO db_test4 (v, t) VALUES (?, ?)", (42, "hat"))
            .await
        {
            Err(e) if matches!(e.content(), ConnectionErrorContent::TestCancelled) => {
                continue;
            }
            Ok(_) => (),
            Err(e) => return Err(e.into()),
        }

        commit_attempted = true;
        match tr.commit().await {
            Err(e) if matches!(e.content(), ConnectionErrorContent::TestCancelled) => {
                continue;
            }
            Ok(()) => (),
            Err(e) => return Err(e.into()),
        };

        break;
    }

    conn.set_cancel_count(None);
    conn.cleanup().await?;
    let (cnt,): (i64,) = conn.fetch_one("SELECT COUNT(*) FROM db_test4", ()).await?;
    assert_eq!(cnt, 1);

    let mut commit_attempted = false;
    for c in 0.. {
        conn.set_cancel_count(None);
        conn.cleanup().await?;

        let (cnt,): (i64,) = conn.fetch_one("SELECT COUNT(*) FROM db_test4", ()).await?;
        if cnt == 3 && commit_attempted {
            break;
        }
        assert_eq!(cnt, 1);

        // Tests are repeated due to caching of prepared queries
        conn.set_cancel_count(Some(c / 3));

        let mut tr = match conn.begin().await {
            Err(e) if matches!(e.content(), ConnectionErrorContent::TestCancelled) => continue,
            Ok(tr) => tr,
            Err(e) => return Err(e.into()),
        };

        match tr
            .execute("INSERT INTO db_test4 (v, t) VALUES (?, ?)", (55, "hat"))
            .await
        {
            Err(e) if matches!(e.content(), ConnectionErrorContent::TestCancelled) => continue,
            Ok(_) => (),
            Err(e) => return Err(e.into()),
        }

        let mut tr2 = match tr.begin().await {
            Err(e) if matches!(e.content(), ConnectionErrorContent::TestCancelled) => continue,
            Ok(tr) => tr,

            Err(e) => return Err(e.into()),
        };

        match tr2
            .execute("INSERT INTO db_test4 (v, t) VALUES (?, ?)", (43, "kat"))
            .await
        {
            Err(e) if matches!(e.content(), ConnectionErrorContent::TestCancelled) => continue,
            Ok(_) => (),
            Err(e) => return Err(e.into()),
        }

        match tr2.rollback().await {
            Err(e) if matches!(e.content(), ConnectionErrorContent::TestCancelled) => continue,
            Ok(_) => (),
            Err(e) => return Err(e.into()),
        };

        let mut tr2 = match tr.begin().await {
            Err(e) if matches!(e.content(), ConnectionErrorContent::TestCancelled) => continue,
            Ok(tr) => tr,
            Err(e) => return Err(e.into()),
        };

        match tr2
            .execute("INSERT INTO db_test4 (v, t) VALUES (?, ?)", (66, "kat"))
            .await
        {
            Err(e) if matches!(e.content(), ConnectionErrorContent::TestCancelled) => continue,
            Ok(_) => (),
            Err(e) => return Err(e.into()),
        }

        match tr2.commit().await {
            Err(e) if matches!(e.content(), ConnectionErrorContent::TestCancelled) => continue,
            Ok(_) => (),
            Err(e) => return Err(e.into()),
        };

        commit_attempted = true;
        match tr.commit().await {
            Err(e) if matches!(e.content(), ConnectionErrorContent::TestCancelled) => continue,
            Ok(tr) => tr,
            Err(e) => return Err(e.into()),
        };
        break;
    }

    conn.set_cancel_count(None);
    conn.cleanup().await?;
    let (cnt,): (i64,) = conn.fetch_one("SELECT COUNT(*) FROM db_test4", ()).await?;
    assert_eq!(cnt, 3);

    Ok(())
}

#[tokio::test]
async fn pool() -> Result<(), Error> {
    let pool = tokio::time::timeout(
        Duration::from_secs(2),
        Pool::connect(opts(), PoolOptions::new().max_connections(2)),
    )
    .await??;

    {
        let mut conn = pool.acquire().await?;

        let mut tr = conn.begin().await?;
        tr.execute("DROP TABLE IF EXISTS db_test5", ()).await?;
        tr.execute(
            "CREATE TABLE db_test5 (
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

    async fn insert_task(pool: Pool, i: i32) -> Result<(), Error> {
        let mut conn = pool.acquire().await?;
        let mut tr = conn.begin().await?;
        tr.execute("INSERT INTO db_test5 (v,t) VALUES (?, ?)", (i, "hi"))
            .await?;
        tr.commit().await?;
        Ok(())
    }

    let mut tasks = Vec::new();
    for i in 0..20 {
        tasks.push(tokio::task::spawn(insert_task(pool.clone(), i)));
    }

    for task in tasks {
        task.await??;
    }
    Ok(())
}

#[tokio::test]
async fn pool_drop() -> Result<(), Error> {
    let pool = tokio::time::timeout(
        Duration::from_secs(2),
        Pool::connect(opts(), PoolOptions::new().max_connections(2)),
    )
    .await??;

    {
        let mut conn = pool.acquire().await?;

        let mut tr = conn.begin().await?;
        tr.execute("DROP TABLE IF EXISTS db_test6", ()).await?;
        tr.execute(
            "CREATE TABLE db_test6 (
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

    async fn test_task(pool: Pool, c: usize) -> Result<(), Error> {
        let mut conn = pool.acquire().await?;

        conn.set_cancel_count(None);
        conn.cleanup().await?;

        let (cnt,): (i64,) = conn.fetch_one("SELECT COUNT(*) FROM db_test6", ()).await?;
        assert_eq!(cnt, 0);

        // Tests are repeated due to caching of prepared queries
        conn.set_cancel_count(Some(c / 3));

        let mut tr = match conn.begin().await {
            Err(e) if matches!(e.content(), ConnectionErrorContent::TestCancelled) => {
                return Ok(());
            }
            Ok(tr) => tr,
            Err(e) => return Err(e.into()),
        };

        match tr
            .execute("INSERT INTO db_test6 (v, t) VALUES (?, ?)", (42, "hat"))
            .await
        {
            Err(e) if matches!(e.content(), ConnectionErrorContent::TestCancelled) => {
                return Ok(());
            }
            Ok(_) => (),
            Err(e) => return Err(e.into()),
        }

        match tr.commit().await {
            Err(e) if matches!(e.content(), ConnectionErrorContent::TestCancelled) => {
                return Ok(());
            }
            Ok(()) => (),
            Err(e) => return Err(e.into()),
        };

        let _: Vec<(i64, i32, &str)> =
            match conn.fetch_all("SELECT id, v, t FROM db_test6", ()).await {
                Err(e) if matches!(e.content(), ConnectionErrorContent::TestCancelled) => {
                    return Ok(());
                }
                Ok(v) => v,
                Err(e) => return Err(e.into()),
            };

        return Ok(());
    }

    let mut tasks = Vec::new();
    for i in 0..40 {
        tasks.push(tokio::task::spawn(test_task(pool.clone(), i)));
    }

    Ok(())
}

#[tokio::test]
async fn typed() -> Result<(), Error> {
    let pool = tokio::time::timeout(
        Duration::from_secs(2),
        Pool::connect(opts(), PoolOptions::new().max_connections(2)),
    )
    .await??;

    {
        let mut conn = pool.acquire().await?;

        let mut tr = conn.begin().await?;
        tr.execute("DROP TABLE IF EXISTS db_test7", ()).await?;
        tr.execute(
            "CREATE TABLE db_test7 (
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

    let mut conn = pool.acquire().await?;
    qusql_mysql_type::execute!(conn, "INSERT INTO db_test7 (v,t) VALUES (?,?)", 42, "hat").await?;

    #[derive(Debug)]
    struct RowO {
        id: i64,
        v: i32,
        t: String,
    }

    #[derive(Debug)]
    struct RowB<'a> {
        id: i64,
        v: i32,
        t: &'a str,
    }

    let row =
        qusql_mysql_type::fetch_one!(conn, "SELECT id, v, t FROM db_test7 WHERE v = ?", 42).await?;
    assert_eq!(row.id, 1);
    assert_eq!(row.v, 42);
    assert_eq!(row.t, "hat");

    let row =
        qusql_mysql_type::fetch_one_owned!(conn, "SELECT id, v, t FROM db_test7 WHERE v = ?", 42)
            .await?;
    assert_eq!(row.id, 1);
    assert_eq!(row.v, 42);
    assert_eq!(row.t, "hat");

    let row = qusql_mysql_type::fetch_one_as!(
        RowB,
        conn,
        "SELECT id, v, t FROM db_test7 WHERE v = ?",
        42
    )
    .await?;
    assert_eq!(row.id, 1);
    assert_eq!(row.v, 42);
    assert_eq!(row.t, "hat");

    let row = qusql_mysql_type::fetch_one_as_owned!(
        RowO,
        conn,
        "SELECT id, v, t FROM db_test7 WHERE v = ?",
        42
    )
    .await?;
    assert_eq!(row.id, 1);
    assert_eq!(row.v, 42);
    assert_eq!(row.t, "hat");

    let row =
        qusql_mysql_type::fetch_optional!(conn, "SELECT id, v, t FROM db_test7 WHERE v = ?", 42)
            .await?
            .unwrap();
    assert_eq!(row.id, 1);
    assert_eq!(row.v, 42);
    assert_eq!(row.t, "hat");

    let row = qusql_mysql_type::fetch_optional_owned!(
        conn,
        "SELECT id, v, t FROM db_test7 WHERE v = ?",
        42
    )
    .await?
    .unwrap();
    assert_eq!(row.id, 1);
    assert_eq!(row.v, 42);
    assert_eq!(row.t, "hat");

    let row = qusql_mysql_type::fetch_optional_as!(
        RowB,
        conn,
        "SELECT id, v, t FROM db_test7 WHERE v = ?",
        42
    )
    .await?
    .unwrap();
    assert_eq!(row.id, 1);
    assert_eq!(row.v, 42);
    assert_eq!(row.t, "hat");

    let row = qusql_mysql_type::fetch_optional_as_owned!(
        RowO,
        conn,
        "SELECT id, v, t FROM db_test7 WHERE v = ?",
        42
    )
    .await?
    .unwrap();
    assert_eq!(row.id, 1);
    assert_eq!(row.v, 42);
    assert_eq!(row.t, "hat");

    let [row] = qusql_mysql_type::fetch_all!(conn, "SELECT id, v, t FROM db_test7 WHERE v = ?", 42)
        .await?
        .try_into()
        .unwrap();
    assert_eq!(row.id, 1);
    assert_eq!(row.v, 42);
    assert_eq!(row.t, "hat");

    let [row] =
        qusql_mysql_type::fetch_all_owned!(conn, "SELECT id, v, t FROM db_test7 WHERE v = ?", 42)
            .await?
            .try_into()
            .unwrap();
    assert_eq!(row.id, 1);
    assert_eq!(row.v, 42);
    assert_eq!(row.t, "hat");

    let [row] = qusql_mysql_type::fetch_all_as!(
        RowB,
        conn,
        "SELECT id, v, t FROM db_test7 WHERE v = ?",
        42
    )
    .await?
    .try_into()
    .unwrap();
    assert_eq!(row.id, 1);
    assert_eq!(row.v, 42);
    assert_eq!(row.t, "hat");

    let [row] = qusql_mysql_type::fetch_all_as_owned!(
        RowO,
        conn,
        "SELECT id, v, t FROM db_test7 WHERE v = ?",
        42
    )
    .await?
    .try_into()
    .unwrap();
    assert_eq!(row.id, 1);
    assert_eq!(row.v, 42);
    assert_eq!(row.t, "hat");

    let mut iter =
        qusql_mysql_type::fetch!(conn, "SELECT id, v, t FROM db_test7 WHERE v = ?", 42).await?;
    let row = iter.next().await?.unwrap();
    assert_eq!(row.id, 1);
    assert_eq!(row.v, 42);
    assert_eq!(row.t, "hat");
    assert!(iter.next().await?.is_none());

    let mut iter =
        qusql_mysql_type::fetch_owned!(conn, "SELECT id, v, t FROM db_test7 WHERE v = ?", 42)
            .await?;
    let row = iter.next().await?.unwrap();
    assert_eq!(row.id, 1);
    assert_eq!(row.v, 42);
    assert_eq!(row.t, "hat");
    assert!(iter.next().await?.is_none());

    let mut iter =
        qusql_mysql_type::fetch_as!(RowB, conn, "SELECT id, v, t FROM db_test7 WHERE v = ?", 42)
            .await?;
    let row = iter.next().await?.unwrap();
    assert_eq!(row.id, 1);
    assert_eq!(row.v, 42);
    assert_eq!(row.t, "hat");
    assert!(iter.next().await?.is_none());

    let mut iter = qusql_mysql_type::fetch_as_owned!(
        RowO,
        conn,
        "SELECT id, v, t FROM db_test7 WHERE v = ?",
        42
    )
    .await?;
    let row = iter.next().await?.unwrap();
    assert_eq!(row.id, 1);
    assert_eq!(row.v, 42);
    assert_eq!(row.t, "hat");
    assert!(iter.next().await?.is_none());

    Ok(())
}
