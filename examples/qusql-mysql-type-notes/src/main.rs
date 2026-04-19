//! qusql-mysql-type-notes -- a tiny CLI for managing notes.
//!
//! This example demonstrates compile-time SQL type checking for MariaDB/MySQL
//! with qusql-mysql-type.  Every macro call is verified against
//! `qusql-mysql-type-schema.sql` at compile time: wrong column names, wrong
//! argument types, or wrong result types all become ordinary Rust compiler
//! errors.  No running database is needed to check the code.
//!
//! # Usage
//!
//! ```text
//! # create the database first (see README.md), then:
//! notes add "Buy milk"
//! notes add "Read the docs" "Start with the README"
//! notes list
//! notes pin 1
//! notes delete 2
//! ```
//!
//! # Type safety
//!
//! ```rust,compile_fail
//! // Passing a string where an integer is expected is a compiler error:
//! execute!(&mut conn, "UPDATE notes SET pinned = NOT pinned WHERE id = ?", "oops")
//! //                                                                         ^^^^^^
//! // error: expected i32, found &str
//! ```
//!
//! ```rust,compile_fail
//! let notes = fetch_all!(
//!     &mut conn,
//!     "SELECT id, title, body FROM notes ORDER BY id",
//! )
//! .await?;
//! for n in &notes {
//!     println!("{}: {}", n.title, n.body.unwrap_or(""));
//!     //                          ^^^^^^^ Option<&str>, no annotation needed
//! }
//! ```

use clap::{Parser, Subcommand};
use qusql_mysql::{execute_script, ConnectionOptions, Pool, PoolOptions};
use qusql_mysql_type::{execute, fetch_all};

/// The schema file is embedded at compile time via include_str! so that
/// the binary can bootstrap an empty database at startup.  The same file
/// is read independently by the proc-macros to type-check every query.
const SCHEMA: &str = include_str!("../qusql-mysql-type-schema.sql");

#[derive(Parser)]
#[command(about = "A tiny note-taking CLI powered by qusql-mysql-type")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Add a new note
    Add {
        title: String,
        /// Optional longer description
        body: Option<String>,
    },
    /// List all notes, pinned ones first
    List,
    /// Toggle the pinned flag on a note
    Pin { id: u32 },
    /// Delete a note by id
    Delete { id: u32 },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "mysql://notes:notes@127.0.0.1:3306/notes_example".to_owned());
    // Leak the URL string so ConnectionOptions can borrow it for 'static.
    let url: &'static str = Box::leak(url.into_boxed_str());

    let pool = Pool::connect(
        ConnectionOptions::from_url(url)?,
        PoolOptions::new().max_connections(3),
    )
    .await?;

    let mut conn = pool.acquire().await?;

    // Bootstrap the schema.  CREATE TABLE IF NOT EXISTS is idempotent so
    // running this on every startup is safe.  execute_script handles DELIMITER
    // directives and skips semicolons inside string literals / comments.
    execute_script(&mut conn, SCHEMA).await?;

    match cli.command {
        // -----------------------------------------------------------------
        // Add
        // -----------------------------------------------------------------
        // execute! returns an ExecuteResult.  last_insert_id() gives back the
        // new AUTO_INCREMENT id.  The macro checks that the argument types
        // match the column types at compile time: `title` is VARCHAR NOT NULL
        // so &str is fine; `body` is TEXT (nullable) so Option<&str> is fine.
        Command::Add { title, body } => {
            let res = execute!(
                &mut conn,
                "INSERT INTO `notes` (`title`, `body`) VALUES (?, ?)",
                title.as_str(),
                body.as_deref(),
            )
            .await?;
            println!("Created note #{}", res.last_insert_id());
        }

        // -----------------------------------------------------------------
        // List
        // -----------------------------------------------------------------
        // fetch_all! generates a Row struct with borrowed &str/&[u8] values
        // (text and blobs reference directly into the parsed packet buffer).
        // `body` is TEXT (nullable) so row.body has type Option<&str>.
        // `pinned` is TINYINT(1) NOT NULL so row.pinned has type bool.
        Command::List => {
            let notes = fetch_all!(
                &mut conn,
                "SELECT `id`, `title`, `body`, `pinned`, `created_at`
                 FROM `notes`
                 ORDER BY `pinned` DESC, `created_at` DESC",
            )
            .await?;

            if notes.is_empty() {
                println!("No notes yet.  Use `notes add <title>` to create one.");
            } else {
                for n in &notes {
                    let pin = if n.pinned { "[pinned] " } else { "" };
                    let body = n.body.unwrap_or("");
                    println!("#{} {}{} ({})", n.id, pin, n.title, n.created_at);
                    if !body.is_empty() {
                        println!("   {}", body);
                    }
                }
            }
        }

        // -----------------------------------------------------------------
        // Pin
        // -----------------------------------------------------------------
        // UPDATE without a result row.  The macro still checks that the
        // argument type matches `id INT NOT NULL`.
        Command::Pin { id } => {
            let res = execute!(
                &mut conn,
                "UPDATE `notes` SET `pinned` = NOT `pinned` WHERE `id` = ?",
                id,
            )
            .await?;
            if res.affected_rows() == 0 {
                eprintln!("Note #{id} not found.");
            } else {
                println!("Toggled pin on note #{id}.");
            }
        }

        // -----------------------------------------------------------------
        // Delete
        // -----------------------------------------------------------------
        Command::Delete { id } => {
            let res = execute!(&mut conn, "DELETE FROM `notes` WHERE `id` = ?", id).await?;
            if res.affected_rows() == 0 {
                eprintln!("Note #{id} not found.");
            } else {
                println!("Deleted note #{id}.");
            }
        }
    }

    Ok(())
}

// Compile-check the doc examples from docs/src/qusql-mysql.md.
// These functions are never called; they exist solely so that the code is
// type-checked against the notes schema at `cargo check` / `cargo build` time.
#[allow(dead_code)]
async fn _compile_check_qusql_mysql(
    _pool: qusql_mysql::Pool,
) -> Result<(), qusql_mysql::ConnectionError> {
    use qusql_mysql::{
        ConnectionError, ConnectionOptions, Executor, ExecutorExt, Pool, PoolOptions,
    };

    async fn example() -> Result<(), ConnectionError> {
        let pool = Pool::connect(
            ConnectionOptions::from_url("mysql://user:pw@127.0.0.1:3306/db").unwrap(),
            PoolOptions::new().max_connections(10),
        )
        .await?;

        let mut conn = pool.acquire().await?;

        // Execute a statement
        let mut tr = conn.begin().await?;
        tr.execute("INSERT INTO notes (title) VALUES (?)", ("Hello",))
            .await?;
        tr.commit().await?;

        // Fetch rows as tuples: no schema knowledge required
        let _rows: Vec<(i64, String)> = conn.fetch_all("SELECT id, title FROM notes", ()).await?;
        Ok(())
    }
    Ok(())
}

#[allow(dead_code)]
async fn _compile_check_qusql_mysql_type(
    mut conn: qusql_mysql::Connection,
) -> Result<(), Box<dyn std::error::Error>> {
    use qusql_mysql_type::{execute, fetch_all};

    // Argument types are checked at compile time
    execute!(
        &mut conn,
        "INSERT INTO notes (title, pinned) VALUES (?, ?)",
        "Hello",
        false,
    )
    .await?;

    // Return types are inferred from the schema:
    // (i32, String, Option<String>)
    let notes = fetch_all!(&mut conn, "SELECT id, title, body FROM notes ORDER BY id",).await?;

    for n in &notes {
        println!("{}: {}", n.title, n.body.unwrap_or(""));
    }
    Ok(())
}
