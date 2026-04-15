//! qusql-sqlx-type-notes -- a tiny CLI for managing notes.
//!
//! This example demonstrates compile-time SQL type checking for PostgreSQL
//! with qusql-sqlx-type.  Every `query!` call is verified against
//! `sqlx-type-schema.sql` at compile time: wrong column names, wrong argument
//! types, or wrong result types all become ordinary Rust compiler errors.
//! No `cargo sqlx prepare` step and no running database are needed to check
//! the code.
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
//! query!("UPDATE notes SET pinned = NOT pinned WHERE id = $1", "oops")
//! //                                                            ^^^^^^
//! // error: expected i32, found &str
//! ```
//!
//! ```text
//! // Return types are inferred from the schema, no annotations needed:
//! //   row.id         : i32            (integer NOT NULL)
//! //   row.title      : String         (text NOT NULL)
//! //   row.body       : Option<String> (text, nullable)
//! let notes = query!("SELECT id, title, body FROM notes ORDER BY id")
//!     .fetch_all(&pool).await?;
//! for n in &notes {
//!     println!("{}: {}", n.title, n.body.as_deref().unwrap_or(""));
//!     //                          ^^^^^^^ Option<String>, no annotation needed
//! }
//! ```

use clap::{Parser, Subcommand};
use qusql_sqlx_type::query;
use sqlx::postgres::PgPoolOptions;

/// The schema file is embedded at compile time via include_str!.
/// The same file is also read independently by the query! proc-macro to
/// type-check every SQL statement in this file.
const SCHEMA: &str = include_str!("../sqlx-type-schema.sql");

#[derive(Parser)]
#[command(about = "A tiny note-taking CLI powered by qusql-sqlx-type")]
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
    Pin { id: i32 },
    /// Delete a note by id
    Delete { id: i32 },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://localhost/notes_example".to_owned());

    let pool = PgPoolOptions::new()
        .max_connections(3)
        .connect(&database_url)
        .await?;

    // Bootstrap the schema.  CREATE TABLE IF NOT EXISTS is idempotent, so
    // running this on every startup is safe.
    sqlx::raw_sql(SCHEMA).execute(&pool).await?;

    match cli.command {
        // -----------------------------------------------------------------
        // Add
        // -----------------------------------------------------------------
        // INSERT ... RETURNING gives back a typed anonymous struct.  The
        // macro knows `id` is `integer NOT NULL`, so `row.id` has type i32
        // -- no casting needed.
        Command::Add { title, body } => {
            let row = query!(
                "INSERT INTO notes (title, body) VALUES ($1, $2) RETURNING id",
                title,
                body, // Option<String> -- nullable column, compiles fine
            )
            .fetch_one(&pool)
            .await?;

            println!("Created note #{}", row.id);
        }

        // -----------------------------------------------------------------
        // List
        // -----------------------------------------------------------------
        // `body` is `text` (nullable) so the macro gives it type
        // Option<String>.  `pinned` is `boolean NOT NULL` -> bool.
        // `created_at` is `timestamptz NOT NULL` ->
        // chrono::DateTime<chrono::Utc>.
        Command::List => {
            let notes = query!(
                "SELECT id, title, body, pinned, created_at
                 FROM   notes
                 ORDER  BY pinned DESC, created_at DESC",
            )
            .fetch_all(&pool)
            .await?;

            if notes.is_empty() {
                println!("No notes yet.  Use `notes add <title>` to create one.");
            } else {
                for n in &notes {
                    let pin = if n.pinned { "[pinned] " } else { "" };
                    let body = n.body.as_deref().unwrap_or("");
                    println!(
                        "#{} {}{} ({})",
                        n.id,
                        pin,
                        n.title,
                        n.created_at.format("%Y-%m-%d %H:%M"),
                    );
                    if !body.is_empty() {
                        println!("   {}", body);
                    }
                }
            }
        }

        // -----------------------------------------------------------------
        // Pin
        // -----------------------------------------------------------------
        // UPDATE without RETURNING -- the macro still checks that `id` is
        // the right type for the WHERE clause ($1 must be compatible with
        // `integer NOT NULL`).
        Command::Pin { id } => {
            let result = query!("UPDATE notes SET pinned = NOT pinned WHERE id = $1", id,)
                .execute(&pool)
                .await?;

            if result.rows_affected() == 0 {
                eprintln!("Note #{id} not found.");
            } else {
                println!("Toggled pin on note #{id}.");
            }
        }

        // -----------------------------------------------------------------
        // Delete
        // -----------------------------------------------------------------
        Command::Delete { id } => {
            let result = query!("DELETE FROM notes WHERE id = $1", id)
                .execute(&pool)
                .await?;

            if result.rows_affected() == 0 {
                eprintln!("Note #{id} not found.");
            } else {
                println!("Deleted note #{id}.");
            }
        }
    }

    Ok(())
}

// Compile-check the introductory doc examples from docs/src/qusql-sqlx-type.md.
// This function is never called; it exists solely so that the query! invocations
// are type-checked against the notes schema at `cargo check` / `cargo build` time.
#[allow(dead_code, unused_variables)]
async fn _compile_check(
    pool: sqlx::Pool<sqlx::Postgres>,
    title: String,
    body: Option<String>,
) -> Result<(), sqlx::Error> {
    use qusql_sqlx_type::query;

    // Argument types checked: $1 must be compatible with `text NOT NULL`,
    // $2 must be compatible with `text` (nullable; Option<...> is fine).
    query!(
        "INSERT INTO notes (title, body) VALUES ($1, $2)",
        title,
        body
    )
    .execute(&pool)
    .await?;

    // Return types inferred from the schema, no annotations needed:
    //   row.id         : i32                         (integer NOT NULL)
    //   row.title      : String                      (text NOT NULL)
    //   row.body       : Option<String>              (text, nullable)
    //   row.created_at : chrono::DateTime<chrono::Utc> (timestamptz NOT NULL)
    let notes = query!("SELECT id, title, body, created_at FROM notes ORDER BY id")
        .fetch_all(&pool)
        .await?;
    Ok(())
}
