//! Books catalog - qusql-sqlx-type example
//!
//! Demonstrates compile-time SQL type-checking for PostgreSQL with sqlx.
//! Every `query!` call in this file is validated against `sqlx-type-schema.sql`
//! at compile time.  Type errors in SQL appear as Rust compiler errors - no
//! `cargo sqlx prepare` step is needed.
//!
//! # Running
//!
//! ```text
//! DATABASE_URL=postgres://localhost/books_example cargo run -p qusql-sqlx-type-books
//! ```
//!
//! The schema is bootstrapped automatically on first run.  Subsequent runs skip
//! every revision that has already been applied, leaving all existing rows intact.

use chrono::NaiveDate;
use qusql_sqlx_type::query;
use sqlx::postgres::PgPoolOptions;

/// The migration SQL is embedded verbatim.  `include_str!` is evaluated at
/// compile time (the file path is relative to this source file), while the
/// `query!` macros read the same file independently via `CARGO_MANIFEST_DIR`
/// to type-check every SQL statement against the declared schema.
const SCHEMA: &str = include_str!("../sqlx-type-schema.sql");

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://localhost/books_example".to_owned());

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;

    // Apply pending migrations.  On an empty database this creates every table;
    // on an existing one it only runs revisions not yet recorded in
    // schema_revisions.  The whole file is wrapped in BEGIN/COMMIT, so a
    // failed revision leaves the database unchanged.
    sqlx::raw_sql(SCHEMA).execute(&pool).await?;

    // -----------------------------------------------------------------------
    // Authors
    // -----------------------------------------------------------------------

    // INSERT ... RETURNING gives back a typed struct whose field names match
    // the RETURNING column list.  `id` is uuid -> UuidValue (uuid::Uuid with
    // the "uuid" feature, otherwise String).
    let author_id = query!(
        "INSERT INTO authors (name, email, bio)
         VALUES ($1, $2, $3)
         RETURNING id",
        "Ada Lovelace",
        "ada@lovelace.example",
        "English mathematician and the first computer programmer.",
    )
    .fetch_one(&pool)
    .await?
    .id;

    println!("Created author {author_id}");

    // -----------------------------------------------------------------------
    // Books
    // -----------------------------------------------------------------------

    // genre is declared as Genre ENUM in the schema; the macro accepts &str
    // for enum inputs and decodes them as String on the way out.
    let book1_id = query!(
        "INSERT INTO books (author_id, title, isbn, published_on, genre, total_copies)
         VALUES ($1, $2, $3, $4, $5::Genre, $6)
         RETURNING id",
        author_id,
        "Notes on the Analytical Engine",
        "978-0-000-00001-1",
        NaiveDate::from_ymd_opt(1843, 7, 10).unwrap(),
        "Science", // Genre enum - passed as &str, checked at compile time
        3_i32,
    )
    .fetch_one(&pool)
    .await?
    .id;

    let book2_id = query!(
        "INSERT INTO books (author_id, title, isbn, published_on, genre, total_copies)
         VALUES ($1, $2, $3, $4, $5::Genre, $6)
         RETURNING id",
        author_id,
        "Sketch of the Analytical Engine",
        "978-0-000-00002-8",
        NaiveDate::from_ymd_opt(1843, 9, 1).unwrap(),
        "Science",
        2_i32,
    )
    .fetch_one(&pool)
    .await?
    .id;

    // SELECT with a JOIN.  The macro infers the result struct from the SELECT
    // list; field types are derived from the schema:
    //   id           -> i32
    //   title / isbn -> String
    //   genre        -> String   (enum output decoded as text)
    //   total_copies -> i32
    let books = query!(
        "SELECT b.id, b.title, b.isbn, b.genre::text AS genre, b.total_copies
         FROM   books b
         JOIN   authors a ON a.id = b.author_id
         WHERE  a.id = $1
         ORDER  BY b.published_on",
        author_id,
    )
    .fetch_all(&pool)
    .await?;

    println!("\nBooks by Ada Lovelace:");
    for b in &books {
        println!(
            "  [{}] {} (ISBN: {}, genre: {}, {} copies)",
            b.id, b.title, b.isbn, b.genre, b.total_copies
        );
    }

    // -----------------------------------------------------------------------
    // Loans
    // -----------------------------------------------------------------------

    // due_date is date NOT NULL -> NaiveDate for both input and output.
    let loan_id = query!(
        "INSERT INTO loans (book_id, borrower_name, due_date)
         VALUES ($1, $2, $3)
         RETURNING id",
        book1_id,
        "Charles Babbage",
        NaiveDate::from_ymd_opt(2026, 5, 15).unwrap(),
    )
    .fetch_one(&pool)
    .await?
    .id;

    println!("\nLoan #{loan_id}: 'Notes on the Analytical Engine' issued to Charles Babbage");

    // returned_at IS NULL - only fetch active loans.
    // `returned_at` is nullable, so the field would be Option<DateTime<Utc>>;
    // it does not appear in this SELECT, but due_date (date NOT NULL) decodes
    // as chrono::NaiveDate directly.
    let active = query!(
        "SELECT l.id, b.title, l.borrower_name, l.due_date
         FROM   loans l
         JOIN   books b ON b.id = l.book_id
         WHERE  l.returned_at IS NULL
         ORDER  BY l.due_date",
    )
    .fetch_all(&pool)
    .await?;

    println!("\nActive loans ({} total):", active.len());
    for l in &active {
        println!(
            "  [{}] '{}' -> {} (due {})",
            l.id, l.title, l.borrower_name, l.due_date
        );
    }

    // Mark the book as returned.
    query!(
        "UPDATE loans SET returned_at = now() WHERE id = $1",
        loan_id,
    )
    .execute(&pool)
    .await?;

    println!("  Loan #{loan_id} returned.");

    // -----------------------------------------------------------------------
    // Reviews
    // -----------------------------------------------------------------------

    // `body text` is nullable; passing a &str to a nullable text column is
    // fine - ArgIn is implemented for both T and Option<T>.
    query!(
        "INSERT INTO reviews (book_id, reviewer_name, rating, body)
         VALUES ($1, $2, $3, $4)",
        book1_id,
        "Charles Babbage",
        5_i32,
        "An indispensable companion to the engine itself.",
    )
    .execute(&pool)
    .await?;

    query!(
        "INSERT INTO reviews (book_id, reviewer_name, rating, body)
         VALUES ($1, $2, $3, $4)",
        book2_id,
        "Charles Babbage",
        4_i32,
        "A clear and faithful translation of Menabrea's memoir.",
    )
    .execute(&pool)
    .await?;

    // `r.body` is nullable text -> decoded as Option<String>.
    // `r.rating` is integer NOT NULL -> decoded as i32.
    let reviews = query!(
        "SELECT r.reviewer_name, r.rating, r.body
         FROM   reviews r
         WHERE  r.book_id = $1
         ORDER  BY r.reviewed_at",
        book1_id,
    )
    .fetch_all(&pool)
    .await?;

    println!("\nReviews of 'Notes on the Analytical Engine':");
    for r in &reviews {
        let body = r.body.as_deref().unwrap_or("(no comment)");
        println!("  {} star(s) - {}: {}", r.rating, r.reviewer_name, body);
    }

    // -----------------------------------------------------------------------
    // Clean up
    // -----------------------------------------------------------------------

    // Delete in dependency order: reviews and loans before books, books before
    // authors (RESTRICT foreign keys enforce this).
    query!("DELETE FROM reviews WHERE book_id = $1", book1_id)
        .execute(&pool)
        .await?;
    query!("DELETE FROM reviews WHERE book_id = $1", book2_id)
        .execute(&pool)
        .await?;
    query!(
        "DELETE FROM loans WHERE book_id = $1 OR book_id = $2",
        book1_id,
        book2_id
    )
    .execute(&pool)
    .await?;
    query!("DELETE FROM books WHERE author_id = $1", author_id)
        .execute(&pool)
        .await?;
    query!("DELETE FROM authors WHERE id = $1", author_id)
        .execute(&pool)
        .await?;

    println!("\nDone - demo data cleaned up.");

    Ok(())
}
