//! qusql-mysql-type-books -- library catalog demonstrating the migration pattern.
//!
//! This example shows how to use stored procedures as idempotent revision
//! guards for schema migrations with qusql-mysql-type.  Every macro call is
//! verified at compile time against `qusql-mysql-type-schema.sql`.
//!
//! The schema evaluator in qusql-type executes each procedure body as if
//! against an empty database, so the compile-time type checker always sees
//! the fully-migrated schema regardless of the IF NOT EXISTS guards.
//!
//! # Running
//!
//! ```text
//! DATABASE_URL=mysql://books:books@127.0.0.1:3306/books_example \
//!     cargo run -p qusql-mysql-type-books
//! ```

use chrono::NaiveDate;
use qusql_mysql::{execute_script, ConnectionOptions, Pool, PoolOptions};
use qusql_mysql_type::{execute, fetch_all};

/// Schema is embedded at compile time for runtime bootstrap; the proc-macros
/// read the same file independently for type checking.
const SCHEMA: &str = include_str!("../qusql-mysql-type-schema.sql");

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "mysql://books:books@127.0.0.1:3306/books_example".to_owned());
    // Leak the URL string so ConnectionOptions can borrow it for 'static.
    let url: &'static str = Box::leak(url.into_boxed_str());

    let pool = Pool::connect(
        ConnectionOptions::from_url(url)?,
        PoolOptions::new().max_connections(5),
    )
    .await?;

    let mut conn = pool.acquire().await?;

    execute_script(&mut conn, SCHEMA).await?;

    // -----------------------------------------------------------------------
    // Authors
    // -----------------------------------------------------------------------

    // execute! checks that (VARCHAR, VARCHAR, TEXT) match the column types at
    // compile time.  last_insert_id() returns the AUTO_INCREMENT id as u64.
    let author_res = execute!(
        &mut conn,
        "INSERT INTO `authors` (`name`, `email`, `bio`) VALUES (?, ?, ?)",
        "Ada Lovelace",
        "ada@lovelace.example",
        "English mathematician and the first computer programmer.",
    )
    .await?;
    let author_id = author_res.last_insert_id() as i32;
    println!("Created author #{author_id}");

    // -----------------------------------------------------------------------
    // Books
    // -----------------------------------------------------------------------

    // `genre` is declared as ENUM; the macro accepts &str for enum inputs.
    // `published_on` is DATE; chrono::NaiveDate is the correct Rust type.
    let book1_res = execute!(
        &mut conn,
        "INSERT INTO `books` (`author_id`, `title`, `isbn`, `published_on`, `genre`, `total_copies`)
         VALUES (?, ?, ?, ?, ?, ?)",
        author_id,
        "Notes on the Analytical Engine",
        "978-0-000-00001-1",
        NaiveDate::from_ymd_opt(1843, 7, 10).unwrap(),
        "Science",
        3_i32,
    )
    .await?;
    let book1_id = book1_res.last_insert_id() as i32;

    let book2_res = execute!(
        &mut conn,
        "INSERT INTO `books` (`author_id`, `title`, `isbn`, `published_on`, `genre`, `total_copies`)
         VALUES (?, ?, ?, ?, ?, ?)",
        author_id,
        "Sketch of the Analytical Engine",
        "978-0-000-00002-8",
        NaiveDate::from_ymd_opt(1843, 9, 1).unwrap(),
        "Science",
        2_i32,
    )
    .await?;
    let book2_id = book2_res.last_insert_id() as i32;

    // SELECT with a JOIN.  fetch_all! returns borrowed row structs:
    //   id           -> i32     (INT NOT NULL)
    //   title        -> &str    (VARCHAR NOT NULL)
    //   isbn         -> &str    (VARCHAR NOT NULL)
    //   genre        -> &str    (ENUM decoded as text)
    //   total_copies -> i32     (INT NOT NULL)
    let books = fetch_all!(
        &mut conn,
        "SELECT b.`id`, b.`title`, b.`isbn`, b.`genre`, b.`total_copies`
         FROM `books` b
         JOIN `authors` a ON a.`id` = b.`author_id`
         WHERE a.`id` = ?
         ORDER BY b.`published_on`",
        author_id,
    )
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

    // due_date is DATE NOT NULL -> NaiveDate for both input and output.
    let loan_res = execute!(
        &mut conn,
        "INSERT INTO `loans` (`book_id`, `borrower_name`, `due_date`) VALUES (?, ?, ?)",
        book1_id,
        "Charles Babbage",
        NaiveDate::from_ymd_opt(2026, 5, 15).unwrap(),
    )
    .await?;
    let loan_id = loan_res.last_insert_id() as i32;
    println!("\nLoan #{loan_id}: 'Notes on the Analytical Engine' issued to Charles Babbage");

    // `returned_at` is TIMESTAMP NULL -> Option<...> in the row.
    // It does not appear in this SELECT; due_date (DATE NOT NULL) -> NaiveDate.
    let active = fetch_all!(
        &mut conn,
        "SELECT l.`id`, b.`title`, l.`borrower_name`, l.`due_date`
         FROM `loans` l
         JOIN `books` b ON b.`id` = l.`book_id`
         WHERE l.`returned_at` IS NULL
         ORDER BY l.`due_date`",
    )
    .await?;

    println!("\nActive loans ({} total):", active.len());
    for l in &active {
        println!(
            "  [{}] '{}' -> {} (due {})",
            l.id, l.title, l.borrower_name, l.due_date
        );
    }

    // Mark the book as returned.
    execute!(
        &mut conn,
        "UPDATE `loans` SET `returned_at` = NOW() WHERE `id` = ?",
        loan_id,
    )
    .await?;
    println!("  Loan #{loan_id} returned.");

    // -----------------------------------------------------------------------
    // Reviews
    // -----------------------------------------------------------------------

    // `body` is TEXT (nullable); passing &str to a nullable text column is
    // fine: the bind layer handles Option<&str> and &str both.
    execute!(
        &mut conn,
        "INSERT INTO `reviews` (`book_id`, `reviewer_name`, `rating`, `body`) VALUES (?, ?, ?, ?)",
        book1_id,
        "Charles Babbage",
        5_i8,
        "An indispensable companion to the engine itself.",
    )
    .await?;

    execute!(
        &mut conn,
        "INSERT INTO `reviews` (`book_id`, `reviewer_name`, `rating`, `body`) VALUES (?, ?, ?, ?)",
        book2_id,
        "Charles Babbage",
        4_i8,
        "A clear and faithful translation of Menabrea's memoir.",
    )
    .await?;

    // `body` is nullable TEXT -> Option<&str> in the row struct.
    // `rating` is TINYINT NOT NULL -> i8.
    let reviews = fetch_all!(
        &mut conn,
        "SELECT r.`reviewer_name`, r.`rating`, r.`body`
         FROM `reviews` r
         WHERE r.`book_id` = ?
         ORDER BY r.`reviewed_at`",
        book1_id,
    )
    .await?;

    println!("\nReviews of 'Notes on the Analytical Engine':");
    for r in &reviews {
        let body = r.body.unwrap_or("(no comment)");
        println!("  {} star(s) - {}: {}", r.rating, r.reviewer_name, body);
    }

    // -----------------------------------------------------------------------
    // Clean up
    // -----------------------------------------------------------------------

    // Delete in dependency order (RESTRICT foreign keys enforce this).
    execute!(
        &mut conn,
        "DELETE FROM `reviews` WHERE `book_id` = ?",
        book1_id
    )
    .await?;
    execute!(
        &mut conn,
        "DELETE FROM `reviews` WHERE `book_id` = ?",
        book2_id
    )
    .await?;
    execute!(
        &mut conn,
        "DELETE FROM `loans`   WHERE `book_id` = ? OR `book_id` = ?",
        book1_id,
        book2_id
    )
    .await?;
    execute!(
        &mut conn,
        "DELETE FROM `books`   WHERE `author_id` = ?",
        author_id
    )
    .await?;
    execute!(&mut conn, "DELETE FROM `authors` WHERE `id` = ?", author_id).await?;

    println!("\nDone - demo data cleaned up.");
    Ok(())
}
