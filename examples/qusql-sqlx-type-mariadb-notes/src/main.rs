use qusql_sqlx_type::query;
use sqlx::mysql::MySqlPool;
use std::env;

// Compile-check the _LIST_ doc example from docs/src/qusql-sqlx-type.md.
// This function is never called; it exists solely so that the query! invocation
// is type-checked against the MariaDB notes schema at `cargo check` time.
#[allow(dead_code, unused_variables)]
async fn _list_example(pool: MySqlPool) -> Result<(), sqlx::Error> {
    // Pass a slice; the macro expands _LIST_ to the correct number of ? at runtime.
    let ids: Vec<i32> = vec![1, 2, 3];
    let rows = query!("SELECT id, title FROM notes WHERE id IN (_LIST_)", &ids,)
        .fetch_all(&pool)
        .await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database_url = env::var("DATABASE_URL")?;
    let _pool = MySqlPool::connect(&database_url).await?;
    Ok(())
}
