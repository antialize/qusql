# Reddit post - r/rust

**Title:**
Announcing qusql: compile-time SQL checking with no running database

---

**Body:**

If you use sqlx's `query!` macro, you know the drill: `cargo sqlx prepare`, commit `sqlx-data.json`, keep a database running in CI.  It works, but there is friction.

`qusql-sqlx-type` is a drop-in replacement for `sqlx::query!` that reads your schema from a plain SQL file next to your `Cargo.toml` and type-checks all queries at `cargo check` time with no database connection.  No side-channel file, no prepare step, offline CI.

When you typo a column name you get this instead of a runtime error:

```
error:    ╭─[ query:1:8 ]
          │
        1 │ SELECT titl, body FROM notes WHERE id = $1
          │        ──┬─  
          │          ╰─── Unknown identifier
          │ 
          │ Help: did you mean `title`?
       ───╯
 --> src/main.rs:7:24
```

It also narrows nullability through `WHERE` clauses (`WHERE x IS NOT NULL` gives you `T`, not `Option<T>`) and has stricter argument type checking than stock sqlx.

For MySQL/MariaDB there is [`qusql-mysql-type`](https://antialize.github.io/qusql/qusql-mysql.html), which wraps [`qusql-mysql`](https://antialize.github.io/qusql/qusql-mysql.html), a cancellation-safe async driver that benchmarks roughly 1.5-2x faster than sqlx on MySQL workloads.

Full write-up in the [book](https://antialize.github.io/qusql/), source on [GitHub](https://github.com/antialize/qusql).

- [`qusql-sqlx-type` on crates.io](https://crates.io/crates/qusql-sqlx-type)
- [`qusql-mysql-type` on crates.io](https://crates.io/crates/qusql-mysql-type)
- [`qusql-mysql` on crates.io](https://crates.io/crates/qusql-mysql)
