// Integration tests for qusql-sqlx-type against a real PostgreSQL database.
//
// Start a local Postgres instance and set DATABASE_URL, e.g.:
//   podman run --rm -e POSTGRES_PASSWORD=test -e POSTGRES_DB=test \
//       -p 5432:5432 docker.io/postgres:16
//   DATABASE_URL=postgres://postgres:test@localhost:5432/test cargo test -p qusql-sqlx-type-test

#[cfg(test)]
mod test;
