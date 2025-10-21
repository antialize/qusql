# Run tests against a live mariadb

Start a test database and load the schema
```
podman run --name test_db -p 3367:3306 --rm -e MARIADB_ALLOW_EMPTY_ROOT_PASSWORD=1 --mount type=tmpfs,destination=/var/lib/mysql -e MYSQL_DATABASE=db docker.io/mariadb:12.0
```

In another tab run
```
mysql -h 127.0.0.1 -P 3367 -u root db < mariadb_test/src/schema.sql
``

Next run the tests as many times as wanted
```
cargo run -p mariadb_test
```