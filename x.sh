#!/bin/bash
set -eu
cargo clippy --all -- --deny warnings
echo "=================> Running clippy <=================="
cargo fmt --all
echo "=================> Running tests <=================="
cargo test --all
echo "=================> Running schema parse test <=================="
cargo run -p schema-parse-test -- /home/jakobt/dev/mono/rustweb/qusql-mysql-type-schema.sql
cd parse-test
echo "=================> Running mysql parse test <=================="
./test.py test-mysql --update --failures-only
echo "=================> Running postgresql parse test <=================="
./test.py test-postgresql --update --failures-only
echo "=================> Running export test <=================="
python3 ../qusql-parse/check_exports.py
