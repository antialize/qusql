#!/bin/bash

for p in qusql-parse qusql-type qusql-sqlx-type qusql-mysql-type qusql-py-mysql-type-plugin; do
    (cd $p && cargo semver-checks)
done