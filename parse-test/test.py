#!/usr/bin/env python3
"""
SQL Parser Test Harness

This script tests the qusql-parse SQL parser by running test cases from JSON files.
It supports multiple SQL dialects (MySQL/MariaDB, PostgreSQL, SQLite) and provides
both interactive and automated testing modes.
"""

import rust_lexer
import subprocess
import json
import sys
from typing import TypedDict, NotRequired, Optional
import argparse
import time


class TestCase(TypedDict):
    """
    Represents a single test case for SQL parsing.

    Fields:
    - input: The SQL statement to parse
    - output: Expected AST output (optional)
    - issues: Expected parse issues/warnings (optional)
    - should_fail: Whether the test is expected to fail parsing (optional)
    - failure: Whether the test actually failed (optional)
    """

    input: str
    output: NotRequired[str]
    issues: NotRequired[list[str]]
    should_fail: NotRequired[bool]
    failure: NotRequired[bool]


def read_tests(path: str) -> dict[str, TestCase]:
    """
    Load test cases from a JSON file.

    Returns a dictionary mapping SQL input strings to TestCase objects.
    """
    tests = {}
    with open(path, "r") as f:
        data = json.load(f)
        for item in data:
            tests[item["input"]] = item
    return tests


def write_tests(path: str, tests: dict[str, TestCase]) -> None:
    """
    Write test cases back to a JSON file, sorted by input SQL.
    """
    tests_lists = list(tests.values())
    tests_lists.sort(key=lambda t: t["input"])
    with open(path, "w") as f:
        json.dump(tests_lists, f, indent=2)


def import_tests(
    tests_file: str, source_file: str, keywords: list[str], dialect_name: str
) -> None:
    """
    Import test cases from a datafusion-sqlparser-rs Rust test file.

    Extracts SQL strings from the Rust test file and adds them to the test JSON file
    if they match certain SQL keywords and aren't already present.

    Args:
        tests_file: Path to the JSON file to write tests to
        source_file: Path to the Rust source file to extract SQL from
        keywords: List of SQL keywords to filter for
        dialect_name: Human-readable dialect name for error messages
    """
    tests = read_tests(tests_file)
    try:
        with open(source_file) as f:
            for token in rust_lexer.lex(f.read()):
                token = token.strip()
                match = False
                # Check if this SQL statement contains a relevant keyword
                for keyword in keywords:
                    if keyword in token:
                        match = True
                        break
                # Skip if no keyword match or already in tests
                if not match or token in tests:
                    continue
                tests[token] = TestCase(input=token)
        write_tests(tests_file, tests)
        print(f"Imported {dialect_name} tests successfully")
    except FileNotFoundError:
        print(
            f"Warning: {dialect_name} test file not found at {source_file}, creating empty test set"
        )
        write_tests(tests_file, tests)


def import_mysql_tests(args) -> None:
    """
    Import MySQL test cases from datafusion-sqlparser-rs Rust test file.

    Extracts SQL strings from the Rust test file and adds them to mysql-tests.json
    if they match certain SQL keywords and aren't already present.
    """
    import_tests(
        tests_file="mysql-tests.json",
        source_file="../../datafusion-sqlparser-rs/tests/sqlparser_mysql.rs",
        keywords=[
            "UPDATE",
            "CREATE",
            "SELECT",
            "SHOW",
            "ALTER",
            "DROP",
            "INSERT",
            "DELETE",
            "KILL",
            "LOCK TABLES",
            "REPLACE",
            "FLUSH",
        ],
        dialect_name="MySQL",
    )


def import_postgresql_tests(args) -> None:
    """
    Import PostgreSQL test cases from datafusion-sqlparser-rs Rust test file.

    Extracts SQL strings from the Rust test file and adds them to postgres-tests.json
    if they match certain SQL keywords and aren't already present.
    """
    import_tests(
        tests_file="postgres-tests.json",
        source_file="../../datafusion-sqlparser-rs/tests/sqlparser_postgres.rs",
        keywords=[
            "UPDATE",
            "CREATE",
            "SELECT",
            "ALTER",
            "DROP",
            "INSERT",
            "DELETE",
            "WITH",
            "TRUNCATE",
            "COPY",
        ],
        dialect_name="PostgreSQL",
    )


def run_parser(sql: str, dialect: str, not_pretty: bool) -> subprocess.CompletedProcess:
    """
    Run the parse-test binary with the given SQL and dialect.

    Args:
        sql: The SQL statement to parse
        dialect: The SQL dialect to use ('maria', 'postgresql', or 'sqlite')

    Returns:
        CompletedProcess object with stdout/stderr/returncode
    """
    return subprocess.run(
        [
            "../target/release/parse-test",
            "--dialect",
            dialect,
            "--output-format",
            "json" if not_pretty else "pretty-json",
        ],
        capture_output=True,
        input=sql.encode(),
    )


def test_dialect(args, tests_file: str, dialect: str, dialect_name: str) -> int:
    """
    Run tests for a specific SQL dialect.

    Args:
        args: Argparse namespace with command-line arguments
        tests_file: Path to the JSON file containing test cases
        dialect: Dialect identifier for the parser ('maria', 'postgresql', 'sqlite')
        dialect_name: Human-readable dialect name for output

    Returns:
        Number of failed tests
    """
    tests = read_tests(tests_file)

    failure_count = 0

    for inp, test in tests.items():
        # Apply filter if specified
        if args.filter and args.filter not in inp:
            continue

        # Run the parser on this test case
        result = run_parser(
            test["input"], dialect, args.interactive or args.update_output
        )

        if args.update_output:
            # Update output mode: automatically update output and issues without user input
            if result.returncode != 0:
                print(f"Crash: {test['input'][:80]}")
                test["failure"] = True
                test.pop("output", None)
                test.pop("issues", None)
            else:
                out = json.loads(result.stdout.decode())
                # Update test output and issues
                if "value" in out:
                    test["output"] = out.get("value")
                else:
                    test.pop("output", None)
                if "issues" in out:
                    test["issues"] = out.get("issues", [])
                else:
                    test.pop("issues", None)

                # Update failure flag based on parse success (but don't touch should_fail)
                if out["success"]:
                    test.pop("failure", None)
                    if not getattr(args, "failures_only", False):
                        print(f"Updated (success): {test['input'][:80]}")
                else:
                    test["failure"] = True
                    if not getattr(args, "failures_only", False):
                        print(f"Updated (failure): {test['input'][:80]}")
        elif args.interactive:
            # Interactive mode: prompt user to update expected results
            if result.returncode != 0:
                print(f"Input: {test['input']}")
                print("Program crashed")
                test["failure"] = True
                test.pop("should_fail", None)
                test.pop("output", None)
                test.pop("issues", None)
            else:
                out = json.loads(result.stdout.decode())
                # Check if test output has changed
                if (
                    test.get("output") != out.get("value")
                    or out["success"] == test.get("failure", False)
                    or out["issues"] != test.get("issues", [])
                ):
                    if not getattr(args, "failures_only", False):
                        print("===============> Test state changed <=========")
                        print(f"Input: {test['input']}")
                        print(
                            f"Success: {out['success']} was {test.get('failure', False)}"
                        )
                        print(f"Output: {out['value']}")
                        print(f"Issues: {out['issues'] == test.get('issues', [])}")
                        for issue in out["issues"]:
                            print(f"  {issue.replace('\n', '\n  ')}")

                    # Prompt user for action
                    while True:
                        choice = input(
                            "Mark success (s), failure (f), skip (i), stop(q): "
                        )
                        if choice in ("s", "f", "i", "q"):
                            break

                    if choice == "s":
                        success = True
                    elif choice == "f":
                        success = False
                    elif choice == "i":
                        continue
                    else:
                        break

                    # Update test expectations based on user input
                    if "value" in out:
                        test["output"] = out.get("value")
                    else:
                        test.pop("output", None)
                    if "issues" in out:
                        test["issues"] = out.get("issues", [])
                    else:
                        test.pop("issues", None)

                    if success and not out["success"]:
                        test["should_fail"] = True
                    else:
                        test.pop("should_fail", None)

                    if out["success"]:
                        test.pop("failure", None)
                    else:
                        test["failure"] = True
                else:
                    if not getattr(args, "failures_only", False):
                        print(f"No change: {inp}")
        else:
            # Non-interactive mode: just report pass/fail
            if result.returncode != 0:
                print(f"Crash in: {inp}")
                failure_count += 1
            else:
                out = json.loads(result.stdout.decode())
                # Check if test behaved as expected
                if out["success"] == test.get("should_fail", False):
                    # Test failed (either couldn't parse expected-to-parse, or parsed expected-to-fail)
                    if not out["success"]:
                        print(f"Test failed: '{inp}'")
                    else:
                        print(f"Unexpected success: '{inp}'")
                    print(f"Output: {out['value']}")
                    print("Issues:")
                    for issue in out["issues"]:
                        print(f"  {issue.replace('\\n', '\\n  ')}")
                    failure_count += 1
                    limit = getattr(args, "limit", None)
                    if limit is not None and failure_count >= limit:
                        break
                else:
                    if not getattr(args, "failures_only", False):
                        print(f"Test passed in: '{inp}'")

    # Save changes if in interactive or update-output mode
    if args.interactive or args.update_output:
        write_tests(tests_file, tests)
    elif not args.filter and not getattr(args, "limit", None):
        print(f"\n{dialect_name} - Total failures: {failure_count} out of {len(tests)}")
    return failure_count


def test_mysql(args) -> None:
    """Run MySQL/MariaDB dialect tests."""
    test_dialect(args, "mysql-tests.json", "maria", "MySQL/MariaDB")


def test_postgresql(args) -> None:
    """Run PostgreSQL dialect tests."""
    test_dialect(args, "postgres-tests.json", "postgresql", "PostgreSQL")


def validate_database(
    args,
    tests_file: str,
    setups_file: str,
    container_name: str,
    container_image: str,
    db_name: str,
    db_password: str,
    db_port: int,
    client_command: str,
    client_args: list[str],
    health_check_command: list[str],
    health_check_success: str,
    db_init_sql: list[str],
    setup_file_name: str,
) -> None:
    """
    Generic function to validate test cases against a real database running in Podman.
    This helps ensure that 'should_fail' flags are correct.

    Args:
        args: Command-line arguments
        tests_file: Path to JSON file with test cases
        setups_file: Path to JSON file with named setups
        container_name: Name for the Podman container
        container_image: Docker image to use
        db_name: Database name to create and use
        db_password: Database password
        db_port: Host port to map to container
        client_command: Database client command (e.g., 'mariadb', 'psql')
        client_args: Arguments for client command
        health_check_command: Command to check if database is ready
        health_check_success: String to look for in health check output
        db_init_sql: SQL commands to initialize database for each test
        setup_file_name: Human-readable name of setup file for error messages
    """
    # Load named setups if available
    named_setups = {}
    try:
        with open(setups_file, "r") as f:
            named_setups = json.load(f)
    except FileNotFoundError:
        pass

    # Load tests
    with open(tests_file, "r") as f:
        tests = json.load(f)

    # Filter tests if requested
    if args.filter:
        tests = [t for t in tests if args.filter in t["input"]]

    # Limit if requested
    if args.limit:
        tests = tests[: args.limit]

    def start_container():
        """Start database container using Podman"""
        print(f"🐋 Starting {container_image} container...")

        # Check if container already exists
        result = subprocess.run(
            [
                "podman",
                "ps",
                "-a",
                "--filter",
                f"name={container_name}",
                "--format",
                "{{.Names}}",
            ],
            capture_output=True,
            text=True,
        )

        if container_name in result.stdout:
            print(f"   Container {container_name} already exists, removing...")
            subprocess.run(["podman", "rm", "-f", container_name], check=True)

        # Start new container - build command dynamically
        run_cmd = [
            "podman",
            "run",
            "-d",
            "--name",
            container_name,
            "-p",
            f"{db_port}:5432" if "postgres" in container_image else f"{db_port}:3306",
        ]

        # Add environment variables based on database type
        if "postgres" in container_image:
            run_cmd.extend(
                [
                    "-e",
                    f"POSTGRES_PASSWORD={db_password}",
                    "-e",
                    f"POSTGRES_DB={db_name}",
                ]
            )
        else:  # MariaDB/MySQL
            run_cmd.extend(
                [
                    "-e",
                    f"MYSQL_ROOT_PASSWORD={db_password}",
                    "-e",
                    f"MYSQL_DATABASE={db_name}",
                ]
            )

        run_cmd.append(container_image)
        subprocess.run(run_cmd, check=True)

        print(f"   Waiting for {container_image} to be ready...")
        max_attempts = 60
        for i in range(max_attempts):
            result = subprocess.run(
                ["podman", "exec", container_name] + health_check_command,
                capture_output=True,
                text=True,
            )

            if health_check_success in result.stdout:
                print(f"   ✓ {container_image} is ready!")
                return

            time.sleep(1)

        raise Exception(f"{container_image} failed to start in time")

    def stop_container():
        """Stop and remove database container"""
        print(f"\n🛑 Stopping {container_image} container...")
        subprocess.run(["podman", "rm", "-f", container_name], capture_output=True)

    def execute_sql(sql: str) -> tuple[bool, Optional[str]]:
        """
        Execute SQL in database container.
        Returns (success: bool, error_message: Optional[str])
        """
        result = subprocess.run(
            ["podman", "exec", "-i", container_name, client_command] + client_args,
            input=sql,
            capture_output=True,
            text=True,
        )

        # Check both return code and stderr for errors
        # Note: psql and mariadb return 0 even when SQL statements fail,
        # so we must check stderr for "ERROR:" to detect failures
        has_error = result.returncode != 0 or "ERROR:" in result.stderr
        success = not has_error
        error = result.stderr if has_error else None
        return success, error

    def test_case(test: dict) -> tuple[bool, Optional[str]]:
        """
        Test a single case with fresh database.
        Returns (success: bool, error_message: Optional[str])
        """
        setup_name = test.get("setup")

        # Only named setups are supported (must be a string reference)
        if not isinstance(setup_name, str):
            return False, f"Setup must be a string reference to {setup_file_name}"

        if setup_name not in named_setups:
            return False, f"Unknown setup: {setup_name}"

        setup_statements = named_setups[setup_name]
        input_sql = test["input"]

        # Build SQL with fresh database for each test
        sql_parts = list(db_init_sql)

        # Add semicolons to setup statements if they don't have them
        for stmt in setup_statements:
            stmt = stmt.strip()
            if not stmt.endswith(";"):
                stmt += ";"
            sql_parts.append(stmt)
        sql_parts.append(input_sql + ";")

        sql = "\n".join(sql_parts)
        return execute_sql(sql)

    # Main validation logic
    try:
        start_container()

        print(f"\n📊 Validating {len(tests)} test cases...\n")

        mismatches = []
        correct = 0
        skipped = 0

        for i, test in enumerate(tests):
            input_sql = test["input"]
            should_fail = test.get("should_fail", False)

            # Skip tests without setup
            if "setup" not in test:
                skipped += 1
                continue

            success, error = test_case(test)
            actual_fails = not success

            # Check if expectation matches reality
            if should_fail == actual_fails:
                correct += 1
                status = "✓"
            else:
                status = "✗"
                mismatches.append(
                    {
                        "index": i,
                        "input": input_sql,
                        "should_fail": should_fail,
                        "actual_fails": actual_fails,
                        "error": error,
                    }
                )

            print(
                f"{status} [{i + 1}/{len(tests)}] {input_sql[:60]}{'...' if len(input_sql) > 60 else ''}"
            )
            if status == "✗":
                print(
                    f"    Expected: {'FAIL' if should_fail else 'SUCCESS'}, Got: {'FAIL' if actual_fails else 'SUCCESS'}"
                )
                if error:
                    # Print first 300 chars of error
                    error_preview = error[:300] if len(error) > 300 else error
                    print(f"    Error: {error_preview}")

        # Summary
        print(f"\n{'=' * 70}")
        print(
            f"Results: {correct} correct, {len(mismatches)} mismatches, {skipped} skipped"
        )

        if mismatches:
            print(f"\n❌ Found {len(mismatches)} mismatches:")
            for m in mismatches:
                print(f"\n  Test #{m['index']}: {m['input']}")
                print(
                    f"    should_fail: {m['should_fail']} → should be: {m['actual_fails']}"
                )

            # Update if requested
            if args.update:
                with open(tests_file, "r") as f:
                    all_tests = json.load(f)

                for mismatch in mismatches:
                    idx = mismatch["index"]
                    all_tests[idx]["should_fail"] = mismatch["actual_fails"]

                with open(tests_file, "w") as f:
                    json.dump(all_tests, f, indent=2)

                print(f"\n✏️  Updated {len(mismatches)} test cases in {tests_file}")
        else:
            print(f"\n✓ All tests match {container_image} behavior!")

    finally:
        stop_container()


def validate_mysql(args) -> None:
    """
    Validate MySQL test cases against a real MariaDB database running in Podman.
    This helps ensure that 'should_fail' flags are correct.
    """
    validate_database(
        args=args,
        tests_file="mysql-tests.json",
        setups_file="mysql-setups.json",
        container_name="qusql-test-mysql",
        container_image="docker.io/library/mariadb:latest",
        db_name="testdb",
        db_password="testpass123",
        db_port=13306,
        client_command="mariadb",
        client_args=["-h", "127.0.0.1", "-uroot", "-ptestpass123", "testdb"],
        health_check_command=[
            "mariadb-admin",
            "ping",
            "-h",
            "localhost",
            "-uroot",
            "-ptestpass123",
        ],
        health_check_success="mysqld is alive",
        db_init_sql=[
            "DROP DATABASE IF EXISTS testdb;",
            "CREATE DATABASE testdb;",
            "USE testdb;",
        ],
        setup_file_name="mysql-setups.json",
    )


def set_should_fail(args) -> None:
    """
    Set or clear the should_fail flag on a single test matched by exact input string.

    Args:
        args: Argparse namespace with 'dialect', 'input', and 'value' fields
    """
    tests_file = "postgres-tests.json" if args.dialect == "postgresql" else "mysql-tests.json"
    tests = read_tests(tests_file)

    if args.input not in tests:
        print(f"Error: no test found with input: {args.input!r}")
        return

    test = tests[args.input]
    if args.value:
        if not test.get("should_fail", False):
            test["should_fail"] = True
            print(f"Set should_fail=true:  {args.input[:80]}")
        else:
            print(f"Already should_fail=true: {args.input[:80]}")
    else:
        if test.get("should_fail", False):
            test.pop("should_fail")
            print(f"Set should_fail=false: {args.input[:80]}")
        else:
            print(f"Already should_fail=false: {args.input[:80]}")

    write_tests(tests_file, tests)


def validate_postgresql(args) -> None:
    """
    Validate PostgreSQL test cases against a real PostgreSQL database running in Podman.
    This helps ensure that 'should_fail' flags are correct.
    """
    validate_database(
        args=args,
        tests_file="postgres-tests.json",
        setups_file="postgres-setups.json",
        container_name="qusql-test-postgresql",
        container_image="docker.io/library/postgres:latest",
        db_name="testdb",
        db_password="testpass123",
        db_port=15432,
        client_command="psql",
        client_args=["-h", "127.0.0.1", "-U", "postgres", "-d", "postgres"],
        health_check_command=["pg_isready", "-U", "postgres"],
        health_check_success="accepting connections",
        db_init_sql=[
            "DROP DATABASE IF EXISTS testdb;",
            "CREATE DATABASE testdb;",
            "\\c testdb;",
        ],
        setup_file_name="postgres-setups.json",
    )


if __name__ == "__main__":
    # Set up command-line argument parser
    parser = argparse.ArgumentParser(
        prog="test", description="SQL parser test harness for qusql-parse"
    )
    subparsers = parser.add_subparsers(
        help="subcommand help", required=True, dest="command"
    )

    # MySQL import command
    subparsers.add_parser(
        "import-mysql", help="Import MySQL test cases from datafusion-sqlparser-rs"
    )

    # MySQL test command
    test_mysql_args = subparsers.add_parser(
        "test-mysql", help="Run MySQL/MariaDB dialect tests"
    )
    test_mysql_args.add_argument(
        "--interactive",
        action="store_true",
        help="Update expected outputs for MySQL tests interactively",
    )
    test_mysql_args.add_argument(
        "--update-output",
        action="store_true",
        help="Automatically update test outputs and issues without prompting",
    )
    test_mysql_args.add_argument(
        "--filter",
        type=str,
        help="Only run tests whose input contains this string",
    )
    test_mysql_args.add_argument(
        "--failures-only",
        action="store_true",
        help="Only print failed tests and the final summary line",
    )
    test_mysql_args.add_argument(
        "--limit",
        type=int,
        help="Stop after this many failures",
    )

    # PostgreSQL import command
    subparsers.add_parser(
        "import-postgresql",
        help="Import PostgreSQL test cases from datafusion-sqlparser-rs",
    )

    # PostgreSQL test command
    test_postgresql_args = subparsers.add_parser(
        "test-postgresql", help="Run PostgreSQL dialect tests"
    )
    test_postgresql_args.add_argument(
        "--interactive",
        action="store_true",
        help="Update expected outputs for PostgreSQL tests interactively",
    )
    test_postgresql_args.add_argument(
        "--update-output",
        action="store_true",
        help="Automatically update test outputs and issues without prompting",
    )
    test_postgresql_args.add_argument(
        "--filter",
        type=str,
        help="Only run tests whose input contains this string",
    )
    test_postgresql_args.add_argument(
        "--failures-only",
        action="store_true",
        help="Only print failed tests and the final summary line",
    )
    test_postgresql_args.add_argument(
        "--limit",
        type=int,
        help="Stop after this many failures",
    )

    # MySQL validation command
    validate_mysql_args = subparsers.add_parser(
        "validate-mysql",
        help="Validate MySQL test cases against real MySQL database in Podman",
    )
    validate_mysql_args.add_argument(
        "--limit",
        type=int,
        help="Only validate the first N tests",
    )
    validate_mysql_args.add_argument(
        "--filter",
        type=str,
        help="Only validate tests whose input contains this string",
    )
    validate_mysql_args.add_argument(
        "--update",
        action="store_true",
        help="Automatically update should_fail flags based on MySQL behavior",
    )

    # set-should-fail command
    set_should_fail_args = subparsers.add_parser(
        "set-should-fail",
        help="Set should_fail=true on a specific test by exact SQL input",
    )
    set_should_fail_args.add_argument(
        "dialect",
        choices=["postgresql", "mysql"],
        help="Which test suite to modify",
    )
    set_should_fail_args.add_argument(
        "input",
        type=str,
        help="Exact SQL input string of the test to modify",
    )

    # unset-should-fail command
    unset_should_fail_args = subparsers.add_parser(
        "unset-should-fail",
        help="Remove should_fail flag from a specific test by exact SQL input",
    )
    unset_should_fail_args.add_argument(
        "dialect",
        choices=["postgresql", "mysql"],
        help="Which test suite to modify",
    )
    unset_should_fail_args.add_argument(
        "input",
        type=str,
        help="Exact SQL input string of the test to modify",
    )

    # PostgreSQL validation command
    validate_postgresql_args = subparsers.add_parser(
        "validate-postgresql",
        help="Validate PostgreSQL test cases against real PostgreSQL database in Podman",
    )
    validate_postgresql_args.add_argument(
        "--limit",
        type=int,
        help="Only validate the first N tests",
    )
    validate_postgresql_args.add_argument(
        "--filter",
        type=str,
        help="Only validate tests whose input contains this string",
    )
    validate_postgresql_args.add_argument(
        "--update",
        action="store_true",
        help="Automatically update should_fail flags based on PostgreSQL behavior",
    )

    args = parser.parse_args()

    if args.command not in ("set-should-fail", "unset-should-fail"):
        subprocess.run(
            [
                "cargo",
                "build",
                "--release",
            ]
        )

    # Route to appropriate handler based on subcommand
    if args.command == "import-mysql":
        import_mysql_tests(args)
    elif args.command == "test-mysql":
        if test_mysql(args):
            sys.exit(1)
    elif args.command == "validate-mysql":
        validate_mysql(args)
    elif args.command == "import-postgresql":
        import_postgresql_tests(args)
    elif args.command == "test-postgresql":
        if test_postgresql(args):
            sys.exit(1)
    elif args.command == "validate-postgresql":
        validate_postgresql(args)
    elif args.command == "set-should-fail":
        args.value = True
        set_should_fail(args)
    elif args.command == "unset-should-fail":
        args.value = False
        set_should_fail(args)
