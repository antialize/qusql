"""
SQL Parser Test Harness

This script tests the qusql-parse SQL parser by running test cases from JSON files.
It supports multiple SQL dialects (MySQL/MariaDB, PostgreSQL, SQLite) and provides
both interactive and automated testing modes.
"""

import rust_lexer
import subprocess
import json
from typing import TypedDict, NotRequired
import argparse


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


def test_dialect(args, tests_file: str, dialect: str, dialect_name: str) -> None:
    """
    Run tests for a specific SQL dialect.

    Args:
        args: Argparse namespace with command-line arguments
        tests_file: Path to the JSON file containing test cases
        dialect: Dialect identifier for the parser ('maria', 'postgresql', 'sqlite')
        dialect_name: Human-readable dialect name for output
    """
    tests = read_tests(tests_file)

    failure_count = 0

    for inp, test in tests.items():
        # Apply filter if specified
        if args.filter and args.filter not in inp:
            continue

        # Run the parser on this test case
        result = run_parser(test["input"], dialect, args.interactive or args.update_output)

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
                    print(f"Updated (success): {test['input'][:80]}")
                else:
                    test["failure"] = True
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
                    print("===============> Test state changed <=========")
                    print(f"Input: {test['input']}")
                    print(f"Success: {out['success']} was {test.get('failure', False)}")
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
                    if not out["success"]:
                        print(f"Test failed: '{inp}'")
                    else:
                        print(f"Unexpected success: '{inp}'")
                    print(f"Output: {out['value']}")
                    print(f"Issues:")
                    for issue in out["issues"]:
                        print(f"  {issue.replace('\n', '\n  ')}")
                    failure_count += 1
                else:
                    print(f"Test passed in: '{inp}'")

    # Save changes if in interactive or update-output mode
    if args.interactive or args.update_output:
        write_tests(tests_file, tests)
    else:
        print(f"\n{dialect_name} - Total failures: {failure_count} out of {len(tests)}")


def test_mysql(args) -> None:
    """Run MySQL/MariaDB dialect tests."""
    test_dialect(args, "mysql-tests.json", "maria", "MySQL/MariaDB")


def test_postgresql(args) -> None:
    """Run PostgreSQL dialect tests."""
    test_dialect(args, "postgres-tests.json", "postgresql", "PostgreSQL")


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

    args = parser.parse_args()

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
        test_mysql(args)
    elif args.command == "import-postgresql":
        import_postgresql_tests(args)
    elif args.command == "test-postgresql":
        test_postgresql(args)
