import rust_lexer
import subprocess
import json
from typing import TypedDict, NotRequired
import argparse


class TestCase(TypedDict):
    input: str
    output: NotRequired[str]
    issues: NotRequired[list[str]]
    should_fail: NotRequired[bool]
    failure: NotRequired[bool]


def read_tests(path: str) -> dict[str, TestCase]:
    tests = {}
    with open(path, "r") as f:
        data = json.load(f)
        for item in data:
            tests[item["input"]] = item
    return tests


def write_tests(path: str, tests: dict[str, TestCase]) -> None:
    tests_lists = list(tests.values())
    tests_lists.sort(key=lambda t: t["input"])
    with open(path, "w") as f:
        json.dump(tests_lists, f, indent=2)


def import_mysql_tests(args) -> None:
    tests = read_tests("mysql-tests.json")
    with open("../../datafusion-sqlparser-rs/tests/sqlparser_mysql.rs") as f:
        for token in rust_lexer.lex(f.read()):
            token = token.strip()
            match = False
            for keyword in [
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
            ]:
                if keyword in token:
                    match = True
                    break
            if not match or token in tests:
                continue
            tests[token] = TestCase(input=token)
    write_tests("mysql-tests.json", tests)


def test_mysql(args) -> None:
    tests = read_tests("mysql-tests.json")

    failure_count = 0

    for inp, test in tests.items():
        if args.filter and args.filter not in inp:
            continue
        result = subprocess.run(
            [
                "cargo",
                "run",
                "--release",
                "--",
                "--dialect",
                "maria",
            ],
            capture_output=True,
            input=test["input"].encode(),
        )
        if args.interactive:
            if result.returncode != 0:
                print(f"Input: {test['input']}")
                print("Program crashed")
                test["failure"] = True
                test.pop("should_fail", None)
                test.pop("output", None)
                test.pop("issues", None)
            else:
                out = json.loads(result.stdout.decode())
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
                    while True:
                        choice = input(
                            "Mark success (s), failure (f), skip (i), stop(q)"
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

                    if "value" in out:
                        test["output"] = out.get("value")
                    else:
                        test.pop("output", None)
                    if "issues" in out:
                        test["issues"] = out.get("issues", [])
                    else:
                        test.pop("issues")

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
            if result.returncode != 0:
                print(f"Crash in: {inp}")
                failure_count += 1
            else:
                out = json.loads(result.stdout.decode())
                if out["success"] == test.get("should_fail", False):
                    if not out["success"]:
                        print(f"Test failed: '{inp}'")
                    else:
                        print(f"Unexpected success: '{inp}'")
                    print(f"Output: {out['value']}")
                    print(f"Issues:")
                    for issue in out["issues"]:
                        print(f"  {issue.replace('\n', '\n  ')}")
                else:
                    failure_count += 1
                    print(f"Test passed in: '{inp}'")

    if args.interactive:
        write_tests("mysql-tests.json", tests)
    else:
        print(f"Total failures: {failure_count} out of {len(tests)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="test")
    subparsers = parser.add_subparsers(
        help="subcommand help", required=True, dest="command"
    )
    subparsers.add_parser("import-mysql")

    test_mysql_args = subparsers.add_parser("test-mysql")
    test_mysql_args.add_argument(
        "--interactive",
        action="store_true",
        help="Update expected outputs for MySQL tests",
    )
    test_mysql_args.add_argument(
        "--filter",
        type=str,
        help="Only run tests whose input contains this string",
    )

    args = parser.parse_args()

    if args.command == "import-mysql":
        import_mysql_tests(args)
    elif args.command == "test-mysql":
        test_mysql(args)
