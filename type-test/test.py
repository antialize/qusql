#!/usr/bin/env python3
"""
Test runner for type_test schema typing.

Each test case is a pair of:
  <name>.sql   - the SQL schema file
  <name>.json  - the expected JSON output (produced by type_test)

Usage:
  python3 test.py               # run default tests (expected to pass)
  python3 test.py --all         # run all tests including known-failing ones
  python3 test.py --update      # overwrite expected outputs with current output
  python3 test.py mysql1        # run a single test by name (no extension)
"""

import argparse
import json
import subprocess
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent.resolve()
WORKSPACE = SCRIPT_DIR.parent

# Map of test-name -> dialect flag passed to type_test
DIALECT: dict[str, str] = {
    "mysql1": "maria-db",
}
DEFAULT_DIALECT = "postgre-sql"


def run_type_test(sql_file: Path, dialect: str) -> tuple[int, str]:
    """Run type_test via cargo run and return (exit_code, stdout)."""
    result = subprocess.run(
        [
            "cargo",
            "run",
            "-p",
            "type_test",
            "--",
            str(sql_file),
            "--dialect",
            dialect,
            "--error-format",
            "pretty",
        ],
        capture_output=True,
        text=True,
        cwd=WORKSPACE,
    )
    return result.returncode, result.stdout.strip()


def diff_tables(expected: dict, actual: dict) -> list[str]:
    """Return a concise list of change descriptions between two schema dicts."""
    lines = []

    exp_tables = {t["name"]: t for t in expected.get("tables", [])}
    act_tables = {t["name"]: t for t in actual.get("tables", [])}

    for name in sorted(exp_tables.keys() - act_tables.keys()):
        lines.append(f"  - table removed: {name}")
    for name in sorted(act_tables.keys() - exp_tables.keys()):
        lines.append(f"  + table added:   {name}")

    for name in sorted(exp_tables.keys() & act_tables.keys()):
        et = exp_tables[name]
        at = act_tables[name]
        prefix = f"  table {name!r}"

        if et.get("view", False) != at.get("view", False):
            lines.append(f"{prefix}: view changed {et.get('view')} -> {at.get('view')}")

        exp_cols = {c["name"]: c for c in et.get("columns", [])}
        act_cols = {c["name"]: c for c in at.get("columns", [])}

        for col in sorted(exp_cols.keys() - act_cols.keys()):
            lines.append(f"{prefix}: column removed: {col}")
        for col in sorted(act_cols.keys() - exp_cols.keys()):
            lines.append(
                f"{prefix}: column added:   {col} ({act_cols[col].get('type')})"
            )

        exp_order = [c["name"] for c in et.get("columns", [])]
        act_order = [c["name"] for c in at.get("columns", []) if c["name"] in exp_cols]
        if exp_order != act_order:
            lines.append(f"{prefix}: column order changed")

        for col in sorted(exp_cols.keys() & act_cols.keys()):
            ec = exp_cols[col]
            ac = act_cols[col]
            col_prefix = f"{prefix}.{col}"
            for field in (
                "type",
                "not_null",
                "auto_increment",
                "has_default",
                "generated",
            ):
                ev = ec.get(field, False)
                av = ac.get(field, False)
                if ev != av:
                    lines.append(f"{col_prefix}: {field} {ev!r} -> {av!r}")

    return lines


def run_test(name: str, *, update: bool) -> bool:
    """Run a single test. Returns True if passed (or skipped)."""
    sql_file = SCRIPT_DIR / f"{name}.sql"
    json_file = SCRIPT_DIR / f"{name}.json"
    dialect = DIALECT.get(name, DEFAULT_DIALECT)

    if not sql_file.exists():
        print(f"[{name}] SKIP - {sql_file.name} not found")
        return True

    exit_code, output = run_type_test(sql_file, dialect)

    if update:
        if exit_code != 0:
            print(f"[{name}] SKIP update - type_test exited with {exit_code}")
            return False
        json_file.write_text(output + "\n")
        print(f"[{name}] updated {json_file.name}")
        return True

    # ---- test mode ----
    if not json_file.exists():
        # For known-failing tests without an expected file, a non-zero exit is expected
        print(f"[{name}] SKIP - no expected output (run --update to create)")
        print(output)
        return True

    expected_text = json_file.read_text().strip()

    if exit_code != 0:
        print(f"[{name}] FAIL - type_test exited {exit_code}")
        try:
            issues = json.loads(output)
            for issue in issues[:5]:
                print(
                    f"  {issue['level']}: {issue['message']} (at {issue['start']}..{issue['end']})"
                )
            if len(issues) > 5:
                print(f"  ... and {len(issues) - 5} more issue(s)")
        except json.JSONDecodeError:
            print(f"  (raw output): {output[:300]}")
        return False

    try:
        actual = json.loads(output)
        expected = json.loads(expected_text)
    except json.JSONDecodeError as e:
        print(f"[{name}] FAIL - JSON parse error: {e}")
        return False

    if actual == expected:
        print(f"[{name}] ok")
        return True

    changes = diff_tables(expected, actual)
    print(f"[{name}] FAIL - schema changed:")
    for line in changes:
        print(line)
    if not changes:
        print("  (unknown difference - re-run with --update to inspect)")
    return False


def discover_tests() -> list[str]:
    names = [sql.stem for sql in sorted(SCRIPT_DIR.glob("*.sql"))]
    return names


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "tests", nargs="*", help="test names to run (default: all passing tests)"
    )
    parser.add_argument(
        "--update",
        action="store_true",
        help="overwrite expected outputs with current output",
    )
    args = parser.parse_args()

    if args.tests:
        names = [Path(t).stem for t in args.tests]
    else:
        names = discover_tests()

    passed = failed = 0
    for name in names:
        if run_test(name, update=args.update):
            passed += 1
        else:
            failed += 1

    if not args.update:
        total = passed + failed
        print(f"\n{passed}/{total} passed" + (f", {failed} failed" if failed else ""))
        if failed:
            sys.exit(1)


if __name__ == "__main__":
    main()
