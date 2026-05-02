#!/usr/bin/env python3
"""
Test runner for type_test schema typing and query typing.

Each schema test case is a pair of:
  <name>.sql   - the SQL schema file
  <name>.json  - the expected JSON schema output (produced by type_test schema)

Each query test case is a triplet of:
  <name>.sql         - the SQL schema file (shared with schema test)
  <name>.queries.sql - SQL queries with `-- query: <name>` separators
  <name>.queries.json - the expected JSON query-type output (produced by type_test queries)

Usage:
  python3 test.py               # run all schema + query tests
  python3 test.py --update      # overwrite expected outputs with current output
  python3 test.py mysql1        # run schema test for mysql1
  python3 test.py mysql1.queries # run query test for mysql1
"""

import argparse
import json
import subprocess
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent.resolve()
WORKSPACE = SCRIPT_DIR.parent

# Map of schema-name -> dialect flag passed to type_test
DIALECT: dict[str, str] = {
    "mysql1": "maria-db",
}
DEFAULT_DIALECT = "postgre-sql"


def run_schema_test_cmd(sql_file: Path, dialect: str) -> tuple[int, str]:
    """Run `type_test schema` and return (exit_code, stdout)."""
    result = subprocess.run(
        [
            "cargo",
            "run",
            "-p",
            "type_test",
            "--",
            "schema",
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


def run_queries_test_cmd(
    schema_file: Path, queries_file: Path, dialect: str
) -> tuple[int, str]:
    """Run `type_test queries` and return (exit_code, stdout)."""
    result = subprocess.run(
        [
            "cargo",
            "run",
            "-p",
            "type_test",
            "--",
            "queries",
            str(schema_file),
            str(queries_file),
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


def diff_query_results(expected: list, actual: list) -> list[str]:
    """Return a concise list of change descriptions between two query-type result lists."""
    lines = []
    exp_map = {q["query"]: q for q in expected}
    act_map = {q["query"]: q for q in actual}

    for name in sorted(exp_map.keys() - act_map.keys()):
        lines.append(f"  - query removed: {name}")
    for name in sorted(act_map.keys() - exp_map.keys()):
        lines.append(f"  + query added:   {name}")

    for name in sorted(exp_map.keys() & act_map.keys()):
        eq = exp_map[name]
        aq = act_map[name]
        prefix = f"  query {name!r}"

        if eq.get("kind") != aq.get("kind"):
            lines.append(f"{prefix}: kind {eq.get('kind')!r} -> {aq.get('kind')!r}")

        # Compare columns
        exp_cols = eq.get("columns") or []
        act_cols = aq.get("columns") or []
        if len(exp_cols) != len(act_cols):
            lines.append(f"{prefix}: columns count {len(exp_cols)} -> {len(act_cols)}")
        else:
            for i, (ec, ac) in enumerate(zip(exp_cols, act_cols)):
                for field in ("name", "type", "not_null"):
                    ev = ec.get(field)
                    av = ac.get(field)
                    if ev != av:
                        lines.append(f"{prefix} column[{i}]: {field} {ev!r} -> {av!r}")

        # Compare arguments
        exp_args = eq.get("arguments", [])
        act_args = aq.get("arguments", [])
        if len(exp_args) != len(act_args):
            lines.append(
                f"{prefix}: arguments count {len(exp_args)} -> {len(act_args)}"
            )
        else:
            for i, (ea, aa) in enumerate(zip(exp_args, act_args)):
                for field in ("index", "name", "type", "not_null"):
                    ev = ea.get(field)
                    av = aa.get(field)
                    if ev != av:
                        lines.append(
                            f"{prefix} argument[{i}]: {field} {ev!r} -> {av!r}"
                        )

    return lines


def run_schema_test(name: str, *, update: bool) -> bool:
    """Run a single schema test. Returns True if passed (or skipped)."""
    sql_file = SCRIPT_DIR / f"{name}.sql"
    json_file = SCRIPT_DIR / f"{name}.json"
    dialect = DIALECT.get(name, DEFAULT_DIALECT)

    if not sql_file.exists():
        print(f"[{name}] SKIP - {sql_file.name} not found")
        return True

    exit_code, output = run_schema_test_cmd(sql_file, dialect)

    if update:
        if exit_code != 0:
            print(f"[{name}] SKIP update - type_test exited with {exit_code}")
            return False
        json_file.write_text(output + "\n")
        print(f"[{name}] updated {json_file.name}")
        return True

    # ---- test mode ----
    if not json_file.exists():
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


def run_queries_test(base_name: str, *, update: bool) -> bool:
    """Run a single query-typing test. Returns True if passed (or skipped)."""
    schema_file = SCRIPT_DIR / f"{base_name}.sql"
    queries_file = SCRIPT_DIR / f"{base_name}.queries.sql"
    json_file = SCRIPT_DIR / f"{base_name}.queries.json"
    dialect = DIALECT.get(base_name, DEFAULT_DIALECT)
    label = f"{base_name}.queries"

    if not schema_file.exists():
        print(f"[{label}] SKIP - {schema_file.name} not found")
        return True
    if not queries_file.exists():
        print(f"[{label}] SKIP - {queries_file.name} not found")
        return True

    exit_code, output = run_queries_test_cmd(schema_file, queries_file, dialect)

    if update:
        if exit_code != 0:
            print(f"[{label}] SKIP update - type_test exited with {exit_code}")
            return False
        json_file.write_text(output + "\n")
        print(f"[{label}] updated {json_file.name}")
        return True

    # ---- test mode ----
    if not json_file.exists():
        print(f"[{label}] SKIP - no expected output (run --update to create)")
        print(output)
        return True

    expected_text = json_file.read_text().strip()

    if exit_code != 0:
        print(f"[{label}] FAIL - type_test exited {exit_code}")
        print(f"  (raw output): {output[:500]}")
        return False

    try:
        actual = json.loads(output)
        expected = json.loads(expected_text)
    except json.JSONDecodeError as e:
        print(f"[{label}] FAIL - JSON parse error: {e}")
        return False

    if actual == expected:
        print(f"[{label}] ok")
        return True

    changes = diff_query_results(expected, actual)
    print(f"[{label}] FAIL - query types changed:")
    for line in changes:
        print(line)
    if not changes:
        print("  (unknown difference - re-run with --update to inspect)")
    return False


def discover_tests() -> list[str]:
    """Return all schema base names (from *.sql files, excluding *.queries.sql)."""
    names = [
        sql.stem
        for sql in sorted(SCRIPT_DIR.glob("*.sql"))
        if not sql.stem.endswith(".queries")
    ]
    return names


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "tests",
        nargs="*",
        help="test names to run (default: all tests); append '.queries' to run only query tests",
    )
    parser.add_argument(
        "--update",
        action="store_true",
        help="overwrite expected outputs with current output",
    )
    args = parser.parse_args()

    if args.tests:
        requested = [Path(t).stem for t in args.tests]
    else:
        requested = None

    base_names = discover_tests()

    # Resolve the list of (kind, base_name) pairs to run.
    # kind is "schema" or "queries".
    to_run: list[tuple[str, str]] = []
    if requested is not None:
        for name in requested:
            if name.endswith(".queries"):
                to_run.append(("queries", name[: -len(".queries")]))
            else:
                to_run.append(("schema", name))
                # Also run the corresponding queries test if the file exists.
                if (SCRIPT_DIR / f"{name}.queries.sql").exists():
                    to_run.append(("queries", name))
    else:
        for name in base_names:
            to_run.append(("schema", name))
            if (SCRIPT_DIR / f"{name}.queries.sql").exists():
                to_run.append(("queries", name))

    passed = failed = 0
    for kind, name in to_run:
        if kind == "schema":
            ok = run_schema_test(name, update=args.update)
        else:
            ok = run_queries_test(name, update=args.update)
        if ok:
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
