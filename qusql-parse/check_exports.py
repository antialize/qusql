#!/usr/bin/env python3
"""
Check that every pub struct defined in qusql-parse/src is re-exported in lib.rs.
"""

import re
import sys
from pathlib import Path

SRC = Path(__file__).parent / "src"
LIB_RS = SRC / "lib.rs"

# Backup/merge-conflict suffixes to skip
SKIP_SUFFIXES = ("_BACKUP_", "_BASE_", "_LOCAL_", "_REMOTE_")


def is_backup(path: Path) -> bool:
    return any(part in path.stem for part in SKIP_SUFFIXES)


def collect_pub_structs(src_dir: Path) -> dict[str, list[tuple[Path, int]]]:
    """Return {name: [(file, lineno), ...]} for all pub structs and pub enums found."""
    pattern = re.compile(r"^pub (?:struct|enum) (\w+)")
    result: dict[str, list[tuple[Path, int]]] = {}
    for path in sorted(src_dir.glob("*.rs")):
        if path.name == "lib.rs" or is_backup(path):
            continue
        for lineno, line in enumerate(path.read_text().splitlines(), 1):
            m = pattern.match(line)
            if m:
                name = m.group(1)
                result.setdefault(name, []).append((path, lineno))
    return result


def collect_pub_use_names(lib_rs: Path) -> set[str]:
    """Extract every identifier inside pub use ... ; blocks in lib.rs (handles multi-line)."""
    ident_pattern = re.compile(r"\b([A-Z][A-Za-z0-9_]*)\b")
    names: set[str] = set()
    text = lib_rs.read_text()
    # Collect full text of each pub use ... ; statement (may span multiple lines)
    for block in re.findall(r"\bpub\s+use\s+[^;]+;", text, re.DOTALL):
        names.update(ident_pattern.findall(block))
    return names


def main() -> int:
    structs = collect_pub_structs(SRC)
    exported = collect_pub_use_names(LIB_RS)

    missing: list[tuple[str, Path, int]] = []
    for name, locations in sorted(structs.items()):
        if name not in exported:
            for path, lineno in locations:
                missing.append((name, path, lineno))

    if not missing:
        print(f"OK: all {len(structs)} pub structs/enums are re-exported in lib.rs")
        return 0

    print(f"MISSING from lib.rs pub use ({len(missing)} occurrence(s)):\n")
    for name, path, lineno in missing:
        rel = path.relative_to(SRC.parent.parent)
        print(f"  {name:40s}  {rel}:{lineno}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
