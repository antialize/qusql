#!/usr/bin/python3

"""
Checks that all rust examples in markdown files are also present in rust files.
This makes sure that they compile.

Each example must appear either as:
  1. Consecutive doc-comment lines (//! or ///) in a .rs file, or
  2. Consecutive source lines in a .rs file (leading/trailing whitespace stripped).
"""

import os.path
import re
import sys
from concurrent.futures import ThreadPoolExecutor

rust_files: list[str] = []
md_files: list[str] = []

for root, dirs, files in os.walk("."):
    dirs[:] = [d for d in dirs if d != "target"]
    for file in files:
        if file.endswith(".rs"):
            rust_files.append(os.path.join(root, file))
        elif file.endswith(".md") and file != "DEVELOPMENT.md":
            md_files.append(os.path.join(root, file))


# Read all .md files to find rust examples
find_rust_examples = re.compile(r"```rust((`?`?[^`])*)```", re.DOTALL | re.MULTILINE)

rust_examples_dict: dict[str, tuple[str, int]] = {}


def visit_md(p: str) -> None:
    global rust_examples_dict
    with open(p) as f:
        for m in find_rust_examples.finditer(f.read()):
            rust_examples_dict[m.group(1).strip()] = (p, m.start())


with ThreadPoolExecutor() as e:
    for p in md_files:
        e.submit(visit_md, p)

rust_examples: list[tuple[str, int, str]] = [
    (p, off, content) for content, (p, off) in rust_examples_dict.items()
]

rust_examples.sort()

# Read all rust files to se if they contain the rust examples from the md files

find_examples = re.compile(
    "|".join(
        "("
        + ("\\n".join(["//[!/][ ]?" + re.escape(line) for line in e.split("\n")]))
        + ")"
        for (_, _, e) in rust_examples
    )
)

found = [False for _ in rust_examples]


def visit_rust(p: str) -> None:
    global found
    with open(p) as f:
        for m in find_examples.finditer(f.read()):
            for g, v in enumerate(m.groups()):
                if v is not None:
                    found[g] = True


with ThreadPoolExecutor() as e:
    for p in rust_files:
        e.submit(visit_rust, p)

# -- Pass 2: plain source-line match ------------------------------------------
# Build an index: stripped_line -> [(file, line_number)].
# Then for each still-unfound example, check whether all of its lines appear
# consecutively (after stripping leading/trailing whitespace) in a single file.

file_lines: dict[str, list[str]] = {}
source_line_index: dict[str, list[tuple[str, int]]] = {}

for p in rust_files:
    with open(p) as f:
        lines = [ln.rstrip("\n").strip() for ln in f]
    file_lines[p] = lines
    for i, stripped in enumerate(lines):
        source_line_index.setdefault(stripped, []).append((p, i))


def example_in_source(example: str) -> bool:
    ex_lines = [ln.strip() for ln in example.split("\n")]
    if not ex_lines:
        return False
    first = ex_lines[0]
    for file, start in source_line_index.get(first, []):
        flines = file_lines[file]
        n = len(ex_lines)
        if start + n > len(flines):
            continue
        if all(flines[start + j] == ex_lines[j] for j in range(1, n)):
            return True
    return False


for i, (_file, _off, example) in enumerate(rust_examples):
    if not found[i]:
        found[i] = example_in_source(example)

bad = 0

# Complain about all the missing rust examples

for (file, off, example), f in zip(rust_examples, found):
    if f:
        continue
    print(
        f"Rust example from {file} not found:\n{'\n'.join(f'  {line}' for line in example.split('\n'))}\n\n"
    )
    bad += 1

if bad:
    print(f"{bad} missing markdown examples")
    sys.exit(1)
