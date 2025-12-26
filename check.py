#!/usr/bin/python3

"""
Checks that all rust examples in markdown files are also present in rust files. This makes sure that they compile.
"""

import os.path
import re
import sys
from concurrent.futures import ThreadPoolExecutor

rust_files: list[str] = []
md_files: list[str] = []

for root, dirs, files in os.walk("."):
    for file in files:
        if file.endswith(".rs"):
            rust_files.append(os.path.join(root, file))
        elif file.endswith(".md"):
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
        + (
            "\\n".join(
                [
                    "//[!/][ ]?" + re.escape(line)
                    for line in e.split("\n")
                ]
            )
        )
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
