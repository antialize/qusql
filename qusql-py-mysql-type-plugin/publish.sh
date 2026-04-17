#!/bin/bash
set -e

# Build manylinux-compatible wheels using the official maturin container.
# Path dependencies are mounted so cargo can resolve them inside the container.
rm -rf target/wheels
podman pull ghcr.io/pyo3/maturin
podman run --rm \
    -v $(pwd):/io \
    -v $(pwd)/../qusql-type:/qusql-type \
    -v $(pwd)/../qusql-parse:/qusql-parse \
    ghcr.io/pyo3/maturin build --release --find-interpreter

# Upload the built wheels to PyPI.
# Read the token from ~/.pypirc if UV_PUBLISH_TOKEN is not already set.
TOKEN="${UV_PUBLISH_TOKEN:-$(sed -nre 's/^\s*password\s*=\s*//p' ~/.pypirc | tr -d '[:space:]')}"
uv publish --token "$TOKEN" target/wheels/*
