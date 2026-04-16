#!/bin/bash
set -e

# Build manylinux-compatible wheels using the official maturin container.
# Path dependencies are mounted so cargo can resolve them inside the container.
podman pull ghcr.io/pyo3/maturin
podman run --rm \
    -v $(pwd):/io \
    -v $(pwd)/../qusql-type:/qusql-type \
    -v $(pwd)/../qusql-parse:/qusql-parse \
    ghcr.io/pyo3/maturin build --release --find-interpreter

# Upload the built wheels to PyPI.
# Credentials are read from ~/.pypirc or the UV_PUBLISH_TOKEN env var.
uv publish target/wheels/*
