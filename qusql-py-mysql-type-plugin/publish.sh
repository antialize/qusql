#!/bin/bash
podman pull ghcr.io/pyo3/maturin
podman run  --rm -it -v $(pwd):/io -v ~/.pypirc:/root/.pypirc ghcr.io/pyo3/maturin publish
