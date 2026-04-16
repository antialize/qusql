#!/bin/bash
podman pull ghcr.io/pyo3/maturin
podman run  --rm -it -v $(pwd):/io -v $(pwd)/../qusql-type:/qusql-type -v $(pwd)/../qusql-parse:/qusql-parse -v ~/.pypirc:/root/.pypirc ghcr.io/pyo3/maturin publish --find-interpreter
