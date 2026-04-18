#!/bin/bash
set -e

# Build wheel and sdist.
rm -rf dist
uv build

# Upload to PyPI.
# Read the token from ~/.pypirc if UV_PUBLISH_TOKEN is not already set.
TOKEN="${UV_PUBLISH_TOKEN:-$(sed -nre 's/^\s*password\s*=\s*//p' ~/.pypirc | tr -d '[:space:]')}"
uv publish --token "$TOKEN" dist/*
