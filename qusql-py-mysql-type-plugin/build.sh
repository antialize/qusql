#!/bin/bash
set -euo pipefail
# TODO: i686 build fails to run 'cargo' with a strange error. Only build x86_64 for now.
# for arch in x86_64 i686; do
for arch in x86_64; do
	for pyver in 37 38 39 310; do
		if [ "$pyver" == "37" ]; then
			pym=m
		else
			pym=
		fi
		docker build --build-arg builder=quay.io/pypa/manylinux2014_$arch --build-arg pyver=$pyver --build-arg pym=$pym -t localhost/python-statfs:latest-$arch-$pyver .
		docker run localhost/python-statfs:latest-$arch-$pyver tar c dist | tar x dist
	done
done
