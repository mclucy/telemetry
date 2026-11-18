#!/usr/bin/env bash
set -euo pipefail

# Read the resolved version of the `worker` crate from Cargo's own metadata so
# the build tool stays in lockstep without a separate version pin.
version=$(
  cargo metadata --format-version 1 --locked \
    | jq -r '. as $m | $m.resolve.nodes[] | select(.id == $m.resolve.root) | .deps[] | select(.name == "worker") | .pkg as $id | $m.packages[] | select(.id == $id) | .version'
)

if ! worker-build --version | grep -qx "$version"; then
  cargo install --locked --version "$version" worker-build
fi
