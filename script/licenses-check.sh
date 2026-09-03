#!/usr/bin/env bash
# Enforces the third-party license policy declared in about.toml.
#
# Runs cargo-about over the actus dependency graph and writes the generated
# output to a temporary file. The --fail flag makes cargo-about exit non-zero
# whenever a dependency license cannot be resolved from the accepted list,
# which fails the CI job. Run locally with:
#
#   cargo install cargo-about --locked
#   script/licenses-check.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
CONFIG_FILE="$PROJECT_DIR/about.toml"

OUTPUT_FILE="$(mktemp)"
trap 'rm -f "$OUTPUT_FILE"' EXIT

cd "$PROJECT_DIR"
# --format json avoids the handlebars template requirement while still
# exercising full license resolution; only the exit code matters here.
cargo about generate --fail --format json -c "$CONFIG_FILE" >"$OUTPUT_FILE"
