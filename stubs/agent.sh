#!/bin/sh
#
# A stand-in for the agent program a development run launches.
#
# actus runs it once per turn with the prompt where the agent's `cli_args` places it, and
# records what it prints. It is a transport and not a product: nothing here answers from a
# model, so a run needs no key, no network and no cost. Naming it as the default is what
# makes `run.sh` work with no arguments, and a run that names its own program with `--bin`
# (or ACTUS_EXECUTOR_BIN) replaces it.
#
# The kind is `ext_cli`, which is what this crate's binary can start: it needs nothing of
# the program beyond its command line. A sessionful executor is the kind `acpws`, and
# starting one takes a deployment that renders the settings format that executor reads,
# which is the executor's own and not this repository's.

set -eu

prompt=${1:-}
if [ -z "$prompt" ]; then
    prompt=$(cat)
fi

printf '[stub agent] received: %s\n' "$prompt"
