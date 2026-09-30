# The browser adapter, reviewed

## Purpose

This is the review record for the first browser commit on `37-browser-agent`
(`4555e7e`), written before the remaining work on the kind. A review pass over
that revision raised ten ranked findings and a set of missing tests. Each
finding is accepted, rejected, or deferred here with its evidence, and what was
accepted is in the commits that follow this record.

Two findings could only be settled by measuring the CLI rather than by reading
the adapter, so the measurements come first. They are facts about
`agent-browser` 0.38.1, taken in an isolated namespace and profile with no
other browser on the machine touched.

## What the CLI does

| Invocation | Observed |
|---|---|
| `fill "#n" "--json"` | Exits 0, prints a success-shaped JSON body, and the field is left empty. The value was consumed as an option, and the fill did not happen. |
| `fill "#nope" "x"` | Exits 1, `Element not found`. A missing selector is loud. |
| `fill "--nope" "x"` | Exits 1, `Element not found: --nope`. A dash-leading target fails as an unmatched selector. |
| `screenshot shots/a.png` | Written to `shots/a.png`, resolved by the daemon. |
| `screenshot bareword` | Treated as a selector, `Element not found`, exit 1. |
| `screenshot "#n"` | Treated as a selector that matches, so the image went to the CLI's default directory. |
| The same session taking `screenshot rel.png` from another working directory | The file landed in the directory of the process that started the daemon, not the caller's. |
| `--headed false` | The documented boolean form (`agent-browser --help`, Configuration), so the adapter's spelling is right. |
| `--allowed-domains` with `--cdp` or `--profile` | Refused by the CLI: containment cannot be installed over a browser it did not launch. |
| `--namespace <name>` | Exists, and isolates daemon sockets. The adapter did not pass it and no option could ask for it. |

Two consequences follow. A dash-leading value is the worst kind of failure
here: the command succeeds and nothing is typed, so the record would have told
a person a field was filled when it was empty. And a screenshot path is
resolved by the daemon, so `current_dir(workdir)` on the spawn does not confine
it at all.

## Findings and decisions

| # | Finding | Decision |
|---|---|---|
| 1 | `run_step` read the cancel flag after observing the child's exit, so a step that had already finished could be reported as cancelled and dropped from the record. | Accepted. The poll loop observes the process before the flag, and a step stopped after it started is recorded, with a detail saying the page state at that step is unknown. |
| 2 | `wait_with_output` resolves on pipe EOF rather than child exit, so a descendant holding a pipe could delay a cancel or a timeout, or cause a false timeout. | Accepted. Steps now use a poll loop over the child plus two reader tasks, as `ext_cli` does, and the readers are aborted when the step is killed. |
| 3 | A raw `libc::kill` on a published pid can name a recycled process. | Accepted. The map holds the `Child` handle, not a pid, and the group signal is sent while the process is still unreaped. |
| 4 | No `deny_unknown_fields`, so a misspelled `ops` silently dropped the narrowing and an unknown step field parsed and was ignored. | Accepted, on the options table, the plan, and the step. |
| 5 | Plan strings reached the CLI parser unvalidated, and `screenshot.path` was unconfined. | Accepted. A dash-leading positional is refused in every field that becomes one; a screenshot path must be relative, confined to the workdir, and carry an image extension; it is passed on as an absolute path. |
| 6 | No step ceiling, no plan deadline, no aggregate record cap. | Accepted in part. Step ceiling of 64 and a plan budget of the smaller of per-step times steps and 600 seconds. The separate aggregate cap is rejected: the two ceilings bound the record at 256 KB, the same order as the 200 KB single-message cap `ext_cli` already accepts. |
| 7 | A second turn queued on one thread can be credited with the first turn's completion. | True, and it belongs to the fabric. `chat_stream` in `src/server.rs` waits for `turn_completed` to exceed the count it read at submit, so the fix is to key the wait on the reply's request id. That changes behavior for every adapter, so it is not changed here. |
| 8 | `cancel_request` answered `Ok` for an unknown id, and `resolve_tool_call` answered `Ok` with no tool surface. | Accepted, both. The fabric surfaces an `Err` as `{"status":"error"}`, so an id this agent never saw is now an error, an id whose reply is on the thread stays a no-op, and resolving a tool call on an adapter with no tool surface is an error. |
| 9 | `bin_present` resolved a relative `bin` against the server directory while the spawn used the workdir; the probe blocked for ten seconds and leaked the child on its error path. | Accepted. The probe runs from the workdir and with the declaration's environment, its window is two seconds, and its error path kills and reaps. |
| 10 | An unresolved `$NAME` in `env` resolved to an empty string. | Accepted for this adapter: the config load refuses a name the server environment does not carry. `ext_cli` keeps the old behavior, because changing it would change an agent that runs today. |

Style items from the same pass are also in: the signal number is named in a
non-zero exit that came from a signal, the `running` lock is not held across
the kill, the serialization fallback is an error record rather than `{}`, and
`session.parent` is recorded from the dispatch, as `ext_cli` does.

## Deferred, with reasons

- The fabric's turn counting (finding 7). It is a change to
  `src/server.rs` that touches every agent kind, and the browser adapter cannot
  fix it locally.
- `ext_cli`'s `resolve_env` and its empty `$NAME`. The same reasoning as
  finding 10: it would change behavior for agents that run today, so it is a
  follow-up of its own rather than a passenger on this branch. `ext_cli`
  already has a synchronous kill, so findings 1 to 3 do not apply to it.
- The CLI's own `--action-policy` and `--confirm-actions`, which would let a
  deployment block actions at the daemon rather than at the argv vocabulary.
  The adapter's vocabulary is already the policy, so this is hardening a
  deployment may add, not a gap in the kind.
- A value that begins with `-` cannot be entered through this CLI at all. The
  refusal is the honest answer, because the alternative is the silent no-op
  measured above, and the CLI offers no separator that would make it
  expressible.

## Tests added

Thirty-one tests in `tests/browser_test.rs`, from eleven before. The stub now
records the pid and the working directory of each invocation, and can be told
to sleep on or fail on an operation. The additions pin the argv of all twelve
operations, the working directory of the probe and the steps, the step and plan
ceilings, an empty plan, an empty operation set, the dash rule, the screenshot
path rule, an unknown step field, a failing step, a timeout that leaves no
process, two turns on one thread serializing, a cancel of a step in flight, a
cancel of a queued turn, a cancel after completion, a cancel of an unknown id,
the empty tool surface, the two configuration refusals, and that the published
operation names and the parser's variants are the same set.

## Verification

- `cargo test`: 172 tests, all passing, of which 31 are the browser contract.
- `cargo clippy --all-targets`: no warnings in the files this branch touches.
  Two warnings remain in `src/telos/mod.rs` and `tests/ext_cli_test.rs` from
  before the branch.
- Live, through the real `actus` binary and `agent-browser` 0.38.1, in an
  isolated namespace, profile, and port: a plan of open, snapshot, fill,
  `get_value`, `get_attr`, and `screenshot` returned `ok` with every step
  recorded, `get_value` returned the filled text, `get_attr` returned the
  field's own `maxlength`, the screenshot landed inside the agent's workdir,
  and a dash-leading fill value was refused with no step run.
- Not verified: the same live plan through a telos dispatch over the control
  surface, which is still on the remaining-work list of the kind.

## One environment note

`cargo fmt --check` reports diffs in thirteen files this branch does not
touch, because the tree was formatted by an older rustfmt than the one on this
host (`rustfmt 1.9.0-stable`, 2026-04-14). The two files this branch changes
are clean under the local formatter, and the unrelated files were left as they
are.
