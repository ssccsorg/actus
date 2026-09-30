# The Browser Agent: a fill-only executor

## Purpose

This devlog records issue #37: the `browser` agent kind that attaches to the
adapter seam of issue #36, and the argument for the policy it enforces. The
work is on its own branch (`37-browser-agent`) because it is a new feature
line, and it is the first adapter whose subject is not a language model
session.

## What a turn is

A turn is a plan: JSON steps drawn from the operations the adapter
implements.

```json
{"steps": [
  {"op": "open", "url": "https://example.com/apply"},
  {"op": "snapshot", "interactive": true},
  {"op": "fill", "target": "#name", "value": "..."},
  {"op": "get_attr", "target": "#bio", "name": "maxlength"}
]}
```

Every step is one `agent-browser` invocation, so the CLI's daemon owns the
browser and the adapter stays a wrapper around argv. The reply is a record,
one entry per step, with what the step printed:

```json
{"ok": true, "steps": [{"op": "fill", "ok": true, "detail": "✓ Done"}]}
```

## The policy is the vocabulary

The requirement is that the agent fills and a person submits. A prompt
instruction cannot carry that: a model that decides to click still can. So
the adapter implements no click, no key press, no JavaScript, and no submit
anywhere in it. `Step` is an enum whose variants are `open`, `snapshot`,
`fill`, `get_attr`, `get_text`, `get_value`, `get_title`, `get_url`,
`screenshot`, `tabs`, `wait`, and `close`; a plan naming anything else fails
to parse before a process starts, and the refusal names the vocabulary. The
`ops` option can only narrow that set, and a config that names an
unimplemented operation fails the load. There is no path from a
configuration to a click.

The DOM-level guard (an `init_script` that intercepts submit events) is
defense in depth, not the policy, and it stays optional because the policy
does not depend on the page.

## The declaration

```toml
[[agents]]
name = "preview"
kind = "browser"
bin = "agent-browser"
cdp = "9222"            # attach to a Chrome a person started, or omit to launch
namespace = "preview"   # isolate daemon sockets from other fleets on the host
headed = true           # show the window
profile = "Default"     # Chrome profile to reuse login state from
session = "preview"     # isolates this agent's browser; defaults to the name
init_script = "/path/to/guard.js"
ops = ["open", "snapshot", "fill"]
env = { AGENT_BROWSER_HEADED = "1" }
timeout_secs = 60
```

`domains` restricts the browser's network, and the CLI refuses it together
with `cdp` or `profile`: containment cannot be installed over a browser the
CLI did not launch, and Chrome may restore existing pages before it is. A
declaration that wants containment names `domains` alone, and the combination
is refused at load with that reason. A key the adapter does not declare is a
load error too, because a misspelled `ops` would drop the narrowing without
saying so.

## What the record promises

The record is what a person reads before submitting, so it says what happened
rather than what was asked. The step loop observes the process before it reads
the cancel flag, so a step that already finished is recorded with its own
outcome even when a cancel arrives in the same instant; a step stopped after
it started is recorded as stopped, with a detail saying the page state at that
step is unknown, because dropping it would hide a field that may hold a
half-typed value. A refused plan, or a failed step, names its reason in the
record's `error` or in the step's `detail`.

## Plan rules

Beyond the vocabulary, a plan is held to what the CLI can express safely:

- A positional that begins with `-` is refused before a process starts. The
  CLI has no `--` separator, and a dash-leading fill value was measured to be
  consumed as an option, leaving the field empty while the command exits 0. A
  value that begins with `-` therefore cannot be entered through this CLI at
  all, and the refusal says so.
- A screenshot path must be relative, stay inside the agent's workdir, and end
  in an image extension, because the CLI reads a single positional without one
  as a selector. It is passed on as an absolute path, because the CLI's daemon
  resolves a relative one against its own working directory rather than the
  adapter's.
- At most 64 steps, a `wait` of at most 30 seconds, and a plan budget of the
  smaller of the per-step timeout times the step count and 600 seconds.

## Session model

One browser per agent. Turns run one at a time (the lock is taken before the
plan runs) and the page survives between turns. A `cancel` sets the turn's
flag; the step loop that owns the process reads it, kills the process group,
and reaps it, so a turn waiting for the browser never starts a step and a plan
stops at a step boundary rather than at its end. The adapter holds threads in
memory, like the other auxiliary kinds; the browser's own state is the CLI's
business.

## Verification

Thirty-one tests in `tests/browser_test.rs` run a stub CLI that records the
argv, the pid, and the working directory of every invocation, and that can be
told to sleep on or fail on an operation. They pin the argv of all twelve
implemented operations, the directories, the plan and step ceilings, the dash
and screenshot rules, a failing step, a timeout that leaves no process, two
turns on one thread serializing, a cancel of a step in flight, of a queued
turn, and after completion, the configuration refusals, and that the published
operation names and the parser's variants are the same set.

Live, through the real `actus` binary with `agent-browser` 0.38.1 and its own
Chrome, in an isolated namespace, profile, and port (no other browser on the
machine touched):

- a plan of `open` (a local file), `snapshot`, `fill`, `get_value`,
  `get_attr`, and `screenshot` returned `ok` with every step recorded;
- `get_value` returned the filled text and `get_attr maxlength` returned the
  field's own limit, so the fill landed where the plan said;
- the screenshot landed inside the agent's workdir, at the absolute path the
  adapter built;
- a `click` plan was refused with the vocabulary named, and the page was never
  submitted (its title stayed empty, which the form would have set);
- a plan whose fill value began with `-` was refused with no step run, which
  is the case that would otherwise have typed nothing and still reported
  success;
- the next turn read the earlier turn's fill, so the session survives.

One operational note the live runs surfaced: a Chrome profile is exclusive.
A browser killed without closing leaves a `SingletonLock`, and the next
launch of that profile fails with `Chrome exited early ... Failed to create
... SingletonLock`. The CLI reports it verbatim, which is what a deployment
sees; the fix is to let the CLI own the profile (or close the browser).

## Contract Pins

- `src/agent/browser.rs`: `IMPLEMENTED_OPS`, `Step`, `parse_plan`,
  `TurnRunner` (argv, plan checks, step execution, cancel), `BrowserAgent`,
  `BrowserFactory`.
- `tests/browser_test.rs`: the stub-driven contract above.
- `src/main.rs`: the `BrowserFactory` registration.
- `docs/devlogs/2026-09-30-browser-agent-review.md`: the review record for the
  first commit of the kind, with the CLI measurements that decided the plan
  rules and the findings that were deferred.

## Remaining Work

- The human review step in the flow: today the record is the review surface,
  and a person submits in the window. An explicit approve step (the ask-mode
  bridge that telos has) is not wired for this kind.
- `select` for dropdown fields is a value-setting operation and is not in the
  first cut; it can be added as one variant when a real form needs it.
- A DOM-level guard script worth shipping (a submit interceptor), and where
  it lives.
- Per-destination gating: `domains` restricts the browser's network, while
  the control policy is per agent.
- How telos dispatches a plan through the control surface in practice: the
  plan is a message, and the record is the reply, but no live telos run has
  been made yet.
- The fabric's turn counting: `chat_stream` waits for `turn_completed` to
  exceed the count it read at submit, so a turn queued behind another on the
  same thread can be credited with the earlier turn's completion. The fix
  belongs in `src/server.rs` and touches every kind, so it is tracked here
  rather than fixed on this branch (see the review record, finding 7).
