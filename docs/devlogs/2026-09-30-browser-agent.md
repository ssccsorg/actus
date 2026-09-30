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
cdp = "9222"           # attach to a Chrome a person started, or omit to launch
headed = true          # show the window
profile = "Default"    # Chrome profile to reuse login state from
session = "preview"    # isolates this agent's browser; defaults to the name
init_script = "/path/to/guard.js"
domains = ["example.com"]
ops = ["open", "snapshot", "fill"]
env = { AGENT_BROWSER_HEADED = "1" }
timeout_secs = 60
```

## Session model

One browser per agent. Turns run one at a time (the lock is taken before the
plan runs), the page survives between turns, and a `cancel` sets the turn's
flag and kills the step in flight, so the plan stops at the next boundary
rather than at its end. The adapter holds threads in memory, like the other
auxiliary kinds; the browser's own state is the CLI's business.

## Verification

Unit and integration tests (`tests/browser_test.rs`) run a stub CLI that
records the argv of every invocation: the plan runs each step as one
invocation with the shared globals first, a plan naming `click` never starts
a process, a narrowed set refuses a step outside it, a pause over the ceiling
is refused, a cancel stops the step in flight, and the record carries each
step's output.

Live, with `agent-browser` 0.38.1 and its own Chrome (a temporary profile, no
other browser on the machine touched):

- a plan of `open` (a local file), `snapshot`, two `fill`s, `get_value`, and
  `get_attr` ran through actus end to end against a real page;
- `get_value` returned the filled text and `get_attr maxlength` returned the
  field's own limit (120);
- a `click` plan was refused with the vocabulary named, and the page was
  never submitted (its title stayed empty, which the form would have set);
- the next turn read the earlier turn's fill, so the session survives.

One operational note the live run surfaced: a Chrome profile is exclusive.
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
