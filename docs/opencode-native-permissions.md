# OpenCode native permission and completion boundary

This change implements part of W0/W1 in the 2026-09-25 AgentDesk 24/7 v5
deployment specification. It uses the existing OpenCode provider, warm server
pool, session API, cancellation token, and stream messages. It does not add a
scheduler, Campaign executor, or another model client.

It is not a complete 24/7 activation release. The first Linux read/edit template
connects durable unknown leases, attempt budgets, and filesystem restrictions.
Protected checks, conditional task completion, and Campaign file delivery now
have fixture integration evidence in the linked validation report. Authenticated
backend settlement, remaining isolation boundaries, and real-model operational
acceptance still require separate evidence.

## Explicit tool allowlists

An explicit `allowed_tools` list now becomes native session permissions before
AgentDesk sends a prompt. Managed execution requires the probed OpenCode
**1.14.41**. Ordinary execution verifies the native API contract and session
binding without inheriting that managed version pin. The permission schema is
V1 `permission` rules with `permission`, `pattern`, and `action` fields.

For example, a read-oriented profile can request:

```json
{"allowed_tools": ["read", "glob", "grep"]}
```

AgentDesk reads the actual default agent and its resolved native permissions.
It starts the session mask with a deny-all rule, then projects that agent's
ordered rules onto the requested categories. Existing native path/command
patterns and `deny`/`ask` decisions retain their meaning. An AgentDesk allowlist
cannot turn a profile-level deny into allow. The prompt binds the same agent
whose permissions were inspected.

For ordinary execution, explicit native `external_directory` and `doom_loop`
rules are retained in their original order. These controls gate permitted tools; they are not tool
names that an `allowed_tools` entry can enable. Dropping them would replace an
operator's scoped worktree permission with the session's deny-all rule. The
adapter preserves native `allow`, `ask`, and `deny` patterns without expanding
their paths, and an empty tool allowlist still enables no tool. Ambiguous
category globs overlapping these controls fail closed, just as overlapping tool
category globs do.
Later global `deny` and `ask` rules also constrain these controls in native
order. Global `allow` never adds an auxiliary grant or removes an earlier
auxiliary restriction.

Managed v1 keeps these auxiliary controls denied. Its pre-authorized native
read/edit contract does not inherit ordinary execution's auxiliary grants.
The managed adapter's real native budget/deadline test verifies this distinction.

The session response must contain the exact requested rules. A missing or
changed permission binding, an unsupported managed version, unknown category glob, or
ambiguous tool name fails before prompt dispatch. There is no advisory-only
fallback for an explicit allowlist. `ask` is never converted into automatic
approval by this adapter.

Supported categories are `bash`, `read`, `edit`, `glob`, `grep`, `list`, `task`,
`webfetch`, `websearch`, `skill`, `todowrite`, `todoread`, `question`, and `lsp`.
The equivalent aliases `Bash`, `Read`, `Glob`, `Grep`, `Task`, `WebFetch`,
`WebSearch`, and `Skill` are accepted. Individual Claude grants such as `Write`,
`Edit`, command-scoped `Bash(...)`, and arbitrary MCP names are rejected. In
particular, an individual write grant is not silently expanded to OpenCode's
broader native `edit` category.

OpenCode settings with an omitted tool list use native category defaults.
Previously stored explicit Claude tool lists require an intentional migration
to OpenCode categories before activation. Other providers keep their defaults.
Calls without an explicit adapter allowlist continue to use the provider's own
configuration; those calls are not evidence of a managed permission boundary.
Some callers normalize an empty list to absence, so an empty application list
must not be used as a deny-all policy.

This is a native tool boundary, not an OS or network sandbox. In particular,
allowing unrestricted `bash` permits shell effects beyond native edit tools.
Protected credentials, task-writable config/plugins, subprocess limits, and
local-only inference admission need the remaining managed execution controls.

## Warm runtime identity

Warm pool reuse includes the complete resolved provider overlay: profile,
provider, environment overrides/removals, and configured home, together with
the binary path and canonical workspace. Editing environment settings under
the same profile ID no longer reuses the old process. Credential values are
not included in diagnostic output or diagnostic hashes.

The managed template additionally binds a protected configuration snapshot,
workspace UID/GID, and binary metadata. Ordinary warm reuse does not inspect
configuration file contents at an unchanged path. Neither key proves that an
older active writer drained or that an in-place binary replacement is safe.
Do not treat a fresh pool key as quiescence of the old key.

## Incomplete streams

A stream EOF without the matching session's terminal event returns an error.
Accumulated or recovered text is preserved as text, including apparently valid
success JSON, but cannot generate a `Done` message. An abort request cannot
promote that result into a completed turn. A normal matching `session.idle`
still produces the existing completion event.

The error explicitly reports unknown execution state. It does not prove that
server-side tools stopped. Durable quarantine/admission and process-tree
reconciliation are still required before unattended retries can be enabled.

## Reproducible native probe

```powershell
python scripts/probe_opencode_native.py `
  --binary <approved-opencode-executable> `
  --expected-version 1.14.41 `
  --output target/24x7-native/opencode-native-probe.json
```

The probe supplies a disposable home/config and a loopback model fixture. It
checks real native read execution, denial of a model-requested shell tool,
preservation of a profile deny, a positive shell execution control, and HTTP
400 for an invalid permission action. The positive shell control writes only
inside the disposable fixture workspace. It invokes no real model and does not
load production provider credentials or change the installed service.

Rust regressions exercise the actual adapter's permission preparation,
rejection before prompt dispatch, warm identity, and EOF/cancellation behavior:

```powershell
$env:CARGO_PROFILE_DEV_DEBUG = '0'
$env:CARGO_PROFILE_TEST_DEBUG = '0'
$env:CARGO_BUILD_JOBS = '1'
python scripts/build_token.py cargo test --locked --lib services::opencode:: -- --test-threads=1
```

Use the repository-pinned Rust toolchain and a matching native build environment.
The debug settings match CI and reduce compiler/linker memory use. A successful
contract probe is distinct from an AgentDesk build, a deployed bundle identity,
real model execution, and the E1/E4/E5 or 24-hour operational journeys.

## Native steps is not a hard limit in the probed version

The installed 1.14.41 binary reports `steps: 3` for the selected agent, but a
deliberately repeating loopback model completed **eight native read calls** and
made a **ninth main-model request**. A separate auxiliary model served the title;
its one request is excluded from the main-model count. The ninth request was
rejected by the fixture's independent safety ceiling, and OpenCode entered a
retry state. This demonstrates a missing hard bound, not a successful E4 test.

Reproduce the failure without a real model or production configuration:

```powershell
python scripts/probe_opencode_steps.py `
  --binary <approved-opencode-executable> `
  --expected-version 1.14.41 `
  --output target/24x7-native/opencode-steps-probe.json
$LASTEXITCODE # 2 when the hard-limit acceptance fails
```

The independent experiment limits are eight successful main-model responses and
a 30-second observation window. The script stops only its own disposable server.
An acknowledged abort remains a request observation, not writer quiescence.

Do not enable unattended managed execution using `steps` as its only bound.
W1 must enforce the approved attempt deadline and tool budget, retain admission
protection when execution is unknown, and prove recovery with live fixtures.
The existing timeout notification, expiring capacity lease, and stream return
are insufficient substitutes for these postconditions.

The [validation report](24x7-native-v5/VALIDATION.md),
[deployment plan](24x7-native-v5/DEPLOYMENT-PLAN.md), and
[runbook](24x7-native-v5/RUNBOOK.md) distinguish implemented behavior from the
remaining activation gates.
