---
doc_type: "spec_source"
schema_version: "2"
status: "approved"
topic_slug: "stream-json-cli-family"
topic_folder: "design"
linked_prd: "./stream-json-cli-family-prd.md"
traceability_mode: "req-task-test"
created_at: "2026-08-17"
updated_at: "2026-08-17"
target_repo: "https://github.com/kunkunGames/AgentDesk"
code_survey_date: "2026-08-17"
survey_head: "feat/scheduled-kakao-fanout @ b933a7c23c6ac5b269d9ceebf334a0f668414cc7"
implementation_readiness: "ready-to-implement-by-slice"
adversarial_review: "2026-08-17-p0-holes-closed"
---

# SPEC: Stream-JSON CLI 패밀리와 Grok·Antigravity(AGY) pipe 착지

## Linked Document

- PRD: [stream-json-cli-family-prd.md](./stream-json-cli-family-prd.md)

## State and Normative Language

- status: `approved`
- implementation order authority: **§10 only**. PRD P0-A/B/C is a product grouping, not a commit order.
- source-of-truth: process runner, dialect/codec/policy 경계, Grok·Antigravity 실행 계약, provider 등록·config·UI 표면, MUST/MUST NOT
- sync policy: PRD 범위가 바뀌면 REQ/TSK/TEST/Traceability를 같은 변경에서 갱신한다.

이 문서에서 `MUST`, `MUST NOT`, `SHOULD`는 구현 계약이다. 현재 코드나 실캡처와 충돌하면 현재 확인 사실을 우선하고 이 문서를 같은 변경에서 고친다. 구현자는 아래 해석 공백을 새로 만들지 않는다. 공백이 보이면 코드를 쓰기 전에 이 문서를 고친다.

---

## 1. Critical Decisions

1. **절단선은 벤더가 아니라 실행 패밀리다.** StreamJsonCli / ClaudeTui / CodexTui / CodexExec / OpenCodeHttp / ManagedTmuxWrapper를 섞지 않는다.
2. **StreamJsonCli 공통화 대상은 process lifecycle이다.** binary resolve, spawn, stdout/stderr transport, watchdog, cancel, child-tree kill, bounded diagnostics만 runner가 소유한다.
3. **argv, prompt transport, system prompt, session, retry, tool policy, codec, finalization은 dialect가 소유한다.** 수동 flag bag으로 유효하지 않은 조합을 만들지 않는다.
4. **codec은 stateful이다.** 모든 provider를 `parse_stream_message` 한 함수에 강제로 맞추지 않는다.
5. **Gemini는 custom Gemini codec, Grok는 MessagesJson codec, Antigravity는 AGY codec을 쓴다.** 세 codec은 같은 runner를 공유한다.
6. **Grok와 Antigravity는 이 spec의 제품 착지 범위다.** Grok가 먼저, Antigravity가 다음 merge slice지만 둘 다 완료 조건과 traceability를 가진다.
7. **Antigravity의 canonical provider id는 `antigravity`다.** 바이너리는 `agy`, 입력 alias는 선택적으로 `agy`, suffix는 기존 `-ag`다. UI에 별도 `agy` provider를 만들지 않는다.
8. **execution family와 provider identity/capabilities를 분리한다.** `ProviderExecutionAdapter::StreamJsonCli(StreamJsonDialectId)`는 provider id나 capability를 소유하지 않는다.
9. **StreamJson 실행 dispatch는 한 곳이다.** Gemini/Grok/Antigravity는 `provider_exec::execute_streaming(ProviderTurnRequest)`만 탄다. Discord headless/intake는 이 세 provider를 이름으로 match하지 않는다. Claude/Codex/OpenCode/Qwen는 기존 함수를 유지한다.
10. **권한 provenance와 변환은 typed/fail-closed다.** provider 기본 surface, 사용자 explicit allowlist, meeting read-only를 구분하고 dialect가 요청을 정확히 표현하지 못하면 spawn하지 않는다.
11. **resume identity는 provider별 validator가 지킨다.** `--continue` 또는 “resume 실패 후 fresh retry”로 Discord channel session을 바꾸지 않는다.
12. **`AgentChannels` named five fields를 제거한다.** map은 unknown key를 보존하고, 신규 write는 canonical supported id만 허용한다.
13. **backend registry가 selectable provider catalog의 권위다.** dashboard의 수동 `CLI_PROVIDERS` 목록은 정본이 아니다.
14. **신규 TUI 설정은 capability validation에서 거절한다.** legacy 설정에만 기존 unavailable fallback과 명확한 reason을 남긴다.
15. **새 DB 컬럼과 migration은 없다.** Claude/Codex legacy 전용 컬럼을 제외한 provider는 configured primary `discord_channel_id` 규칙으로 일반화한다.
16. **frozen giant에 기능 로직을 추가하지 않는다.** touched frozen giant의 production line aggregate는 net-negative여야 하고 root는 얇은 facade/delegation만 남긴다.
17. **실캡처 fixture는 merge gate다.** 문서 예제만으로 Grok/Antigravity capability를 true로 만들지 않는다.
18. **알 수 없는 context window, auth usability, tool capability를 추정값으로 포장하지 않는다.** `unknown`, `installed`, `catalog reachable`, `authenticated smoke passed`를 구분한다.
19. **`counterpart()` 첫 항은 현재 제품 동작이다.** Claude→Codex, Codex→Claude, Gemini→Codex, OpenCode→Codex, Qwen→Codex. 새 provider는 목록 뒤에만 붙는다. 사전순 `all other`는 금지.
20. **Discord settings loader가 typed `ConfiguredToolPolicy`를 올린다.** `DEFAULT_ALLOWED_TOOLS`로 채운 벡터를 Grok/Antigravity dialect에 넘기지 않는다. 이 provenance 없이 Grok/AGY를 켜면 기본 Discord 턴이 fail-closed되거나 권한이 확대된다.
21. **`ProviderTurnRequest`는 StreamJson·공통 필드만 담는다.** Claude/Codex/Qwen extras는 headless giant 안에 새 빌더를 키우지 않고 기존 호출 또는 headless 밖 `LegacyProviderTurnExtras`로만 옮긴다.
22. **Grok는 일반 Discord/headless 턴에서 사용 가능하다.** Antigravity도 일반(`ProviderDefault`) 턴에서 사용 가능하다. meeting/restricted 워크플로는 Grok만 ReadOnly로 넣고, Antigravity는 P0에서 제외한다.

---

## 2. Surveyed Facts

### 2.1 Current repository

| ID | 확인 사실 | Anchor |
|---|---|---|
| L-001 | `ProviderKind`와 `ProviderExecutionAdapter`는 Claude/Codex/Gemini/OpenCode/Qwen + Unsupported에 닫혀 있다 | `src/services/provider.rs` |
| L-002 | execution adapter가 `provider_id()`와 `supported_capabilities()`를 가져 한 adapter가 한 provider라는 불변식이 있다 | `src/services/provider.rs` |
| L-003 | conformance test가 adapter provider id/capability와 registry row의 동등성을 요구한다 | `src/services/provider/provider_conformance_invariant_tests.rs` |
| L-004 | `ProviderKind::from_str`은 registry를 본 뒤 다시 닫힌 id match를 한다 | `src/services/provider.rs` |
| L-005 | counterpart provider 배열은 모든 provider 쌍을 수동 열거한다 | `src/services/provider.rs` |
| L-006 | Discord headless/intake와 `provider_exec`가 provider별 streaming match를 반복한다 | `src/services/discord/router/message_handler/headless_turn.rs`, `src/services/discord/router/message_handler/intake_turn.rs`, `src/services/provider_exec.rs` |
| L-007 | Gemini streaming은 process lifecycle과 Gemini event parsing/finalization이 한 파일에 결합되어 있다 | `src/services/gemini.rs` |
| L-008 | Gemini parser는 `init`, `message`, `tool_use`, `tool_result`, `error`, `result`를 자체 해석한다 | `src/services/gemini.rs` |
| L-009 | `process_stream_line`은 Messages stream state와 multi-block emission을 다루지만 `parse_stream_message`는 한 line convenience 함수다 | `src/services/session_backend/stream_line.rs` |
| L-010 | `AgentChannels`는 5 named fields + 고정 길이 `iter()`다 | `src/config.rs` |
| L-011 | config merge, DB, audit, CRUD, onboarding 등이 AgentChannels named fields를 직접 참조한다 | `src/runtime_layout/config_merge.rs`, `src/db`, `src/server`, `src/services` |
| L-012 | `runtime_layout::normalize_provider_name`은 `claude\|codex\|gemini\|qwen`만 인정하고 **`opencode`도 빠진다** | `src/runtime_layout/mod.rs` |
| L-013 | TUI hosting driver는 Claude/Codex만 있다 | `src/services/provider_hosting.rs` |
| L-014 | dashboard `CliProvider`/wizard에는 runtime에 없는 `copilot`, `antigravity`, `api`가 있다 | `dashboard/src/types/index.ts`, `components/agent-manager/constants.ts` |
| L-015 | `-ag -> antigravity` mapping이 frontend onboarding과 config default map에 이미 있다 | `setupWizardHelpers.ts`, `src/config.rs` |
| L-016 | `config.rs`, `provider.rs`, `gemini.rs`, doctor orchestrator와 Discord turn 파일은 frozen giant다 | `docs/agent-maintenance/change-surfaces.md`, generated inventory |
| L-017 | Discord settings는 생략된 `allowed_tools`를 공용 `DEFAULT_ALLOWED_TOOLS` vector로 materialize하고 headless/intake에 전달해 policy provenance를 잃는다 | `src/services/discord/settings/read.rs` (`unwrap_or_else(default_allowed_tools_for_provider)`), `src/services/agent_protocol.rs` |
| L-018 | `counterpart()`는 배열 첫 원소만 쓴다. 현재 Claude 첫 항은 Codex. meeting/handoff가 이 값에 의존한다 | `src/services/provider.rs` `CLAUDE_COUNTERPARTS` 등, `meeting_orchestrator.rs`, `meeting_cmd.rs`, `dispatch_context.rs` |
| L-019 | meeting readonly는 `["Read"]`만 넘긴다. `is_readonly_tool_policy`는 read/grep/glob 전용일 때만 true | `src/services/discord/meeting_orchestrator/rounds.rs`, `src/services/provider.rs` |
| L-020 | headless/intake는 match 전에 Claude compact/cache TTL, Codex goals, `force_fresh`, `tmux_session_name`을 계산한다 | `headless_turn.rs`, `intake_turn.rs` |

### 2.2 Grok 1.0.4 local contract

| ID | 확인 사실 |
|---|---|
| G-001 | `grok -p <prompt> --output-format streaming-messages-json`은 NDJSON Messages shape를 낸다 |
| G-002 | stream은 `system/init`, `assistant`, `user`, terminal `result`이고 init/result의 placeholder field는 생략될 수 있다 |
| G-003 | assistant/user content에는 복수 text/thinking/tool_use/tool_result block이 들어갈 수 있다 |
| G-004 | `--resume <ID_OR_TITLE>`는 resume, `-s/--session-id`는 새 UUID 생성, `--continue`는 cwd-latest다 |
| G-005 | 자동화는 terminal result의 UUID session id를 저장해 `--resume`으로만 재사용해야 한다 |
| G-006 | `--rules`는 기본 system prompt에 추가되고 `--system-prompt-override`는 기본 prompt와 rules를 대체한다 |
| G-007 | `--tools`는 built-in allowlist지만 always-on MCP meta-tools는 남는다; `--deny MCPTool(*)` 같은 별도 차단이 필요하다 |
| G-008 | `--yolo`는 auto-approve지만 deny rules/admin locks는 유지된다 |
| G-009 | `--no-subagents`, `--disable-web-search`, `--no-memory`, `--no-auto-update`, `--cwd`가 있다 |
| G-010 | auth metadata source는 `~/.grok/auth.json` 또는 `XAI_API_KEY`다 |

### 2.3 AGY 1.1.13 local contract

| ID | 확인 사실 |
|---|---|
| A-001 | 실행 바이너리는 `agy`; Windows canonical fallback은 `%LOCALAPPDATA%\agy\bin\agy.exe`다 |
| A-002 | `-p/--print`, `--output-format stream-json`, `--model`, `--print-timeout`, `--sandbox`, `--disable-slash-commands`가 있다 |
| A-003 | fresh stream은 `event=init`과 UUID-shaped `conversation_id`로 시작한다 |
| A-004 | response delta는 `event=step_update`, `step_type=agent_response`, `text_delta`에 있다 |
| A-005 | terminal `event=result`는 같은 conversation id, status, aggregate response, num_turns, usage를 포함한다 |
| A-006 | `--conversation <id>`로 같은 id를 resume했고 실캡처에서 `num_turns=2`가 됐다 |
| A-007 | `--continue`는 most recent conversation이라 channel identity에 사용할 수 없다 |
| A-008 | child process cwd가 작업 디렉터리다; Grok식 `--cwd`가 없다 |
| A-009 | `--mode plan`과 `--disable-slash-commands`를 함께 쓰면 plan mode가 무효라는 경고가 난다 |
| A-010 | sandboxed capture도 넓은 tool 목록과 `permission_mode=always-proceed`를 보고했다; sandbox만으로 read-only를 증명하지 못한다 |
| A-011 | `step_update.usage`는 step-scoped이고 terminal `result.usage`는 conversation aggregate로 관찰됐다; 둘을 합산하면 중복된다 |
| A-012 | stable credential-file contract는 확인하지 않았고 secret 탐색은 범위 밖이다 |

---

## 3. Target Architecture

### 3.1 Module ownership

Canonical owner modules MUST be small modules below existing facades.

```text
src/services/
  provider.rs                         # thin re-export/facade, net-negative
  provider/
    registry.rs                       # identity, aliases, capabilities, behavior rows
    catalog.rs                        # serializable public catalog projection
  provider_exec.rs                    # one ProviderTurnRequest dispatch
  stream_json_cli/
    mod.rs                            # facade
    request.rs                        # typed request/prepared command
    runner.rs                         # process lifecycle only
    codec.rs                          # stateful codec contract
    policy.rs                         # normalized tool/system/prompt policy types
    session.rs                        # session/retry contracts
    dialects/
      mod.rs
      gemini.rs
      grok.rs
      agy.rs
src/config.rs                         # thin facade, net-negative
src/config/
  agent_channels.rs                  # map newtype + serde/validation
src/server/routes/
  providers_api.rs                   # authenticated provider catalog endpoint
```

Rules:

- Each new production file MUST remain below 1000 production lines.
- `runner.rs` and each dialect SHOULD remain below 700 production lines; exceeding it requires cohesive extraction, not inventory allowlisting.
- `provider.rs`, `gemini.rs`, `config.rs`, doctor orchestrator, headless/intake MUST lose more feature logic than they gain in aggregate.
- New provider logic MUST NOT land in turn bridge, watcher, relay sink, turn finalizer, or TUI trees.

### 3.2 Provider execution binding

`ProviderExecutionAdapter` remains the low-diff public name, but its semantics become execution family.

```text
ProviderExecutionAdapter {
  Claude,                  # existing provider-specific adapter; unchanged in this scope
  Codex,                   # existing provider-specific adapter; unchanged in this scope
  OpenCode,                # existing HTTP-backed adapter; unchanged in this scope
  Qwen,                    # existing managed-wrapper behavior; unchanged in this scope
  StreamJsonCli(StreamJsonDialectId),
}

StreamJsonDialectId {
  Gemini,
  Grok,
  Agy,
}
```

- Gemini registry row MUST bind `StreamJsonCli(Gemini)`.
- Grok registry row MUST bind `StreamJsonCli(Grok)`.
- Antigravity registry row MUST bind `StreamJsonCli(Agy)`.
- Claude/Codex/OpenCode/Qwen adapter internals and their TUI/exec/wrapper sub-routing remain unchanged in this scope.
- execution adapter MUST NOT expose `provider_id()` or provider capabilities.
- provider identity and capabilities MUST come from `ProviderRegistryEntry`.
- compaction/readiness variants MUST be behavioral (`Disabled`, `GenericBanner`) rather than `GrokDisabled`, `AgyDisabled`, `Grok`, `Agy` boilerplate.

### 3.3 ProviderTurnRequest

StreamJson and shared callers MUST construct one typed request and call one dispatcher. This type is **not** a dumping ground for Claude/Codex/Qwen extras.

```text
ProviderTurnRequest {
  provider: ProviderKind,
  prompt: SecretText,                 // repo style: owned String + redaction at log boundary is enough
  system_prompt: Option<SecretText>,
  tool_policy: ConfiguredToolPolicy,  // never a materialized default Vec
  model: Option<String>,
  working_directory: PathBuf,         // trusted/validated before construct
  session: Option<ProviderSessionToken>,  // None = fresh
  remote_profile: Option<RemoteProfile>,
  structured_output: Option<JsonSchemaRequest>,
  timeout: TurnTimeoutPolicy,
  cancel: CancelToken,
}
```

- Discord headless/intake MUST call `provider_exec::execute_streaming(request)` for **Gemini, Grok, and Antigravity** and MUST NOT match those three by name.
- Claude, Codex, OpenCode, and Qwen MAY keep their existing `execute_command_streaming(...)` signatures in this scope. If extras move, they MUST move to `src/services/provider_exec/legacy_extras.rs` (or equivalent) **outside** the headless/intake giant files. MUST NOT grow those giants with an extras builder.
- `force_fresh` is expressed as `session = None`. Dialects MUST NOT invent `--continue` from a missing token.
- `tmux_session_name` and Discord channel id are **not** StreamJson request fields. StreamJson dialects MUST ignore tmux.
- Simple and structured execution MUST share the same StreamJson family selection.
- Prompt, system prompt, schema, session, and secret env values MUST be redacted from command/debug logs.
- `remote_profile` on local process dialects MUST fail before spawn unless a dialect explicitly declares remote support.

### 3.4 PreparedCommand

Dialect preparation returns a validated command, not a bag of optional flags.

```text
PreparedCommand {
  executable: ResolvedExecutable,
  args: Vec<OsString>,
  redacted_args: Vec<OsString>,
  current_dir: PathBuf,
  environment_overlay: SecretEnvironment,
  stdin: Null,
  codec: Box<dyn StreamJsonCodec>,
  retry_policy: RetryPolicy,
  cleanup: Vec<OwnedTempArtifact>,
}
```

- Invalid flag combinations MUST be impossible after `prepare()` succeeds.
- Dialect MUST validate tool policy, structured output, prompt length/transport, model, session, cwd, and required capability before returning `PreparedCommand`.
- Temporary prompt/rules files, if used, MUST have owner-only permissions where supported and MUST be deleted on success, error, timeout, and cancellation.
- Runner MUST log only `redacted_args` and environment key names.

### 3.5 Dialect and codec contracts

Exact Rust signatures may follow repository style, but the ownership below is normative.

```text
trait StreamJsonDialect {
  prepare(request, registry_entry) -> Result<PreparedCommand>;
}

trait StreamJsonCodec {
  push_stdout_line(line) -> Result<Vec<StreamMessage>>;
  finish(exit_status, bounded_stderr) -> Result<Vec<StreamMessage>>;
}
```

Dialect owns:

- CLI argv and child cwd.
- prompt and system-prompt transport.
- model/structured-output flags.
- normalized tool policy mapping.
- session token validation and resume argv.
- retry classification and finalization semantics.
- a stateful codec instance per process attempt.

Codec owns:

- provider wire event parsing.
- text/tool/status/session/usage normalization.
- duplicate suppression and terminal fallback.
- unknown event forward-compatibility policy.

Runner MUST NOT switch on provider id, output event type, tool names, session flag, or model flag.

### 3.6 Runner responsibilities

Runner MUST:

1. Resolve the executable from PATH plus declarative registry hints.
2. Create the child in the requested trusted working directory with null stdin.
3. Create a process group on Unix (`setpgid` / kill process group). Windows MUST compile and cancel the direct child; a full Job Object is SHOULD, not a P0 blocker. Production authority is the Unix dcserver host.
4. Read stdout as bounded NDJSON lines and stderr separately as bounded diagnostics.
5. Apply startup, idle, and total timeout policy without blocking the async runtime.
6. Feed each stdout line to one stateful codec in order.
7. Send every codec-produced `StreamMessage` in order.
8. Kill the process tree on cancellation/timeout and wait/reap it.
9. Run cleanup on all terminal paths.
10. Retry only when the dialect returns a typed retry decision and the retry preserves session identity.

Runner MUST NOT:

- write prompts to stdin.
- know tmux, pane readiness, hooks, rollout files, MCP configuration, or provider update commands.
- ignore malformed stdout JSON as harmless noise. A dialect may explicitly classify a known banner, otherwise it is a bounded protocol error.
- merge stderr into stdout.
- retry a failed resume as a fresh session.
- retain unbounded lines, stderr, or accumulated raw JSON.

### 3.7 Session and retry invariants

- `ProviderSessionToken` is opaque to callers and validated by the selected dialect.
- General `is_valid_session_id` MUST NOT substitute for strict UUID validation where the CLI contract requires UUID.
- A fresh run MUST capture the terminal session/conversation id before reporting resumable success.
- A resumed run MUST verify the terminal id matches the requested id when the protocol reports it.
- `--continue`, cwd-latest, title lookup, numeric shortcut, or “latest” MUST NOT identify a Discord channel session unless that behavior is the existing Gemini compatibility contract being preserved and covered by characterization tests.
- Missing/expired/invalid Grok or AGY sessions MUST fail closed with a user-actionable error.
- Retry counters reset per outer request and MUST be observable without exposing prompt/session secrets.

### 3.8 Normalized tool policy

```text
ToolPolicy {
  ProviderDefault,
  ReadOnly,
  AllowListed(BTreeSet<AgentTool>),
}

ConfiguredToolPolicy {
  Explicit(ToolPolicy),
  LegacyAllowedTools(Vec<AgentTool>),
}
```

- `ProviderDefault`는 “AgentDesk가 제한하지 않은 provider 기본 surface”를 뜻한다. provider가 OS/sandbox로 완전히 unrestricted라는 뜻이 아니다.
- Config/API에는 `tool_policy_mode = provider_default | allowlist | read_only`와 mode에 맞는 `allowed_tools`를 표현할 수 있어야 한다.
- 신규/수정 write는 mode를 명시적으로 저장해야 한다.
- Parser/settings model MUST retain whether `tool_policy_mode` and `allowed_tools` existed in raw config; materialized vectors alone are not enough.
- Legacy normalization는 다음 순서를 지킨다.
  1. mode가 있으면 그 mode를 검증해 `Explicit`으로 사용한다.
  2. mode와 `allowed_tools`가 모두 없거나 legacy empty이면 `Explicit(ProviderDefault)`로 본다.
  3. mode 없이 non-empty `allowed_tools`가 명시돼 있으면 vector equality로 추측하지 않고 `LegacyAllowedTools`로 보존한다.
  4. 기존 provider의 `LegacyAllowedTools`는 그 provider의 현재 동작을 보존하는 compatibility resolver를 탄다.
  5. Grok/Antigravity 신규 생성 또는 provider 변경 write는 `tool_policy_mode`를 반드시 기록하며 기본값은 `provider_default`다.
  6. meeting orchestration은 저장 설정과 별개로 `ReadOnly`를 명시적으로 요청한다.
- `LegacyAllowedTools`는 config audit와 UI에 구분되어 보여야 하며 다음 정상 save에서 운영자가 선택한 explicit mode로 기록된다.
- 공용 default vector와 같다는 이유만으로 provider-default라고 추론해서는 안 된다. 이는 새 provider tool을 조용히 여는 권한 확대가 될 수 있다.
- `allowlist` mode의 빈 목록은 validation error다. 빈 목록을 unrestricted로 확대하지 않는다.
- Every dialect MUST return either an exact native mapping or `UnsupportedToolPolicy` before spawn.
- `meeting_readonly` MUST request `ReadOnly`, not rely on system-prompt prose.
- A provider whose registry says `supports_restricted_tool_policy=false` MUST be disabled in restricted UI/workflows and must still reject a forged backend request.
- Tool policy and approval policy are separate. Auto-approval is permitted only after the allowed tool surface is constructed.

#### 3.8.1 Discord settings → dialect (P0 gate)

Current code (`settings/read.rs`) fills omitted `allowed_tools` with `DEFAULT_ALLOWED_TOOLS` (Bash/Edit/Write/…). Passing that Vec to Grok/AGY as `AllowListed` makes every default Discord turn `UnsupportedToolPolicy`. Inferring ProviderDefault by vector equality is forbidden.

MUST:

1. Change the settings **read model** so it carries `ConfiguredToolPolicy`, not only `Vec<String>`.
2. Absent/empty `allowed_tools` and absent `tool_policy_mode` → `Explicit(ProviderDefault)`.
3. Mode-less non-empty vector → `LegacyAllowedTools` (existing Claude/Codex/Gemini/Qwen compatibility only).
4. Headless/intake MUST pass that enum into `ProviderTurnRequest.tool_policy`.
5. Grok `ProviderDefault` → §5.2 argv (no `--tools` restriction). Grok `ReadOnly` or `{Read,Grep,Glob}` subset → §5.3. Any other set → fail-closed before spawn.
6. Antigravity `ProviderDefault` → §6.2. Any `ReadOnly`/`AllowListed` → fail-closed before spawn.
7. **GATE-007 MUST be green before Slice D or E enables the provider in catalog/UI.**

Owner: `src/services/discord/settings/read.rs` plus a small typed policy module under `stream_json_cli/policy.rs` or `discord/settings`. MUST NOT dump the resolver into `headless_turn.rs`.

---

## 4. Gemini Dialect and Extraction

### 4.1 Behavior preservation

Before extracting production code, characterization tests MUST pin:

- fresh and resume argv.
- Gemini selector coercion/normalization.
- trusted working-directory behavior.
- remote-profile rejection.
- system prompt and allowed-tool composition.
- read-only sandbox/admin policy and temporary-file cleanup.
- `init`, `message`, `tool_use`, `tool_result`, `error`, `result` mappings.
- status and usage emission.
- startup/idle watchdog behavior.
- cancel child-tree behavior.
- retry classifier/count and finalization fallback.

The extraction MUST produce the same ordered `StreamMessage` sequence and equivalent error classification for the same fixture/child behavior.

### 4.2 Ownership after extraction

- `stream_json_cli::runner` owns the process attempt loop.
- `dialects::gemini` owns Gemini args, selector/session policy, readonly policy, Gemini codec, and retry/finalization decisions.
- `services/gemini.rs` remains a thin facade for existing public APIs and unrelated Gemini management operations.
- Grok or AGY registry rows MUST NOT land before the Gemini-only runner path is green.

---

## 5. Grok Dialect

### 5.1 Identity and registry

| Field | Value |
|---|---|
| `ProviderKind` | `Grok` |
| canonical id | `grok` |
| aliases | none initially |
| display name | `Grok` |
| binary | `grok` |
| executable hints | PATH, then `~/.grok/bin/grok` (`grok.exe` on Windows) |
| channel suffix | `-gx` |
| execution | `StreamJsonCli(Grok)` |
| managed tmux | false |
| compaction | behavioral `Disabled` |
| TUI hosting | false |
| context window | `Unknown`, not an invented constant |

### 5.2 Base launch contract

Fresh `ProviderDefault` turn MUST be equivalent to:

```text
grok
  -p <prompt>
  --output-format streaming-messages-json
  --cwd <working_directory>
  --verbatim
  --no-auto-update
  --no-memory
  --yolo
  [--model <model>]
  [--rules <system_prompt>]
```

Resume adds only:

```text
--resume <uuid>
```

Rules:

- `--system-prompt-override` MUST NOT be used in the default AgentDesk path because it replaces the provider default prompt and skips rules.
- `--rules` MUST carry AgentDesk system instructions when present.
- `--no-memory` MUST isolate provider cross-session memory; the explicit Grok session still retains its own context.
- `--no-auto-update` MUST keep service execution deterministic. Updates use a separate provider update operation.
- `-s/--session-id`, `--continue`, title-based resume, `--fork-session`, `streaming-json`, and stdin prompt MUST NOT be used.
- `--verbatim` and `--rules` together are required for the default path. Slice A MUST capture one live command that uses both. If the installed Grok version rejects the pair, drop `--verbatim` (keep `--rules`) and record the version in the fixture note. Do not guess.
- UUID MUST be validated with a Grok-specific strict UUID validator before spawn.
- A resumed terminal result whose session id differs from the requested UUID is a protocol error.
- Missing/expired resume errors MUST NOT retry fresh.

### 5.3 Tool policy mapping

Native mapping for the AgentDesk read tools is:

| AgentDesk tool | Grok built-in tool |
|---|---|
| `Read` | `read_file` |
| `Grep` | `grep` |
| `Glob` | `list_dir` |

`ReadOnly` or an explicit allowlist composed only of these tools MUST add an exact comma-separated `--tools` value and defense-in-depth restrictions:

```text
--yolo
--tools <mapped_read_tools>
--disallowed-tools Agent
--no-subagents
--disable-web-search
--deny MCPTool(*)
```

- `--yolo` is allowed only after the allowlist/deny set is successfully constructed; deny rules remain authoritative.
- `--tools` alone is insufficient because Grok retains MCP meta-tools.
- Unknown AgentDesk tool names, an explicit empty set, or write/shell tools without a reviewed native mapping MUST return `UnsupportedToolPolicy` before spawn.
- System prompt prose MUST NOT substitute for native restrictions.
- Snapshot tests MUST assert forbidden flags and the complete effective policy, not only the presence of `--tools`.

### 5.4 MessagesJson codec

- Grok MUST use a stateful codec extracted around `process_stream_line` behavior, not per-line `parse_stream_message` calls.
- Codec MUST preserve state across `system/init`, assistant/user messages, and terminal result.
- All text/thinking/tool_use/tool_result blocks in a line MUST be processed in order. Only returning the first block is a defect.
- Multiple parallel tool uses and multiple tool results MUST have fixtures.
- Missing placeholder fields in Grok init/result MUST be tolerated; session identity, terminal status, errors, and real usage fields remain validated.
- Unknown content blocks SHOULD produce an observable bounded status/diagnostic and MUST NOT corrupt subsequent known blocks.
- Terminal success without a valid session id MUST NOT be reported as resumable success.

### 5.5 Auth, update, and hosting

- Auth metadata sources: `~/.grok/auth.json`, `XAI_API_KEY`.
- File detection MUST inspect only whether expected credential fields are non-empty and MUST never return/log their values.
- `grok --version` proves installation, not authentication.
- Interactive login MUST NOT run from doctor. Doctor may show `grok login --device-auth` guidance.
- P0 MUST NOT add an npm/pip update strategy. P1 may add an explicit `grok update` owner module.
- Dashboard/onboarding MUST reject new `runtime: tui`/`tui_hosting: true` settings for Grok.
- Runtime MAY retain legacy `LegacyPrompt` fallback with `tui_hosting_driver_unavailable`, but it MUST be visible in result/log/doctor.

---

## 6. Antigravity (AGY) Dialect

### 6.1 Identity and registry

| Field | Value |
|---|---|
| `ProviderKind` | `Antigravity` |
| canonical id | `antigravity` |
| accepted input alias | `agy` |
| display name | `Antigravity` |
| binary | `agy` |
| executable hints | PATH; Windows `%LOCALAPPDATA%\agy\bin\agy.exe`; unverified OS paths are not invented |
| channel suffix | existing `-ag` |
| execution | `StreamJsonCli(Agy)` |
| managed tmux | false |
| compaction | behavioral `Disabled` unless a verified operation exists |
| TUI hosting | false |
| context window | `Unknown` |

- Serialization, API output, database values, dashboard values, and new config MUST use `antigravity`.
- `ProviderKind::from_str("agy")` MAY normalize to `Antigravity`, but `as_str()` MUST return `antigravity`.
- A separate `agy` dashboard option, channel suffix, registry row, or provider color MUST NOT be created.

### 6.2 Launch contract

Fresh `ProviderDefault` turn MUST be equivalent to:

```text
agy
  --sandbox
  --disable-slash-commands
  --output-format stream-json
  --print-timeout <derived_timeout>
  [--model <model>]
  --print <composed_prompt>
```

Resume adds:

```text
--conversation <conversation_uuid>
```

Rules:

- Child process current directory MUST be the trusted AgentDesk working directory. There is no invented `--cwd` flag.
- `--continue`, `-c`, `--new-project`, implicit latest conversation, and stdin prompt MUST NOT be used for channel identity.
- `--dangerously-skip-permissions` MUST never be passed.
- `--mode plan` MUST NOT be combined with `--disable-slash-commands`; current CLI reports that plan mode then has no effect.
- P0 `ProviderDefault` mode uses `--sandbox` and disables slash/skill expansion, but MUST NOT describe this as read-only.
- `--print-timeout` MUST be `outer_hard_kill_deadline - 5s`, floored at 30s. If the outer deadline is under 35s, omit `--print-timeout` only when that outer deadline is below the CLI default (documented 5m) so the runner still hard-kills first. The 5s skew is the P0 constant; do not invent another.
- If the composed prompt exceeds the OS argv budget, preparation MUST fail with a clear provider limitation unless a verified AGY prompt-file transport is added. It MUST NOT truncate.

### 6.3 System prompt transport

AGY 1.1.13 exposes no verified native system-prompt flag in the surveyed contract.

- `dialects::agy` MUST compose system and user input into a deterministic, tested envelope passed to `--print`.
- The envelope MUST preserve distinct system/user boundaries and escape or length-prefix delimiter material so user text cannot terminate the system section.
- Logs MUST redact the entire composed prompt.
- Registry/catalog MUST describe this as `system_prompt_transport=envelope`, not native system-role parity.
- A future native AGY mechanism may replace the envelope only with an actual fixture and regression tests.

### 6.4 Session invariants

- Fresh `event=init.conversation_id` and terminal `result.conversation_id` MUST be captured and must agree.
- The captured UUID-shaped contract MUST use an AGY-specific strict validator; generic session token validation is insufficient.
- Resume uses only `--conversation <id>`.
- Resumed init/result ids MUST equal the requested id.
- Invalid, missing, or expired conversation MUST fail closed and MUST NOT retry without `--conversation`.
- `--continue` MUST NOT be used because cwd-latest is not Discord channel identity.

### 6.5 AGY codec

The codec is stateful and MUST cover at least:

```text
event=init
event=step_update
event=result
```

Mapping rules:

- `init.conversation_id` -> `StreamMessage::Init` session identity.
- `step_update.step_type=agent_response` and `text_delta` -> text delta exactly once per received line.
- Non-response lifecycle steps (`user_input`, `system_message`, `checkpoint`) MAY become bounded status events but MUST NOT be rendered as assistant text.
- A terminal aggregate `result.response` MUST be emitted only as fallback when no response delta was observed. Re-emitting it after deltas is a duplicate-output defect.
- `step_update.usage` MUST be counted once per terminal/DONE `step_index` for the current process attempt.
- `result.usage` is treated as conversation aggregate telemetry and MUST NOT be added to step usage. If step usage is absent, an explicitly tested fallback/delta policy is required.
- `result.status=SUCCESS` yields Done only after session identity validation. Error status maps to Error plus terminal Done/error semantics consistent with existing `StreamMessage` consumers.
- Unknown event/step types MUST be bounded and observable. They MUST NOT crash the process reader or be silently treated as text.
- Tool-call/result mapping and `supports_tool_stream=true` require a sanitized real tool fixture. Until then lifecycle steps may be status-only and the registry capability remains false.
- `--json-schema` and structured-output capability require a separate real fixture. Until then explicit structured requests fail before spawn.

### 6.6 Tool policy

- P0 Antigravity supports `ToolPolicy::ProviderDefault` only.
- `ReadOnly` or `AllowListed(...)` MUST return `UnsupportedToolPolicy` before spawn until native enforcement is proven.
- `--sandbox` and prompt instructions MUST NOT be used as evidence of exact read-only enforcement.
- Dashboard meeting/restricted workflows MUST disable Antigravity and explain “restricted tool policy not supported”.
- Meeting participant/reviewer pickers and `counterpart()` consumers MUST NOT select Antigravity in P0. `meeting_orchestrator` already sends `["Read"]` (`ReadOnly`). Grok MUST accept that mapping. If a meeting request still names Antigravity, the dialect MUST fail closed before spawn with the same unsupported-policy error — UI filter is not the only gate.
- A future capability may be enabled only after a scratch-root test proves representative file write, delete, shell, browser, MCP, and subagent actions are blocked while expected reads still work.
- That proof MUST also test slash-command handling; `--mode plan` with disabled slash expansion is known-invalid.

### 6.7 Auth, model catalog, update, and hosting

- `agy --version` -> installed/version only.
- `agy models` -> model catalog reachable only; it MUST NOT be labeled authenticated execution by itself.
- An opt-in minimal `agy -p` smoke -> authenticated/usable for that model and host. It may incur cost and create a conversation, so default doctor MUST NOT run it.
- No credential file path or env key is invented or searched. Secret discovery is out of scope.
- Model ids come from live `agy models`; static model lists are not source of truth.
- P1 may add an explicit `agy update` strategy in a provider update owner module.
- Dashboard/onboarding MUST reject new TUI hosting for Antigravity; legacy runtime fallback follows the Grok visibility rule.

---

## 7. Registry, Config, DB, Doctor, and UI

### 7.1 Provider registry as identity authority

`ProviderRegistryEntry` MUST contain the known `ProviderKind` value so canonical id resolution does not require a second closed id match.

Conceptual fields:

```text
ProviderRegistryEntry {
  kind,
  id,
  aliases,
  display_name,
  cli_init_label,
  channel_suffix,
  executable,
  executable_hints,
  capabilities,
  execution_adapter,
  compaction_behavior,
  readiness_behavior,
  context_window_policy,
  default_behavior,
  auth_probe,
  managed_tmux_behavior,
}
```

Invariants:

- Every known `ProviderKind` has exactly one registry row.
- Canonical ids and aliases are unique case-insensitively.
- `from_str(id|alias)` returns the row kind; `as_str()` returns only canonical id.
- `supported_provider_ids()` returns canonical runtime ids only.
- execution adapter family/dialect is compatible with declared capabilities.
- adapter no longer duplicates provider id/capabilities.
- preferred counterparts are derived, not hand-maintained N×N tables. **`counterpart()` first item is frozen:**

  | Provider | `counterpart()` MUST return |
  |---|---|
  | Claude | Codex |
  | Codex | Claude |
  | Gemini | Codex |
  | OpenCode | Codex |
  | Qwen | Codex |
  | Grok | Codex |
  | Antigravity | Codex |

  Remaining counterparts are “all other supported providers except self and the first item”, stable-sorted by canonical id. Full pairwise arrays in `provider.rs` are forbidden. Changing any first-item pairing is out of scope.
- `default_context_window` becomes an explicit known/dynamic/unknown policy; unknown is not rendered as a fabricated number.

The conformance test MUST be rewritten around these invariants. It cannot preserve the old one-adapter-one-provider assertion.

### 7.2 AgentChannels map

Canonical owner MUST be `src/config/agent_channels.rs`.

```text
AgentChannels(BTreeMap<String, AgentChannel>)
```

- Existing YAML keys `claude`, `codex`, `gemini`, `opencode`, `qwen` MUST deserialize unchanged.
- `grok` and `antigravity` MUST require no new struct fields.
- `get`, `insert`, `remove`, `iter` MUST operate on canonical ids.
- New write/API operations MUST normalize accepted aliases through registry and store canonical ids.
- Unknown legacy keys MUST round-trip and produce an unsupported-provider diagnostic; config merge MUST NOT silently drop them.
- Unknown keys MUST NOT become dispatchable until a registry row exists.
- Direct `.claude`, `.gemini`, etc. field access MUST be removed from config reload, merge, DB, audit, CRUD, onboarding, and tests.
- `runtime_layout::normalize_provider_name` MUST delegate to registry normalization rather than maintain another list.

### 7.3 DB binding

- No schema migration or `discord_channel_grok`/`discord_channel_antigravity` columns.
- Claude/Codex keep their existing dedicated legacy column behavior.
- Every other supported configured provider uses the agent's primary `discord_channel_id` through one generic rule.
- The existing Gemini/Qwen safety check — primary id must not equal the dedicated Claude `discord_channel_cc` — MUST apply to **every** non-Claude/non-Codex configured provider, including Grok and Antigravity. Do not add named match arms per new provider.
- DB read/write tests MUST cover Gemini, OpenCode, Qwen, Grok, Antigravity, and unsupported preserved config keys.

### 7.4 Doctor and onboarding

- Base provider checks MUST iterate registry rows in a small owner module. Doctor orchestrator only composes returned checks.
- Generic checks: executable resolution, version probe, declared auth metadata probe, hosting/capability summary.
- Provider-specific optional checks remain in dialect/auth owner modules.
- Grok doctor states MUST distinguish `binary missing`, `installed`, `credential metadata present`, `live smoke not run`.
- AGY doctor states MUST distinguish `binary missing`, `installed`, `catalog reachable`, `authenticated smoke not run/passed`.
- Doctor MUST NOT run interactive login, print credential contents, or infer live usability from a file path alone.
- Onboarding provider validation MUST use registry normalization and capability checks instead of a closed provider match.

### 7.5 Backend provider catalog

Add a read-only `GET /api/providers` projection from the registry. Auth MUST match the existing dashboard settings/agent CRUD guard (same session/cookie or bearer the dashboard already uses for `GET /api/settings/*`). It is not a public unauthenticated catalog. It MUST expose only non-secret product metadata, for example:

```text
ProviderCatalogEntry {
  id,
  display_name,
  channel_suffix,
  binary_name,
  execution_surface,
  supports_resume,
  supports_structured_output,
  supports_tool_stream,
  supports_restricted_tool_policy,
  supports_tui_hosting,
  system_prompt_transport,
}
```

- Auth paths, env keys, command arguments, model credentials, and internal filesystem hints MUST NOT be returned.
- Catalog order MUST be deterministic.
- The endpoint and Rust registry serialization MUST have snapshot/conformance tests.

### 7.6 Dashboard catalog consumption

- New provider selectors MUST consume `/api/providers`; static `CLI_PROVIDERS` MUST NOT be the authority.
- Provider settings UI/API MUST preserve `tool_policy_mode`; the displayed legacy tool vector alone MUST NOT determine whether a policy is provider-default or explicit.
- `AgentFormModal`, setup wizard, onboarding, profile selector, meeting provider selector, suffix mapping, and labels MUST derive from the same loaded catalog/presentation layer.
- Existing `copilot` and `api` values MAY remain in a `legacyProviderPresentation` map so old data renders, but they MUST be excluded from create/edit selectors.
- Existing `antigravity` presentation becomes runtime-backed; it is no longer a ghost after P0-C.
- Theme metadata may remain frontend-owned, but unknown ids MUST receive a deterministic accessible fallback rather than requiring CSS plumbing to render.
- Catalog unavailable: existing values may render from legacy presentation, but new provider creation MUST be disabled with a clear loading/error state.
- Restricted workflows MUST filter or disable providers whose catalog says `supports_restricted_tool_policy=false`.
- No cast from arbitrary input to a closed `CliProvider` union may bypass catalog validation.

### 7.7 Registration surface audit

Implementation MUST run compile- and search-driven inventory rather than rely on this finite list. Each hardcoded provider surface is categorized as:

1. derive from registry/catalog,
2. intentionally provider-specific behavior,
3. legacy compatibility only,
4. unsupported and explicitly rejected.

At minimum audit:

- `src/services/provider.rs` and conformance tests.
- `src/services/provider_exec.rs`, Discord headless/intake.
- `src/config.rs`, config live reload/merge, runtime layout.
- `src/db`, config audit, agent CRUD.
- doctor, onboarding, CLI init/run/args/reporting.
- dispatch suffix and provider hosting.
- meeting/review/health provider lists and error strings.
- dashboard types, schemas, wizard, profile, meeting, theme, suffix maps.
- example YAML, API/OpenAPI/generated docs, inventory docs, E2E docs.

Compiler exhaustiveness is evidence, not proof of complete product registration; string arrays and frontend lists require conformance tests.

---

## 8. Forbidden Designs

- Copy Gemini/Qwen process loops into `grok.rs` or `agy.rs`.
- Make every dialect call `parse_stream_message` regardless of wire schema.
- Encode CLI behavior as a passive bag of `resume_flag`, `approve_args`, `parse_via`, and arbitrary `extra_args`.
- Let runner branch on `provider_id` or provider event names.
- Keep adapter-level provider id/capabilities after introducing a shared family.
- Add provider matches to Discord headless/intake for Gemini/Grok/Antigravity instead of central dispatch.
- Change `counterpart()` first item for any existing provider, or make meeting reviewers a newly added StreamJson provider by default.
- Pass `settings/read.rs` materialized `DEFAULT_ALLOWED_TOOLS` to Grok or Antigravity as an explicit allowlist.
- Retry Grok/AGY resume failure without the requested session/conversation token.
- Use `--continue`, cwd-latest, or title lookup for channel identity.
- Use Grok `-s/--session-id` to resume.
- Use Grok `--system-prompt-override` in the default path.
- Apply Grok `--yolo` before an explicit restricted policy is constructed.
- Treat AGY `--sandbox` or prompt prose as read-only enforcement.
- Pass AGY `--dangerously-skip-permissions`.
- Combine AGY `--mode plan` and `--disable-slash-commands`.
- Create a second `agy` provider beside canonical `antigravity`.
- Keep named AgentChannels fields or silently drop unknown keys.
- Add `discord_channel_grok` or `discord_channel_antigravity`.
- Add Grok/AGY TUI drivers, hooks, pane watchers, ACP, or hotfile branches in this scope.
- Put secret values, raw prompts, session credentials, or auth JSON contents in logs/doctor/API/fixtures.
- Set tool-stream, structured-output, auth-ready, context-window, or production-ready claims without the corresponding proof gate.
- Grow frozen giant production code and excuse it only with line-count registry updates.

---

## 9. Requirements, Tasks, Tests, and Traceability

### 9.1 Requirements

- **REQ-001** Runner is provider-neutral and Gemini behavior is preserved exactly enough for existing consumers.
- **REQ-002** Each provider uses a stateful dialect codec appropriate to its wire schema.
- **REQ-003** All streaming callers enter one `ProviderTurnRequest` family dispatch.
- **REQ-004** Registry owns canonical identity, aliases, capabilities, family/dialect binding, and derived counterpart coverage without adapter duplication.
- **REQ-005** Grok obeys the launch, strict UUID resume, system rules, Messages codec, and no-fresh-fallback contracts.
- **REQ-006** Grok restricted tool requests map exactly to native allow/deny controls or fail closed.
- **REQ-007** Antigravity uses canonical id `antigravity`, binary/alias `agy`, AGY launch, and strict `--conversation` identity.
- **REQ-008** AGY codec normalizes init/deltas/result/session/usage without duplicate response or usage.
- **REQ-009** Antigravity restricted/structured/tool-stream capabilities remain disabled until separately proven and forged requests fail before spawn.
- **REQ-010** AgentChannels and DB binding are provider-map/generic and preserve legacy/unknown configuration without silent loss.
- **REQ-011** Backend registry drives dashboard selectable providers and capability-aware UX; no runtime-less new selector entry exists.
- **REQ-012** Unsupported new TUI settings are rejected while legacy fallback remains observable.
- **REQ-013** Binary, auth, context-window, model, and live-usability states are represented honestly and without secrets.
- **REQ-014** No new DB schema, hotfile/TUI/ACP logic, provider loop copy, or net growth of touched frozen giant feature logic.
- **REQ-015** Sanitized real fixtures and proportional live smoke gates support every enabled provider capability and release claim.
- **REQ-016** Tool-policy config preserves provider-default vs explicit allowlist vs read-only provenance from legacy read through new write and runner dispatch. Discord settings read MUST emit `ConfiguredToolPolicy` before any Grok/AGY catalog row is enabled.
- **REQ-017** `counterpart()` first item stays as in L-018/§7.1. Meeting/restricted pickers never select Antigravity in P0. Grok accepts meeting `ReadOnly`.
- **REQ-018** Default Discord/headless turns for Grok and Antigravity are usable: omitted tools → `ProviderDefault` → dialect default argv. Restricted/meeting paths stay fail-closed per provider capability.

### 9.2 Tasks

- **TSK-001** Add Gemini characterization tests and sanitized Grok/AGY protocol captures before extraction. (REQ-001, REQ-002, REQ-015)
- **TSK-002** Extract provider registry/catalog owner modules; add kind binding, aliases, family/dialect, behavioral adapters, context-window policy, and new conformance invariants. (REQ-004, REQ-013, REQ-014)
- **TSK-003** Move AgentChannels to a map newtype and generalize config merge, runtime normalization, DB, audit, CRUD, onboarding writes. (REQ-010, REQ-014)
- **TSK-004** Introduce `ProviderTurnRequest` and collapse simple/structured/headless/intake streaming dispatch to one family selection. (REQ-003, REQ-014)
- **TSK-005** Implement process runner/stateful codec contracts and move Gemini through them with golden parity. (REQ-001, REQ-002, REQ-014)
- **TSK-006** Implement Grok dialect, executable resolution, auth metadata, session, policy mapping, Messages codec, and thin public facade. (REQ-005, REQ-006, REQ-013)
- **TSK-007** Register/enable Grok in registry, doctor, onboarding, config example, catalog, dashboard, and generic DB/channel paths. (REQ-005, REQ-010, REQ-011, REQ-012)
- **TSK-008** Implement AGY dialect, prompt envelope, session policy, AGY codec, policy rejection, executable resolution, and thin public facade. (REQ-007, REQ-008, REQ-009, REQ-013)
- **TSK-009** Register/enable canonical Antigravity in registry, doctor, onboarding, catalog, dashboard, existing `-ag` routing, and generic DB/channel paths. (REQ-007, REQ-009, REQ-010, REQ-011, REQ-012)
- **TSK-010** Add authenticated provider catalog API and migrate dashboard selectors/presentation/capability filters away from static provider authority. (REQ-011, REQ-013)
- **TSK-011** Generalize hosting validation and doctor state labels; preserve observable legacy fallback and opt-in live-smoke separation. (REQ-012, REQ-013)
- **TSK-012** Run full registration-surface audit, regenerate inventory/docs, enforce frozen giant and forbidden-path ratchets. (REQ-014, REQ-015)
- **TSK-013** Add typed tool-policy mode, change `settings/read.rs` to return `ConfiguredToolPolicy`, stop materializing `DEFAULT_ALLOWED_TOOLS` for omitted tools, wire meeting `ReadOnly`, and validate provider capabilities. (REQ-006, REQ-009, REQ-016, REQ-018)
- **TSK-014** Implement derived counterparts with frozen first-item table; filter Antigravity from meeting/restricted pickers; keep Grok meeting-capable. (REQ-017, REQ-018)

### 9.3 Tests

- **TEST-001 Gemini golden parity:** fresh/resume argv, all event mappings, retry, timeout, cancel, cleanup, finalization before/after extraction.
- **TEST-002 Runner fake CLI:** ordered NDJSON, bounded stderr, malformed/oversized stdout, no terminal event, non-zero exit, startup/idle/total timeout, cancel child tree, cleanup.
- **TEST-003 Codec conformance harness:** state isolation per attempt, multi-message output, unknown event handling, terminal requirement, no unbounded accumulation.
- **TEST-004 Grok real sanitized fixture:** init, multiple text/thinking/tool_use, multiple tool_result, success/error result, omitted placeholder fields, session/usage.
- **TEST-005 Grok argv/policy snapshots:** ProviderDefault, ReadOnly, each Read/Grep/Glob subset, unsupported/empty policy fail-before-spawn, no override/continue/session-id/native stream mismatch.
- **TEST-006 Grok session/resolver/auth:** strict UUID, terminal id equality, missing resume no fresh retry, PATH and `~/.grok/bin` fallback, secret-redacted auth metadata.
- **TEST-007 AGY real sanitized fresh/resume fixture:** init/step_update/result, same conversation id, `num_turns=2`, error terminal.
- **TEST-008 AGY codec accounting:** response delta vs aggregate fallback, unique step-index usage, no addition of result aggregate, unknown lifecycle/tool step observability.
- **TEST-009 AGY command/policy:** process cwd, derived timeout, deterministic escaped envelope, strict conversation id, no continue/new-project/dangerous flag, no invalid plan+slash combination, restricted/structured requests fail-before-spawn until enabled.
- **TEST-010 Registry/dispatch invariants:** one row per known kind, unique canonical ids/aliases/suffixes, `agy -> antigravity`, adapter has no identity capability, correct dialect binding, derived remaining counterparts, **frozen first-item table**, StreamJson family dispatch for Gemini/Grok/AGY only.
- **TEST-011 AgentChannels:** legacy five-key read/write, Grok/Antigravity round-trip, alias canonicalization on write, unknown preservation/diagnostic, no named field API.
- **TEST-012 DB/config integration:** generic primary channel for Gemini/OpenCode/Qwen/Grok/Antigravity, Claude/Codex legacy behavior, config live reload/merge/audit/CRUD preservation.
- **TEST-013 Catalog/dashboard:** API snapshot has only non-secret metadata; all create/edit/profile/onboarding/meeting selectors derive from catalog; `copilot`/`api` legacy-only; Antigravity is runtime-backed; restricted filters honor capabilities.
- **TEST-014 Hosting:** new Grok/Antigravity TUI config rejected; legacy config falls back with `tui_hosting_driver_unavailable`; no TUI driver availability change.
- **TEST-015 Maintenance ratchets:** forbidden hotfile/TUI/ACP/DB migration diff empty; touched frozen giant aggregate production net-negative; each new production file <1000; compile/search surface audit has no uncategorized provider lists.
- **TEST-016 Opt-in live smoke:** supported service hosts run version, readiness labels, fresh/resume, cancel, invalid resume; Grok ReadOnly proof; AGY restricted rejection; external cost/state disclosed.
- **TEST-017 Tool-policy provenance:** raw absent/empty legacy config, mode 없는 explicit legacy vector, explicit subset/mode, invalid empty allowlist mode, provider change/new API write, next-save upgrade, and meeting override produce the intended typed policy without widening. Grok/AGY omitted-tools Discord settings MUST be `Explicit(ProviderDefault)`, never the materialized `DEFAULT_ALLOWED_TOOLS` vector.
- **TEST-018 Counterpart and meeting:** first-item table matches §7.1; remaining list is stable-sorted and excludes self/first; meeting reviewer for Claude is still Codex; Antigravity is absent from meeting/restricted selectors; Grok accepts `ReadOnly`/`["Read"]`; a forged Antigravity ReadOnly request fails before spawn.

### 9.4 Traceability

| REQ | TASKS | TESTS |
|---|---|---|
| REQ-001 | TSK-001, TSK-005 | TEST-001, TEST-002 |
| REQ-002 | TSK-001, TSK-005, TSK-006, TSK-008 | TEST-003, TEST-004, TEST-007, TEST-008 |
| REQ-003 | TSK-004 | TEST-010 |
| REQ-004 | TSK-002 | TEST-010 |
| REQ-005 | TSK-006, TSK-007 | TEST-004, TEST-005, TEST-006 |
| REQ-006 | TSK-006 | TEST-005, TEST-016 |
| REQ-007 | TSK-008, TSK-009 | TEST-007, TEST-009, TEST-010 |
| REQ-008 | TSK-008 | TEST-007, TEST-008 |
| REQ-009 | TSK-008, TSK-009 | TEST-009, TEST-013, TEST-016 |
| REQ-010 | TSK-003, TSK-007, TSK-009 | TEST-011, TEST-012 |
| REQ-011 | TSK-007, TSK-009, TSK-010 | TEST-013 |
| REQ-012 | TSK-007, TSK-009, TSK-011 | TEST-014 |
| REQ-013 | TSK-002, TSK-006, TSK-008, TSK-010, TSK-011 | TEST-006, TEST-009, TEST-013, TEST-016 |
| REQ-014 | TSK-002, TSK-003, TSK-004, TSK-005, TSK-012 | TEST-015 |
| REQ-015 | TSK-001, TSK-012 | TEST-001, TEST-004, TEST-007, TEST-016 |
| REQ-016 | TSK-013 | TEST-005, TEST-009, TEST-013, TEST-017 |
| REQ-017 | TSK-014 | TEST-010, TEST-018 |
| REQ-018 | TSK-006, TSK-008, TSK-013, TSK-014 | TEST-005, TEST-009, TEST-016, TEST-017, TEST-018 |

---

## 10. Implementation and Merge Order

Implement as independently reviewable, rollback-safe slices. A single local branch may contain them, but commits/PRs MUST preserve this dependency order.

1. **Slice A — Characterization only**
   - Gemini golden tests.
   - Sanitized Grok text/tool/resume/error capture.
   - Sanitized AGY text/resume/error capture; tool/structured capture only if capability will be enabled.
2. **Slice B — Registration tax, behavior-preserving**
   - Registry kind binding, aliases, family/dialect metadata, behavioral adapters.
   - Frozen `counterpart()` first-item table (§7.1) + TEST-018 characterization on the five existing providers (Grok/AGY rows not yet added).
   - AgentChannels map and generic DB/config paths, including the generic Claude-channel collision check.
   - Typed tool-policy mode **and** `settings/read.rs` `ConfiguredToolPolicy` (TEST-017). Existing five providers keep current Discord behavior.
   - Provider catalog API with existing five providers only. Same auth as dashboard settings.
3. **Slice C — Runner and Gemini**
   - `ProviderTurnRequest` as specified in §3.3 (no Claude extras).
   - Headless/intake: replace only the Gemini match arm with `execute_streaming(request)`. Do not add extras builders to those files.
   - Runner/codec abstractions; Gemini moved to `StreamJsonCli(Gemini)`; all golden tests green.
4. **Slice D — Grok (usable Discord/headless pipe)**
   - Blocked on GATE-001, GATE-007, TEST-017.
   - Grok dialect and policy/session/codec.
   - Registry/doctor/catalog/dashboard/onboarding activation in one change series.
   - Default omitted-tools Discord turn uses ProviderDefault argv and succeeds in live smoke.
   - Meeting/ReadOnly path works. Invalid resume does not go fresh.
5. **Slice E — Antigravity (usable Discord/headless pipe, not meeting)**
   - Blocked on GATE-001, GATE-007, TEST-017.
   - AGY dialect and codec/session/policy rejection.
   - Canonical `antigravity` registry/doctor/catalog/dashboard activation; `agy` alias only.
   - Default omitted-tools Discord turn uses ProviderDefault argv and succeeds in live smoke.
   - Meeting/restricted selectors exclude Antigravity; forged ReadOnly fails before spawn.
6. **Slice F — Closure**
   - Full surface audit, inventory regeneration, docs, forbidden-path/line ratchets, repository-wide relevant tests.

Rules:

- A provider UI/catalog row MUST NOT land before its runner+dialect+fixture are green **and** GATE-007 (settings provenance) is green.
- A provider backend row and UI availability MUST be enabled/disabled together.
- Grok/Antigravity failures MUST be rollbackable by removing their registry availability without reverting the common runner used by Gemini.
- AgentChannels/registry refactors MUST remain useful and behavior-preserving if a provider slice is rolled back.
- Do not start Slice D or E from PRD “P0-A” wording. §10 is the only order.

---

## 11. Verification Commands and Evidence Classes

Exact commands may follow repository scripts discovered at implementation time. The evidence classes are normative.

### Focused code evidence

- Rust unit/integration tests for runner, codecs, registry, config, DB, hosting, doctor.
- Dashboard unit tests, typecheck, and catalog-driven form tests.
- Formatter/linter for touched Rust/TypeScript/Markdown.
- Inventory generator and maintenance freshness checks.
- Search ratchets for direct provider matches, named AgentChannels fields, static selector authority, forbidden modules, and secret-bearing fixture fields.

### Live evidence

- Local CLI version/help snapshots record the exact tested Grok/AGY versions.
- Fixtures replace real UUIDs, cwd, model/account metadata, and prompts with deterministic sanitized values.
- Opt-in live tests state model/provider cost and any conversation/session side effect before running.
- Service-host smoke proves binary resolution and authentication on that host; a developer workstation smoke does not prove deployment readiness.

### Completion language

- `implemented`: production path and tests exist.
- `locally verified`: focused local automated tests passed.
- `live-smoke verified`: an authenticated provider call passed on the named host/OS/version.
- `configured`: config/catalog/UI row is enabled.
- `production-ready`: required service-host binary, auth, policy, resume, cancel, observability, rollback, and relevant CI gates all passed.

These states MUST NOT be collapsed into one “done” claim.

---

## 12. Gates and Open Questions

### Merge-blocking gates

- **GATE-001:** Gemini characterization and extracted-path golden parity pass before any new provider activation.
- **GATE-002:** Grok sanitized real fixture, strict resume/no-fresh test, and tool-policy snapshots pass before Grok activation.
- **GATE-003:** AGY sanitized fresh/resume/error fixture, codec dedup/accounting, and restricted-policy fail-closed tests pass before Antigravity activation.
- **GATE-004:** Backend catalog and dashboard selectors agree exactly for selectable providers.
- **GATE-005:** AgentChannels old/new/unknown round-trip and generic DB binding pass.
- **GATE-006:** Forbidden hotfile/TUI/ACP/DB migration diff is empty and frozen giant aggregate is net-negative.
- **GATE-007:** Legacy/new tool-policy provenance fixtures pass; `settings/read.rs` no longer materializes `DEFAULT_ALLOWED_TOOLS` for omitted tools; provider-default is never inferred by vector equality; new/provider-change writes always persist explicit mode.
- **GATE-008:** `counterpart()` first-item table matches §7.1 for current five providers before Grok/AGY rows land; after they land, first items stay Codex/Claude as specified and Antigravity is meeting-ineligible.

### Capability-blocking, not foundation-blocking

1. **AGY tool event schema.** Until an actual tool-call/result capture is sanitized and tested, `supports_tool_stream=false`.
2. **AGY structured output.** Until an actual `--json-schema` stream/result capture is tested, `supports_structured_output=false`.
3. **AGY restricted tool enforcement.** Until scratch-root mutation proof passes, restricted workflows fail closed.
4. **Grok/AGY context windows.** Until a reliable model-aware source exists, catalog reports unknown and UI hides numeric capacity claims.
5. **AGY non-Windows executable fallback.** Use PATH on unverified platforms; do not invent installation paths.
6. **AGY auth-file metadata.** Do not add file/env probes without a stable documented contract; opt-in live smoke remains the usability proof.

### Operational release gates

- dcserver/service host has the exact binary reachable by the service account.
- credential/auth state is usable by the service account without interactive prompts.
- fresh/resume/cancel and failure observability pass on the deployment OS.
- Grok read-only policy passes representative allowed and denied operations.
- Antigravity restricted workflows remain disabled unless their separate proof passed.
- Rollback removes provider availability from backend catalog and UI together.

No open question above authorizes an unverified capability, guessed default, silent fallback, or “fix later” implementation.

---

## 13. Ready-to-implement freeze

An implementer MAY start from this document without inventing product policy. If a choice is not listed here, it is out of scope or belongs in a spec edit first.

| Topic | Frozen choice |
|---|---|
| Commit order | §10 slices A→F only |
| Headless path | `src/services/discord/router/message_handler/headless_turn.rs` (there is no `services/discord/headless_turn.rs`) |
| StreamJson dispatch | `provider_exec::execute_streaming(ProviderTurnRequest)` |
| Claude/Codex/Qwen/OpenCode dispatch | existing functions; extras stay out of headless giants |
| Grok default Discord turn | omitted tools = ProviderDefault = §5.2 argv; usable |
| Grok meeting/restricted | ReadOnly = §5.3; usable |
| AGY default Discord turn | omitted tools = ProviderDefault = §6.2 argv; usable |
| AGY meeting/restricted | not selectable; forged request fail-closed |
| `counterpart()` first item | §7.1 table; Grok/AGY first item is Codex |
| Settings omitted tools | `Explicit(ProviderDefault)`, never `DEFAULT_ALLOWED_TOOLS` |
| AGY print-timeout | outer hard-kill − 5s, floor 30s |
| Catalog auth | same as dashboard settings GET |
| Process cancel | Unix process group required; Windows compile + direct child |
| `--verbatim`+`--rules` | capture in Slice A; drop verbatim only if live CLI rejects |
| New files | each <1000 prod lines; no hotfile/TUI/ACP/DB migration |

**Usable means:** an operator can assign a Discord agent to `grok` or `antigravity`, send a normal channel message, get a streamed reply, and resume the next message on the same session/conversation id. It does **not** mean TUI, meeting (AGY), or restricted-tool AGY.
---

