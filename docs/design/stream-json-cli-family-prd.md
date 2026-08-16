---
doc_type: "prd"
schema_version: "2"
status: "approved"
topic_slug: "stream-json-cli-family"
topic_folder: "design"
linked_spec: "./stream-json-cli-family-spec.md"
created_at: "2026-08-17"
updated_at: "2026-08-17"
target_repo: "https://github.com/kunkunGames/AgentDesk"
code_survey_date: "2026-08-17"
---

# PRD: Stream-JSON CLI 패밀리와 Grok·Antigravity(AGY) pipe 착지

## PRD 업데이트 규칙

1. 실제 저장소 코드와 현재 로컬 CLI 계약을 문서보다 우선한다.
2. Gemini/Qwen 실행 루프를 복사해 provider별 giant를 만들지 않는다.
3. 공통화 대상은 프로세스 수명주기이고, argv·session·codec·권한 정책은 dialect가 소유한다.
4. 요구사항, TASK, TEST, **커밋 순서(spec §10)** 추적성은 linked spec이 정본이다. 이 PRD의 P0-A/B/C는 제품 묶음일 뿐 구현 순서가 아니다.
5. 런타임 capability가 증명되지 않은 기능은 UI에서 숨기거나 명시적으로 거절한다. 조용한 권한 확대와 fresh-session fallback은 금지한다.
6. 인증 파일을 검사하더라도 secret 값은 읽어 출력하거나 로그에 남기지 않는다.

## 배경

- 현재 실행 provider는 `claude` / `codex` / `gemini` / `opencode` / `qwen` 다섯 개다. Gemini는 process pipe이고, Qwen은 Unix에서 managed tmux wrapper를 탄다.
- Grok는 `grok -p` + `streaming-messages-json` + UUID `--resume`으로 Discord/headless pipe를 구성할 수 있지만 AgentDesk 실행 경로가 없다.
- 대시보드에는 `antigravity`가 이미 존재하고 suffix `-ag`도 일부 onboarding 경로에 있으나, 런타임 `ProviderKind`와 registry에는 없어 현재는 선택 가능한 유령 provider다.
- 로컬 AGY는 `agy -p --output-format stream-json`과 `--conversation` resume을 제공한다. 그러나 출력은 Grok/Claude Messages JSON이 아니라 `init` / `step_update` / `result` 이벤트다.
- 현재 `ProviderExecutionAdapter`, `ProviderKind::from_str`, counterpart 목록, `AgentChannels`, doctor/onboarding/dashboard 목록이 provider 수에 비례해 반복된다. 새 provider를 enum과 수십 개 match로 추가하면 6번째 이후에도 동일한 등록 세금을 낸다.
- 현재 Discord settings는 `allowed_tools` 생략을 공용 `DEFAULT_ALLOWED_TOOLS` vector로 materialize해 호출부에 전달하므로 “provider default”와 “사용자가 명시한 allowlist”의 provenance가 소실된다. 이 벡터를 Grok/AGY에 그대로 넘기면 기본 턴이 전부 거절된다.
- `counterpart()`는 배열 첫 원소만 쓴다. 지금은 Claude→Codex다. 새 provider를 사전순으로 끼워 넣으면 미팅 리뷰어가 바뀐다.
- `provider.rs`, `gemini.rs`, `config.rs`, doctor orchestrator, Discord headless/intake는 frozen giant다. 새 기능 로직을 직접 더하는 것은 저장소 유지보수 계약에 어긋난다.

## 검증된 로컬 CLI 계약

### Grok 1.0.4

- 바이너리: `grok`; 이 호스트의 canonical fallback은 `~/.grok/bin/grok`다.
- 단발 실행: `-p/--single`, 작업 디렉터리: `--cwd`.
- 출력: `--output-format streaming-messages-json`이 Messages JSON 형태의 `system/init`, `assistant`, `user`, terminal `result`를 낸다.
- resume: `-r/--resume <ID_OR_TITLE>`; 자동화는 UUID ID를 사용한다. `-s/--session-id`는 새 UUID 생성 전용이고 기존 세션을 resume하지 않는다. `--continue`는 cwd의 최신 세션이라 채널 identity에 안전하지 않다.
- 시스템 규칙: `--rules`는 기본 system prompt에 추가된다. `--system-prompt-override`는 기본 prompt와 rules를 대체하므로 기본 AgentDesk 경로에 사용하지 않는다.
- 권한: `--tools`, `--disallowed-tools`, `--allow`, `--deny`, `--no-subagents`, `--disable-web-search`, `--no-memory`가 있다. `--yolo`는 auto-approve지만 deny 규칙과 admin lock은 유지된다.
- 인증: `~/.grok/auth.json` 또는 `XAI_API_KEY`; 값 자체는 로그/doctor 응답에 노출하지 않는다.

### AGY 1.1.13

- 제품명/canonical provider id는 `antigravity`, 실행 바이너리는 `agy`다. `agy`는 입력 alias로만 허용할 수 있다.
- 이 호스트의 binary fallback은 `%LOCALAPPDATA%\agy\bin\agy.exe`다.
- 단발 실행: `-p/--print`, 출력: `--output-format stream-json`, 작업 디렉터리: child process cwd.
- fresh 출력은 `event=init`으로 시작하고 `conversation_id`를 제공한다. 텍스트는 `event=step_update`, `step_type=agent_response`, `text_delta`에서 스트리밍되고 terminal `event=result`가 같은 `conversation_id`, status, aggregate response, usage를 제공한다.
- resume: `--conversation <conversation_id>`. 실캡처에서 같은 UUID와 `num_turns=2`가 유지됐다. `--continue`는 최신 대화 선택이라 채널 identity에 사용하지 않는다.
- `--sandbox`, `--disable-slash-commands`, `--mode plan`이 있지만 `--mode plan`과 `--disable-slash-commands`를 같이 쓰면 plan mode가 무효라는 경고가 난다.
- sandboxed 실캡처의 `init`은 넓은 tool 목록과 `permission_mode=always-proceed`를 보고했다. 따라서 `--sandbox`만으로 AgentDesk `allowed_tools` 또는 `meeting_readonly`가 보장된다고 간주하지 않는다.
- 안정된 credential 파일 계약은 이 문서에서 추정하지 않는다. `agy --version`은 설치만, `agy models`는 catalog 접근만 증명한다. 실제 인증 사용 가능성은 opt-in print smoke로 구분한다.

## 문제 정의

현재 draft의 수동 flag bag과 단일 `parse_stream_message` 접근은 다음 이유로 재사용 경계가 아니다.

- Gemini는 자체 event codec과 retry/finalization 규칙을 갖는다.
- Grok는 Messages JSON이지만 stateful multi-block 처리가 필요하다.
- AGY는 별도의 `init` / `step_update` / `result` codec, `--conversation` session 정책, prompt envelope가 필요하다.
- provider마다 system prompt, allowed tools, structured output, working directory, resume 의미가 다르다.
- 한 adapter가 한 provider id와 capabilities를 가진다는 현재 conformance invariant는 공유 실행 family와 충돌한다.

따라서 제품 목표는 “모든 CLI를 같은 flags/parser로 실행”하는 것이 아니라 다음이다.

> process runner는 provider를 모르고, provider dialect는 프로세스 수명주기를 다시 구현하지 않는다.

## 목표

- Gemini의 process lifecycle을 동작 보존 상태로 공유 `StreamJsonCli` runner로 추출한다.
- `ProviderTurnRequest`를 통해 provider execution dispatch를 한 곳으로 모은다.
- Gemini, Grok, Antigravity(AGY)가 각각 stateful dialect/codec으로 같은 runner를 사용한다.
- Grok를 resume 가능한 Discord/headless provider로 registry·doctor·onboarding·dashboard에 동시에 착지한다. **일반 채널 턴과 meeting ReadOnly가 실제로 동작해야 한다.**
- 기존 유령 `antigravity`를 canonical runtime provider로 완성하고 `agy`라는 중복 UI provider를 만들지 않는다. **일반 채널 턴이 동작해야 한다.** meeting/restricted는 P0에서 AGY를 고를 수 없다.
- 제한된 tool policy를 정확히 표현한다. Grok는 검증된 read-only mapping을 사용하고, AGY는 native enforcement가 증명될 때까지 제한 턴을 fail-closed한다.
- 설정부터 runner까지 `provider_default` / `allowlist` / `read_only` provenance를 보존해 기본 전체 목록을 explicit restriction으로 오해하지 않는다.
- `AgentChannels`와 provider catalog를 동적으로 만들어 다음 provider가 config/DB/UI의 구조 변경 없이 들어오게 한다.
- 6번째와 7번째 provider를 통해 경계를 검증하여 이후 추가 비용을 “작은 dialect + registry row + sanitized fixtures”로 제한한다.
- 신규 설정에서 TUI가 없는 provider의 `runtime: tui`를 명시적으로 거절하고, legacy 설정에만 관찰 가능한 fallback을 유지한다.

## 비목표

- Claude/Codex TUI 패리티, hook server, pane capture, idle tail.
- `grok agent` ACP 또는 AGY interactive UI embedding.
- Qwen managed tmux wrapper를 StreamJson runner에 흡수.
- Codex `exec --json` 또는 OpenCode HTTP를 같은 codec으로 통합.
- AGY tool allowlist를 근거 없이 모방하거나 sandbox를 read-only라고 가정.
- Grok/AGY 정적 model catalog를 코드에 고정. 동적 catalog는 별도 enhancement다.
- provider별 Discord channel DB 컬럼 추가 또는 schema migration.
- `ProviderKind` 전체를 문자열 newtype으로 바꾸는 전면 개편.
- 인증 token, cookie, API key, session credential 탐색·출력·복사.

## 제품 동작 계약

| Provider | Canonical id | 실행 | Resume | System prompt | 제한 tool policy |
|---|---|---|---|---|---|
| Gemini | `gemini` | 기존 process pipe 보존 | 기존 Gemini 정책 | 기존 정책 보존 | 기존 provider-default/sandbox/admin policy 보존 |
| Grok | `grok` | `streaming-messages-json` | strict UUID `--resume` | `--rules`; override 금지 | provider-default 또는 Read/Grep/Glob native allowlist; 나머지는 fail-closed |
| Antigravity | `antigravity` | AGY `stream-json` | strict captured `--conversation` token | deterministic prompt envelope | P0에서 provider-default만; explicit allowlist/read-only 요청은 증명 전 fail-closed |

- 구현 순서는 spec §10이다. settings typed policy(GATE-007) 없이 Grok/AGY catalog 행을 켜지 않는다.
- `counterpart()` 첫 항은 Claude↔Codex, 그 외 기존 provider와 Grok/AGY는 Codex. 미팅 리뷰어를 AGY로 바꾸지 않는다.
- `--continue` 계열은 어느 provider에서도 Discord channel session identity에 사용하지 않는다.
- resume 실패를 fresh session으로 자동 전환하지 않는다.
- 지원되지 않는 `allowed_tools`, structured output, remote profile은 무시하지 않고 typed error로 돌려준다.
- AGY의 tool/structured stream capability는 실제 fixture와 테스트가 있을 때만 registry에서 true다.

## 기능 요구사항

- [ ] Gemini process-pipe의 spawn/read/stderr/watchdog/cancel/retry가 공통 runner로 이동하고 기존 `StreamMessage` 시퀀스가 보존된다.
- [ ] Gemini/Grok/Antigravity는 `StreamJsonCli(dialect)` family 한 dispatch를 통과한다.
- [ ] Grok가 strict UUID resume, stateful Messages codec, system rules, tool policy와 함께 런타임/UI에 착지한다.
- [ ] Antigravity가 AGY 전용 codec과 `--conversation` resume으로 런타임/UI에 착지한다.
- [ ] AGY restricted turn은 native policy가 증명되지 않은 한 사용자에게 명확한 unsupported 오류를 반환한다.
- [ ] legacy `allowed_tools`를 typed policy로 정규화하고 신규 config/API write는 policy mode를 명시한다.
- [ ] `AgentChannels`는 provider key map이며 unknown key를 침묵 유실하지 않는다.
- [ ] backend registry가 dashboard selectable provider catalog의 권위다.
- [ ] doctor는 설치, credential metadata, catalog reachability, opt-in live usability를 구분한다.
- [ ] TUI가 없는 provider의 신규 TUI 설정은 validation에서 거절되고 legacy fallback은 이유를 남긴다.

## 비기능 요구사항

- 응집도: runner는 process lifecycle만, dialect는 argv/session/codec/policy만 소유한다.
- 재활용: 새 dialect가 runner, dispatch, AgentChannels 구조를 수정하면 conformance 실패다.
- 안정성: stdout/stderr bound, line-size limit, startup/idle timeout, child-tree cancel, temp artifact cleanup을 테스트한다.
- 보안: tool policy 변환 실패는 fail-closed이고 secret 값은 command/debug/doctor 출력에서 redaction된다.
- 유지보수성: 신규 production 파일은 각각 1000줄 미만이며, touched frozen giant의 production 줄 수는 aggregate net-negative다.
- 직관성: UI는 backend가 실제 실행할 수 있는 provider만 신규 선택지로 보여 주고 capability 제한을 함께 표시한다.
- 운영 정직성: unit/fixture green, local authenticated smoke, Unix service-host smoke, production-ready를 서로 구분한다.

## 구현 범위와 단계

### P0 제품 묶음 (커밋 순서는 spec §10 A→F)

- [ ] Slice A: Gemini golden + Grok/AGY 실캡처. `--verbatim`+`--rules` 동시 사용을 확인한다.
- [ ] Slice B: registry, frozen counterpart 첫 항, AgentChannels map, settings `ConfiguredToolPolicy`, catalog API(기존 5개).
- [ ] Slice C: runner + Gemini 이전. headless extras 빌더 금지.
- [ ] Slice D: Grok 일반 턴 + meeting ReadOnly 사용 가능.
- [ ] Slice E: Antigravity 일반 턴 사용 가능, meeting 제외.
- [ ] Slice F: surface audit / inventory / ratchet.

### P1 — 검증된 확장

- [ ] AGY restricted tool policy를 scratch-root mutation test로 증명한 뒤 capability를 켠다.
- [ ] Grok/AGY model catalog와 update strategy를 provider-specific owner module에 추가한다.
- [ ] Qwen process-only 경로가 확인되면 managed wrapper를 유지한 채 runner 재사용을 검토한다.
- [ ] Grok/AGY TUI는 pane/transcript/readiness 계약을 별도로 촬영한 뒤 다른 execution family로만 검토한다.

## 검증 계획

### 자동 검증

- Gemini: 추출 전후 argv, stream event, retry, timeout, cancel, finalization golden parity.
- Runner: fake CLI로 NDJSON, stderr, malformed/oversized line, non-zero exit, timeout, cancel child tree, retry classification.
- Grok: actual sanitized Messages fixture, multi text/tool block, read-only tool mapping, strict resume, no fresh fallback.
- AGY: actual sanitized fresh/resume fixture, delta/aggregate dedup, usage normalization, invalid conversation, policy fail-closed.
- Registry: canonical id/alias uniqueness, family/dialect/capability invariant, frozen `counterpart()` first-item (Claude↔Codex), remaining list stable-sorted.
- Config/DB: legacy five keys와 `grok`/`antigravity` round-trip, unknown preservation, generic primary channel binding.
- Tool policy config: raw 생략/legacy explicit vector/explicit mode/empty/meeting override가 provenance를 잃지 않고 정규화되며 vector equality로 권한을 추정하지 않는다.
- UI: `/api/providers` selectable ids와 form/profile/onboarding selector 일치, `copilot`/`api` legacy-only 처리.
- Maintenance: forbidden hotfile diff empty, frozen giant aggregate net-negative, 신규 파일 production <1000.

### 수동/운영 검증

- Grok: supported Unix service host에서 version, auth metadata, fresh/resume, read-only, cancel, invalid resume.
- AGY: 설치 OS별 binary resolution, model catalog reachability, authenticated fresh/resume, cancel, restricted-turn rejection.
- 신규 `runtime: tui` config가 명확히 거절되고 legacy 설정만 fallback reason을 남기는지 확인.
- live smoke는 opt-in으로 실행하고 비용·외부 side effect를 명시한다.

## 성공 지표

- Grok와 Antigravity를 추가해도 runner/process lifecycle 코드는 provider별로 복제되지 않는다.
- 이후 stream-JSON CLI provider 추가 시 수정 범위가 dialect, registry, sanitized fixtures, presentation metadata로 제한된다.
- provider별 dispatch match, counterpart 전수 배열, named channel field, dashboard 수동 provider 목록이 사라진다.
- restricted tool 요청이 provider-default 실행으로 확대되는 경로가 없다.
- provider 기본 tool surface와 사용자 explicit allowlist가 config/API/runner 전 구간에서 구분된다.
- 런타임이 없는 provider가 신규 UI 선택지로 노출되지 않는다.
- Gemini 회귀가 없고 기존 config/DB가 migration 없이 동작한다.

## 리스크와 롤백

- Gemini 추출 회귀: characterization golden이 깨지면 Grok/AGY 작업보다 먼저 runner slice를 롤백한다.
- Grok protocol drift: version과 raw fixture를 함께 기록하고 unknown field는 허용하되 terminal/session invariant는 fail-closed한다.
- AGY protocol/permission drift: `antigravity` registry row와 UI 활성화를 함께 비활성화한다. 공통 runner와 Gemini/Grok는 유지한다.
- `AgentChannels` migration 회귀: old five-key fixtures와 unknown-key round-trip을 merge gate로 둔다.
- tool policy migration 회귀: raw 생략은 provider-default로, mode 없는 명시 vector는 legacy compatibility로 보존하고 신규/provider 변경 write에는 explicit mode를 기록하는 fixture를 merge gate로 둔다.
- backend catalog 장애: dashboard는 캐시된 legacy presentation으로 기존 값을 표시할 수 있지만 신규 provider 생성은 catalog 없이는 막는다.
- 운영 rollback은 provider registry row와 dashboard availability를 같은 단위로 내린다. DB rollback은 필요하지 않다.
---
