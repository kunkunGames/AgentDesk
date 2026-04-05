# OpenClaw Migration Contract

이 문서는 OpenClaw migrate PRD를 기준으로 정리한 AgentDesk의 OpenClaw migrate 계약 문서다. 구현 설명만 적는 문서가 아니라, v1 목표와 현재 구현 상태를 함께 고정한다.

기준 코드 경로:

- AgentDesk repo root
- 현재 migration 구현 워킹트리
- 구현 확인 파일:
  - `src/cli/migrate.rs`
  - `src/cli/migrate/source.rs`
  - `src/cli/migrate/plan.rs`
  - `src/cli/migrate/apply.rs`
  - `src/cli/migrate/tests.rs`

기준 날짜: `2026-04-03`

## 0. 운영 검증 업데이트

2026-04-03 실제 OpenClaw import 검증에서 아래 항목을 추가 요구사항으로 확정했다.

- Codex provider만 맞추는 것으로는 충분하지 않다.
  - source model이 `openai-codex/gpt-5.3-codex-spark`처럼 들어오면 provider=`codex`, model=`gpt-5.3-codex-spark`로 분해해야 한다.
  - prefixed source model string을 runtime override로 그대로 남기면 안 되지만, 이미 live-safe한 Codex slug까지 임의 downgrade하면 안 된다.
- `--write-org --with-channel-bindings --write-bot-settings`를 함께 쓸 때는 org/bot channel state가 반드시 같아야 한다.
  - bot_settings 쪽 `allowed_channel_ids`를 만들지 못한 channel은 `org.yaml`에도 live로 쓰면 안 된다.
- role id 충돌로 `openclaw-<id>`가 생성될 때 예전 unprefixed prompt/memory/workspace가 남아 있으면 audit와 manual-followup에 반드시 남겨야 한다.
  - 실제로 이 잔재 때문에 stale tmux/session이 예전 workspace를 계속 복원하는 문제가 확인됐다.
- 이미 import된 runtime에 다시 migrate를 돌릴 때는 기존 `openclaw-<id>`를 재사용해야 한다.
  - 그렇지 않으면 `openclaw-foodie`가 있던 환경에서 다음 dry-run이 다시 `foodie`를 새 role id로 잡는 역회귀가 생긴다.
- `--write-db`가 agent row만 만들고 끝나면 Office dashboard에는 안 보일 수 있다.
  - AgentDesk Office 화면과 office-scoped stats는 `office_agents`를 기준으로 agent를 읽는다.
  - runtime DB에 office가 정확히 1개면 imported agent를 그 office에 자동 연결한다.
  - runtime DB에 office가 0개면 `openclaw-import` Office를 새로 만들고 imported agent를 거기에 자동 연결한다.
  - runtime DB에 office가 여러 개면 기존 `openclaw-import` Office를 재사용하거나 새로 만든 뒤 imported agent를 거기에 자동 연결한다.
- imported agent metadata도 dashboard-safe해야 한다.
  - `agentdesk.yaml` write 시 `name_ko`는 imported display name으로 기본 채운다.
  - dashboard 쪽은 legacy row를 위해 `name_ko`와 `avatar_emoji`가 비어 있어도 검색/렌더링이 깨지지 않아야 한다.
- session import는 여전히 opt-in이고 archival 성격이다.
  - `--with-sessions`가 없으면 세션은 안 옮겨진다.
  - 현재 AgentDesk Discord runtime은 imported `ai_sessions/*.json`만으로 active turn을 복원하지 않는다.

2026-04-03 기준 현재 구현에서는 위 요구사항 중 rerun role id 재사용, Office auto-link, `name_ko` 기본 채움, `manual-followup.md` 생성까지는 구현과 테스트가 들어가 있다. 남아 있는 핵심 비범위/후속 과제는 non-Discord secret target spec, 더 넓은 tool-policy fixture coverage, 그리고 imported session의 live Discord turn 복원이다.

## 1. 목표

`agentdesk migrate openclaw [root_path]`는 OpenClaw의 durable agent 데이터를 AgentDesk 런타임으로 안전하게 가져오는 CLI다.

v1의 핵심 목표:

- OpenClaw agent id, name, emoji, provider, workspace를 AgentDesk role로 안전하게 매핑
- OpenClaw workspace bootstrap 파일을 AgentDesk per-role prompt로 병합
- OpenClaw Markdown memory를 AgentDesk `role-context/<role>.memory/`로 이관
- OpenClaw workspace를 AgentDesk 관리 하위로 snapshot copy
- representable한 Discord channel binding을 `config/org.yaml`로 이관
- live write 전후 상태를 audit artifact와 resume state로 남김

## 2. v1 범위

### v1에서 항상 계획에 포함되는 항목

- agent 선택과 role id 충돌 해결
- provider 매핑
- prompt 생성 계획
- memory import 계획
- workspace copy 계획
- Discord account/channel binding 분석
- tool policy 분석
- audit artifact 생성 계획

### v1에서 명시 플래그가 있을 때만 live write 되는 항목

- `agentdesk.yaml`
- `prompts/agents/<role>/IDENTITY.md`
- `role-context/<role>.memory/*.md`
- `$AGENTDESK_ROOT_DIR/openclaw/workspaces/<role>/`
- `config/org.yaml` (`--write-org`)
- `config/org.yaml`의 `channels.by_id` (`--write-org --with-channel-bindings`)
- DB upsert (`--write-db`)
  - `agents` sync
  - office auto-link
    - office 1개: 기존 office에 연결
    - office 0개: `openclaw-import` Office 생성 후 연결
    - office 여러 개: `openclaw-import` Office 재사용 또는 생성 후 연결
  - `--with-sessions`일 때 `sessions` upsert
- session artifact (`--with-sessions`)
- `config/bot_settings.json` (`--write-bot-settings`)

### v1 비목표

- OpenClaw vector-memory SQLite import
- hook pack live 변환
- pairing / allowFrom / device trust live import
- auth profile / credential live import
- SecretRef를 자동으로 plaintext bot token으로 강등하는 동작
- role / sender allowlist 기반 Discord routing의 무리한 1:1 변환
- OpenClaw skill/hook을 AgentDesk first-class runtime asset으로 직접 이관

## 3. CLI 계약

PRD 기준 추천 명령:

`agentdesk migrate openclaw [root_path]`

### PRD 기준 v1 플래그

- `--agentdesk-root <path>`
- `--agent <id>`
- `--all-agents`
- `--dry-run`
- `--resume <import_id>`
- `--overwrite`
- `--write-db`
- `--write-org`
- `--write-bot-settings`
- `--no-workspace`
- `--no-memory`
- `--no-prompts`
- `--with-sessions`
- `--tool-policy-mode <report|bot-intersection|bot-union>`
- `--discord-token-mode <report|plaintext-only|resolve-env>`
- `--with-channel-bindings`
- `--fallback-provider <claude|codex|gemini|qwen>`
- `--snapshot-source`

### 현재 브랜치에서 이미 노출된 플래그

- `--agentdesk-root`
- `--agent`
- `--all-agents`
- `--dry-run`
- `--resume`
- `--overwrite`
- `--write-db`
- `--write-org`
- `--write-bot-settings`
- `--no-workspace`
- `--no-memory`
- `--no-prompts`
- `--with-sessions`
- `--tool-policy-mode`
- `--discord-token-mode`
- `--with-channel-bindings`
- `--fallback-provider`
- `--snapshot-source`

### 현재 브랜치의 추가 플래그

- `--workspace-root-rewrite <OLD=NEW>`

### 현재 live apply 범위

현재 브랜치는 아래를 실제로 live apply한다.

- 기본 file import
- `--no-memory` / `--no-prompts` / `--no-workspace`로 role별 파일 쓰기 범위 제어
- `--write-org`
- `--write-org --with-channel-bindings`에서 representable binding 반영
- `--write-bot-settings`
  - `--discord-token-mode report`면 audit-only
  - `--discord-token-mode plaintext-only`면 literal token만 live import
  - `--discord-token-mode resolve-env`면 env-backed SecretRef만 live import
  - file/exec SecretRef는 현재도 audit + `manual-followup.md` 대상으로 남긴다.
- `--tool-policy-mode report|bot-intersection|bot-union`
- `--write-db`
- `--write-db`에서 `office_agents` auto-link와 `openclaw-import` Office 정책
- `--write-db --with-sessions`
- `--snapshot-source`
- `--resume`
- rerun 시 기존 `openclaw-<id>` 재사용
- `agentdesk.yaml` write 시 `name_ko` 기본값 주입
- `manual-followup.md`

현재 남아 있는 실질 제약은 아래다.

- session import는 여전히 archival 성격이다.
- 현재 Discord token live import는 `report|plaintext-only|resolve-env`까지만 노출되어 있고, file/exec SecretRef와 non-Discord secret surface는 manual follow-up 범위다.
- intersection/union tool collapse는 동작하지만 report 모드보다 fixture coverage가 아직 얕다.
- imported `ai_sessions/*.json`는 현재 live Discord turn 복원까지 연결되지 않는다.

## 4. 입력 계약

`root_path`는 미리 검증된 OpenClaw state dir만 받는 것이 아니라, 검색 시작점으로도 동작해야 한다.

허용 입력:

- 인자 없음: 현재 디렉터리에서 검색
- `openclaw.json` 직접 경로
- 디렉터리 경로

소스 탐색 규칙:

1. 파일 인자이고 이름이 `openclaw.json`이면 부모 디렉터리를 루트로 사용
2. 디렉터리 인자면 하위에서 `openclaw.json`을 재귀 탐색
3. 유효 후보가 1개면 채택
4. 유효 후보가 여러 개면 실패
5. 유효 후보가 없으면 실패

현재 구현의 scan prune 대상:

- `.git`
- `node_modules`
- `target`
- `dist`
- `.venv`
- `.cache`
- `$AGENTDESK_ROOT_DIR/openclaw/imports/` 하위

유효한 OpenClaw 루트 조건:

- `openclaw.json` 존재
- `agents/` 디렉터리 존재
- resolved config 기준 `agents.list` 비어 있지 않음

### OpenClaw config 파싱 계약

PRD 기준으로 importer는 아래를 만족해야 한다.

- `openclaw.json`은 strict JSON이 아니라 JSON5로 파싱
- `$include`를 먼저 resolve한 뒤 planning
- include provenance를 manifest / fingerprint에 반영
- `agents.list[].provider`를 가정하지 않음
- `agents.list[].model`은 string 또는 `{ primary, fallbacks }` union 지원
- provider는 runtime/model hint에서 유도

현재 브랜치는 위 조건을 이미 구현했다.

## 5. Canonical ImportPlan

PRD의 핵심 원칙은 source discovery와 apply가 같은 결정을 공유해야 한다는 점이다.

`ImportPlan`이 소유해야 하는 결정:

- selected source agents
- final AgentDesk role ids
- provider mapping
- workspace source / destination
- Discord account 선택
- concrete channel id derivation
- live 적용 가능 / preview-only 판정
- prompt / memory / workspace write 계획
- audit artifact payload

필수 규칙:

- `--dry-run`은 live apply가 사용할 동일한 계획을 출력해야 함
- `write-plan.json`은 canonical `ImportPlan`의 serialize 결과여야 함
- writer는 source tree를 다시 스캔하면 안 됨
- Discord account / binding 결정은 `plan`만 소비해야 함

현재 브랜치는 이 원칙을 반영해 `plan`이 Discord binding/account 결정을 직접 들고 있고, `apply`는 이를 다시 계산하지 않는다.

## 6. 매핑 규칙

### Agent metadata

- `agents.list[].id` -> AgentDesk role id
- `agents.list[].name` -> display name, `name_ko` 기본값
- `agents.list[].identity.emoji` -> `avatar_emoji`
- runtime/model hint -> provider
- resolved workspace -> copied workspace target와 `org.yaml` workspace

### Role ID 충돌 규칙

- 기본은 source id 유지
- 충돌 시 `openclaw-<source-id>`
- 계속 충돌하면 suffix 증가

### Provider 결정 규칙

현재/PRD 공통 우선순위:

1. `runtime.provider`
2. `runtime.agent`
3. `runtime.acp.agent`
4. `runtime.acp.backend`
5. `model.primary` 또는 `model`
6. `--fallback-provider`

현재 기본 매핑:

- `anthropic/*` / `anthropic` -> `claude`
- `openai/*` / `openai-codex/*` / `openai` / `codex` -> `codex`
- `google/*` / `google` / `gemini` -> `gemini`

Codex model slug 규칙:

- `openai/*` / `openai-codex/*` / `codex/*` prefix는 provider 판정 후 제거
- 남는 값이 target model이다.
- 예: `openai-codex/gpt-5.3-codex-spark` -> provider=`codex`, model=`gpt-5.3-codex-spark`
- live-safe한 Codex slug는 임의로 다른 모델로 downgrade하지 않는다.

provider를 결정하지 못하면:

- dry-run: warning + preview
- live apply: 중단

### Workspace 결정 규칙

우선순위:

1. `agents.list[].workspace`
2. `agents.defaults.workspace`
3. default agent면 `<root>/workspace`
4. 그 외 `<root>/workspace-<agentId>`

기본 정책:

- live import는 외부 OpenClaw 원본 경로를 직접 가리키지 않음
- copied workspace를 `$AGENTDESK_ROOT_DIR/openclaw/workspaces/<role>/`로 생성

### DB / Office visibility 규칙

- `--write-db`는 `agentdesk.yaml` merge 결과를 기준으로 `agents` 테이블을 sync한다.
- `--with-sessions`도 함께 켜졌을 때만 imported session을 `sessions` 테이블에 upsert한다.
- AgentDesk dashboard의 office-scoped agent 목록과 stats는 `office_agents`를 기준으로 필터링한다.
- 현재 구현은 imported agent를 office에 자동 연결한다.
- office가 0개면 `openclaw-import` Office를 만들고, office가 여러 개면 기존 또는 새 `openclaw-import` Office에 연결한다.
- imported agent metadata에는 `name_ko` 기본값과 `avatar_emoji`를 함께 채운다.
- dashboard reader/API client는 legacy `name_ko`/`avatar_emoji` null 값을 fallback normalization으로 처리한다.
- department는 source에서 안전하게 유도할 수 없으므로 자동 배정하지 않는다.

## 7. Prompt / Memory 전략

### Prompt

OpenClaw bootstrap 파일은 memory가 아니라 AgentDesk per-role prompt로 병합한다.

안정적인 병합 순서:

1. imported role summary
2. `IDENTITY.md`
3. `AGENTS.md`
4. `SOUL.md`
5. `USER.md`
6. `TOOLS.md`
7. `BOOT.md`
8. `BOOTSTRAP.md`
9. `HEARTBEAT.md`

현재 브랜치는 위 순서를 구현하고 있다.

### Memory

PRD 기준 memory 정책:

- `MEMORY.md`는 그대로 복사
- `memory/YYYY-MM-DD.md`는 top-level `daily-YYYY-MM-DD.md`로 flatten
- flattened daily file에는 source provenance frontmatter 추가
- OpenClaw `memory/<agentId>.sqlite`는 가져오지 않음

현재 브랜치는 Markdown memory import와 daily flatten을 구현하고 있다.

## 8. Discord / Tool Policy 계약

### Tool policy

PRD 기본값은 `--tool-policy-mode report`다.

이유:

- AgentDesk는 bot-level `allowed_tools`만 지원
- OpenClaw는 global / agent / provider / guild / channel / sender까지 layered policy를 가짐
- 따라서 v1 기본 동작은 live collapse가 아니라 report-only가 안전함

현재 브랜치 상태:

- tool policy를 scan해서 `tool-policy-report.json`에 남김
- sender-scoped policy(`toolsBySender`)는 report-only warning 처리
- `--tool-policy-mode report`는 기존 `bot_settings.json.allowed_tools`를 보존한다.
- `--tool-policy-mode bot-intersection|bot-union` live apply 경로는 구현돼 있다.
- `--tool-policy-mode bot-intersection`에서 sender-scoped policy가 섞인 account는 `allowed_tools`만 건너뛰는 것이 아니라 해당 `bot_settings` entry 자체를 skip한다.
- explicit allowlist를 인식하지 못하거나 intersection 결과가 비면 skip하지 않고 AgentDesk 기본 `allowed_tools`로 fallback한다.
- 다만 intersection/union 조합은 report mode보다 테스트 커버리지가 아직 얕다.

### Discord account / token / binding

PRD 기준 Discord 쪽은 세 층으로 나뉜다.

1. account 선택
2. token import 가능성
3. concrete channel binding import 가능성

현재 브랜치가 이미 구현한 규칙:

- `channels.discord.token`, `channels.discord.defaultAccount`, `channels.discord.accounts.*.token`을 읽음
- representable한 binding만 live 후보로 계산
- `match.accountId`가 없고 `defaultAccount` 또는 단일 importable account로 수렴하지 않으면 preview-only
- "첫 번째 account 자동 선택"은 금지
- binding conflict가 있으면 preview-only
- 하나의 Discord account에 여러 provider가 매핑되면 해당 account의 `bot_settings` import는 skip한다.
- `--with-channel-bindings`만 있고 `--write-org`가 없으면 preview-only

PRD 기준으로 아직 남은 항목:

- non-Discord secret surface용 explicit target spec
- `bot-intersection` / `bot-union` 경로의 더 넓은 fixture coverage
- file/exec SecretRef를 live import 대상으로 확장할지에 대한 별도 CLI/spec 결정

### 현재 live import 가능한 binding 정의

현재 구현에서 live 후보가 되려면 아래를 만족해야 한다.

- `bindings[].type`이 없거나 `route`
- `match.channel == "discord"`
- `match.peer` 없음
- `match.guildId` 없음
- `match.teamId` 없음
- `match.roles` 비어 있음
- effective guild/channel entry에 sender-scoped tool policy 없음
- effective guild/channel entry에 channel-level system prompt 없음

그 외는 모두 preview-only다.

## 9. 출력 레이아웃

### 일반 runtime 출력

- `$AGENTDESK_ROOT_DIR/agentdesk.yaml`
- `$AGENTDESK_ROOT_DIR/config/org.yaml`
- `$AGENTDESK_ROOT_DIR/config/bot_settings.json`
- `$AGENTDESK_ROOT_DIR/prompts/agents/<role>/IDENTITY.md`
- `$AGENTDESK_ROOT_DIR/role-context/<role>.memory/*.md`
- `$AGENTDESK_ROOT_DIR/ai_sessions/<session_id>.json`
- `$AGENTDESK_ROOT_DIR/data/agentdesk.sqlite`
- `$AGENTDESK_ROOT_DIR/openclaw/workspaces/<role>/`

### audit 출력

- `$AGENTDESK_ROOT_DIR/openclaw/imports/<import_id>/manifest.json`
- `$AGENTDESK_ROOT_DIR/openclaw/imports/<import_id>/agent-map.json`
- `$AGENTDESK_ROOT_DIR/openclaw/imports/<import_id>/write-plan.json`
- `$AGENTDESK_ROOT_DIR/openclaw/imports/<import_id>/apply-result.json`
- `$AGENTDESK_ROOT_DIR/openclaw/imports/<import_id>/resume-state.json`
- `$AGENTDESK_ROOT_DIR/openclaw/imports/<import_id>/warnings.txt`
- `$AGENTDESK_ROOT_DIR/openclaw/imports/<import_id>/tool-policy-report.json`
- `$AGENTDESK_ROOT_DIR/openclaw/imports/<import_id>/discord-auth-report.json`
- `$AGENTDESK_ROOT_DIR/openclaw/imports/<import_id>/channel-binding-preview.yaml`
- `$AGENTDESK_ROOT_DIR/openclaw/imports/<import_id>/session-map.json` (`--with-sessions`)
- `$AGENTDESK_ROOT_DIR/openclaw/imports/<import_id>/snapshot/` (`--snapshot-source`)
- `$AGENTDESK_ROOT_DIR/openclaw/imports/<import_id>/backups/` (`--overwrite`)

현재 브랜치에서 이미 실제로 쓰는 audit 파일:

- `manifest.json`
- `agent-map.json`
- `write-plan.json`
- `apply-result.json`
- `resume-state.json`
- `warnings.txt`
- `tool-policy-report.json`
- `discord-auth-report.json`
- `channel-binding-preview.yaml`
- `manual-followup.md`
- `session-map.json` (`--with-sessions`)
- `snapshot/` (`--snapshot-source`)
- `backups/` (`--overwrite` 또는 `--resume`)

## 10. Dry-run / Resume / Safety

### Dry-run 계약

PRD 기준 dry-run은 파일을 하나도 만들지 않아야 한다.

- prompt 생성 안 함
- memory write 안 함
- workspace copy 안 함
- audit 파일 write 안 함
- runtime config mutation 안 함
- DB mutation 안 함

대신 stdout에 structured `ImportPlan`을 출력해야 한다.

현재 브랜치는 이 계약을 따른다.

### Resume / checkpoint 계약

PRD 기준 필수:

- phase마다 checkpoint 갱신
- agent task마다 checkpoint 갱신
- 실패 시에도 `apply-result.json`, `resume-state.json` 유효해야 함
- `source_fingerprint`는 실제 사용된 source 입력 집합을 해시해야 함
- `--resume <import_id>`는 완료된 phase/task를 다시 실행하면 안 됨

현재 브랜치 상태:

- `apply-result.json`, `resume-state.json`를 apply 도중 갱신
- `source_fingerprint`에 resolved config와 실제 읽은 prompt/memory/workspace 입력 반영
- `--resume` CLI가 있고, `write-plan.json`/`resume-state.json`에서 source path와 flags를 복원한다.
- `--resume`는 strict replay가 아니라 저장된 requested flags를 현재 CLI 인자와 병합한다.
  - boolean 플래그는 OR로 합쳐진다.
  - `tool_policy_mode` / `discord_token_mode`는 현재 값이 default일 때 저장값으로 복원된다.
- 완료된 phase/task를 건너뛰는 resume 경로는 테스트로 검증돼 있다.

## 11. 현재 브랜치 구현 상태

### 이미 PRD에 맞춘 항목

- JSON5 `openclaw.json` 파싱
- `$include` 해석
- `model` string / structured union 지원
- `agents.defaults.workspace` 반영
- `--workspace-root-rewrite` 기반 absolute workspace remap
- multi-candidate source discovery ambiguity fail-close
- default agent / legacy `defaultAgent` 선택 규칙
- provider mapping과 fallback warning
- prompt / memory / workspace import
- `--no-memory` / `--no-prompts` / `--no-workspace` 제어
- canonical `ImportPlan` 기반 Discord account/binding 결정
- "첫 계정 임의 선택" 제거
- `tool-policy-report.json`
- `discord-auth-report.json`
- `channel-binding-preview.yaml`
- `config/bot_settings.json` live write
- `--discord-token-mode report|plaintext-only|resolve-env`
- `--tool-policy-mode report|bot-intersection|bot-union`
- `--write-db` + optional `sessions` upsert
- rerun 시 기존 `openclaw-<source-id>` 재사용
- `office_agents` auto-link와 `openclaw-import` Office 정책
- imported agent `name_ko` 기본값 주입
- dashboard API client의 legacy `name_ko`/`avatar_emoji` fallback normalization
- operator-facing `manual-followup.md`
- `session-map.json`
- `snapshot/` / `backups/`
- phase/task 기반 `apply-result.json`, `resume-state.json`
- `--resume`

### 아직 PRD 대비 미완료인 항목

- non-Discord secret surface용 explicit target spec
- intersection/union tool collapse의 더 넓은 fixture coverage
- file/exec SecretRef를 live import 대상으로 다시 열지 여부에 대한 제품 결정
- imported session이 live Discord turn을 복원하는 경로

### 문서에 고정하는 추가 4개

현재 브랜치 기준으로 문서가 계속 추적해야 하는 후속 작업은 아래 4개다.

1. non-Discord secret surface용 explicit target spec
2. `bot-intersection` / `bot-union` 경로의 fixture coverage 확대
3. file/exec SecretRef를 live import 대상으로 다시 열지 여부에 대한 제품 결정
4. imported `ai_sessions/*.json`를 live Discord turn 복원까지 연결할지 여부

## 12. 검증 기준

이 문서는 아래를 기준으로 유지한다.

- PRD: OpenClaw migrate PRD
- 코드: `src/cli/migrate.rs`, `src/cli/migrate/*.rs`
- 테스트: `src/cli/migrate/tests.rs`

2026-04-03 기준 확인된 검증:

- `cargo test migrate --package agentdesk`
- `cargo check --all-targets`

향후 구현은 이 문서의 “PRD 기준 v1 계약”을 우선 따라가고, “현재 브랜치 구현 상태”는 그 아래에서 주기적으로 갱신한다.
