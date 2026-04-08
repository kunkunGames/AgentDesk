# OpenClaw Migration

`agentdesk migrate openclaw`는 OpenClaw의 durable state를 AgentDesk 런타임으로 이관하기 위한 CLI입니다.

## 지원 범위

- `openclaw.json` 또는 그 상위 루트 탐색
- JSON5 + `$include` 해석
- 기본/default agent 또는 명시 agent 선택
- provider/model 힌트 해석과 fallback provider 매핑
- 프롬프트 부트스트랩 파일 병합
- `MEMORY.md`, 일자별 memory markdown, workspace 복사
- 생성된 AgentDesk prompt에 migrated memory/workspace 경로 고정
- Discord channel binding 미리보기와 `org.yaml` 반영
- `bot_settings.json` 토큰/허용 채널 반영
- `ai_sessions` 및 DB 세션 이관
- audit 산출물 생성과 `--resume` 재개

## 기본 사용 예시

Dry-run으로 import plan만 확인:

`agentdesk migrate openclaw /path/to/openclaw --dry-run`

선택한 agent 하나를 실제 반영:

`agentdesk migrate openclaw /path/to/openclaw --agent alpha --write-org --write-db`

channel binding, bot 설정, 세션까지 함께 이관:

`agentdesk migrate openclaw /path/to/openclaw --all-agents --write-org --write-bot-settings --write-db --with-channel-bindings --with-sessions --snapshot-source`

중단된 import 재개:

`agentdesk migrate openclaw --resume <import_id> --write-org --write-db`

## 주요 동작

1. 소스 루트와 `openclaw.json`을 해석합니다.
2. agent 선택 규칙에 따라 import 대상을 확정합니다.
3. provider/workspace 유효성을 검사하고 import plan을 만듭니다.
4. apply 시 `AGENTDESK_ROOT_DIR/openclaw/imports/<import_id>` 아래에 audit 결과를 남깁니다.
5. 선택된 경우 prompt/memory/workspace/org/bot_settings/session/DB를 순서대로 반영합니다.

## Audit 출력

아래 파일이 import audit 루트에 생성됩니다.

- `manifest.json`
- `agent-map.json`
- `write-plan.json`
- `apply-result.json`
- `resume-state.json`
- `warnings.txt`
- `tool-policy-report.json`
- `discord-auth-report.json`
- `channel-binding-preview.yaml`

이 audit 산출물은 일회성 임시 파일이 아니라 `--resume`과 사후 검증을 위한 durable provenance입니다. migrate가 끝나도 기본적으로 그대로 남습니다.

## 프롬프트/메모리 입력

다음 bootstrap 파일이 있으면 AgentDesk prompt로 병합합니다.

- `IDENTITY.md`
- `AGENTS.md`
- `SOUL.md`
- `USER.md`
- `TOOLS.md`
- `BOOT.md`
- `BOOTSTRAP.md`
- `HEARTBEAT.md`

추가로 `MEMORY.md`와 `memory/` 아래 markdown 파일을 AgentDesk memory 구조로 복사합니다.

생성된 `prompts/agents/<role>/IDENTITY.md` 상단에는 아래 migrated runtime 참조가 함께 기록됩니다.

- `role-context/<role>.memory/` 경로
- `openclaw/workspaces/<role>/` 경로
- 원본 OpenClaw workspace 경로

운영 기준은 AgentDesk가 생성한 memory/workspace 경로이며, 원본 OpenClaw 경로는 provenance로만 남깁니다. `--no-memory` 또는 `--no-workspace`를 쓴 경우에는 prompt에 해당 상태가 그대로 표시됩니다.

## 이관 후 남는 데이터

migrate는 OpenClaw 흔적을 완전히 제거하는 cleanup 도구가 아니라, AgentDesk 런타임으로 state를 옮기면서 provenance를 남기는 import 도구입니다.

- `AGENTDESK_ROOT_DIR/openclaw/imports/<import_id>/` 아래 audit 파일
- `AGENTDESK_ROOT_DIR/openclaw/workspaces/<role>/` 아래 복사된 workspace
- prompt 상단의 migrated runtime 참조와 원본 source path
- 세션/메모리 레코드의 `imported_from`, `source_session_id`, `source_path` 계열 provenance 메타데이터

따라서 "이관 완료"와 "OpenClaw 관련 흔적 삭제 완료"는 같은 의미가 아닙니다. 후자가 필요하면 import 검증이 끝난 뒤 별도의 정리 정책을 운영해야 합니다.

## 주요 플래그

- `--agent <id>`: 특정 agent만 선택
- `--all-agents`: 모든 source agent 선택
- `--agentdesk-root <path>`: 대상 AgentDesk runtime root override
- `--fallback-provider <provider>`: 미지원 source provider를 강제 매핑
- `--workspace-root-rewrite OLD=NEW`: 절대 workspace prefix 재작성
- `--write-org`: `config/org.yaml` 반영
- `--write-bot-settings`: `config/bot_settings.json` 반영
- `--write-db`: SQLite upsert 반영
- `--with-channel-bindings`: Discord channel binding import 적용
- `--with-sessions`: 세션 이관 활성화
- `--snapshot-source`: source tree snapshot 남김
- `--overwrite`: 기존 role/channel binding 덮어쓰기
- `--resume <import_id>`: audit 상태를 기준으로 재개

## 주의 사항

- multi-agent source인데 기본 agent 표기가 없으면 `--agent` 또는 `--all-agents`가 필요합니다.
- 같은 source를 다시 migrate할 때 runtime에 기존 `openclaw-<id>` role이 있으면 새 bare id 대신 그 role id를 재사용합니다.
- `--resume <import_id>`는 현재 runtime 상태로 role id를 다시 계산하지 않고, audit의 `agent-map.json`에 저장된 source→role 매핑을 그대로 재사용합니다.
- Windows source에서 넘어온 `C:\...` 또는 `D:/...` workspace 경로는 절대경로로 유지한 뒤, 필요하면 `--workspace-root-rewrite`로 현 환경 경로에 맞춰 바꿔야 합니다.
- workspace가 없거나 provider 매핑이 불가능한 agent는 audit에는 남지만 apply에서 건너뜁니다.
- token/tool policy는 기본적으로 `report` 모드이며, 실제 쓰기 전 dry-run과 audit 결과를 먼저 확인하는 것이 안전합니다.
- macOS처럼 symlink 경로와 realpath가 다를 수 있는 환경에서는 `config/org.yaml`, channel binding, DB `sessions.cwd`가 하나의 canonical path를 쓰도록 맞추는 편이 안전합니다. 경로 표현이 섞이면 런타임에서 `Ignoring restored DB cwd` 경고가 나거나 restored session 복구가 꼬일 수 있습니다.
- migrate로 세션까지 옮긴 뒤 workspace 경로를 수동으로 바꾸거나 runtime root를 이동했다면, 기존 provider `session_id`를 같이 정리해야 합니다. 오래된 `session_id`가 남아 있으면 Discord agent가 죽은 세션을 resume하려고 하다가 응답 없이 hang처럼 보일 수 있습니다.
