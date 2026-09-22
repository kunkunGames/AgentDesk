# kunkunGames → itismyfield 기여 후보

`kunkunGames/AgentDesk`에만 있고 `itismyfield/AgentDesk`로 올릴 가치가 있는 기능을 정리한다. 이 문서는 후보 목록이며, 업스트림 PR을 연 기록이 아니다.

대상:

- 출발: [kunkunGames/AgentDesk](https://github.com/kunkunGames/AgentDesk)
- 도착: [itismyfield/AgentDesk](https://github.com/itismyfield/AgentDesk)

## 스냅샷

| 항목 | 값 |
| --- | --- |
| 기준일 | 2026-09-15 |
| `origin/main` (`kunkunGames`) | `e542c18df` (#2028) |
| `upstream/main` (`itismyfield`) | `fa5f61452` (#5927 cancel-handoff) |
| merge-base | `23eb9bba1` |
| 위상 | origin 73 ahead / 1 behind |

비교 범위는 `23eb9bba1..origin/main`이다. 큰 PR을 열기 전에 `fa5f61452`를 먼저 reverse-sync하고, 각 후보를 최신 `itismyfield/main`에 다시 diff한다.

카카오 연동과 AGY dialect 자체는 양쪽 `main`에 이미 있다. 아래 목록에 넣지 않는다.

## 추천 올리는 순서

1. 충돌이 적은 보안·a11y·성능·경로 helper (#1890, #1889, #1990, #2008, #2019, #2009, #1880, #2028, #1870/#1881/#1882).
2. AGY StreamJson 에러 계약 (#1841). extra-account와 독립.
3. Provider extra-account / auth profile (#1892). 마이그레이션 재번호 필요.
4. Live recovery 기반 (#1893), 이어서 계정 인식 fallback (#1948). auth 다음.

## 제품 기능

포크에만 있는 제품 클러스터는 세 개다. 마이그레이션·API·대시보드가 묶여 있으므로 커밋 단위 cherry-pick보다 기능 단위 포트가 맞다.

### 1. Provider CLI extra-account / auth profile — 1순위 제품

| 항목 | 내용 |
| --- | --- |
| 포크 PR | [#1892](https://github.com/kunkunGames/AgentDesk/pull/1892) `feat(auth): named provider CLI extra accounts and usage overlays` |
| 대체 | 닫힌 #1807 |
| 의존 | 없음. recovery보다 먼저 올린다. |
| 마이그레이션 | `0117_rate_limit_cache_profile_id.sql` — 업스트림 최신이 `0115`이므로 포트 시 `0116`부터 재번호 |

무엇을 올리는가:

- 프로바이더별 named extra account를 누적 관리하고, Primary / 에이전트 / 채널 단위로 선택한다.
- 해석 순서: `채널 > 에이전트 > 프로바이더 Primary > 시스템 기본값`.
- 관리된 credential home만 허용. symlink·임의 경로·알 수 없는 env key·상속 API key는 overlay에서 차단.
- Settings Providers UI, `org.yaml` `auth_profile`, process/tmux resume identity fence.
- `rate_limit_cache` PK를 `(provider, profile_id)`로 분리해 계정별 쿼터 압력을 섞지 않는다.

주요 표면:

- `src/services/provider_auth_profile.rs`
- `src/server/routes/provider_auth_profiles.rs`
- `src/services/dispatch_gate/auth_profiles.rs`
- `src/services/session_backend/auth_profiles.rs`
- `dashboard/src/components/settings/SettingsProvidersPanel.tsx`
- `GET/POST/PUT/DELETE /api/provider-auth-profiles…`
- `PATCH /api/channels/{id}`

포함하지 않는 것:

- AGY/Gemini OAuth 계정 격리는 extra-account overlay로 보장되지 않는다.
- 운영 서버에 계정을 대신 만들어 주지 않는다.

후속 커밋 (`b984c7741` … `b34cc2c1f`)은 #1892 검토 결함 수정이다. 포트할 때 같이 가져간다.

### 2. Same-channel live recovery — 2순위 제품

auth profile이 있어야 계정 인식 fallback이 의미가 있다. 두 PR을 한 기능으로 보지 말고 **#1893 다음 #1948** 순서로 올린다.

#### 2a. Exclusive intake WAL (#1893)

| 항목 | 내용 |
| --- | --- |
| 포크 PR | [#1893](https://github.com/kunkunGames/AgentDesk/pull/1893) `feat(recovery): same-channel live fallback with exclusive intake WAL` |
| 대체 | 닫힌 #1806 |
| 의존 | #1892 |
| 마이그레이션 | `0118_agent_recovery_channel_state.sql`, `0119_agent_recovery_fencing_generation.sql`, `0120_agent_recovery_pending_intents.sql` |

무엇을 올리는가:

- 같은 Discord 채널에서 owner stall 시 fallback 에이전트가 실제 turn을 이어받는다.
- 메모리 선반영이 아니라 unpublished snapshot → PostgreSQL atomic fence/WAL → 성공 커밋만 캐시 공개.
- 채팅·API·루틴 mailbox 예약을 공통 durable admission으로 보호한다.
- `TakeoverPending` / `RestorePending` 기록 후 기존 실행 종료 → 새 실행 시작 → 시작 확인.
- 활성 lease의 provider/workspace를 PG에 보존하므로 org 설정 변경이 실행 중 fallback fence를 풀지 않는다.

#1893 병합본의 자동 복구 진입은 watchdog 경로 기준이다. 429·쿼터 오류의 직접 연결은 #1948이다.

#### 2b. Account-aware fallback + quota takeover (#1948)

| 항목 | 내용 |
| --- | --- |
| 포크 PR | [#1948](https://github.com/kunkunGames/AgentDesk/pull/1948) `feat(recovery): same-provider account fallback and immediate quota-error takeover` |
| 의존 | #1892, #1893 |
| 문서 | [`docs/account-aware-recovery.md`](account-aware-recovery.md) |

무엇을 올리는가:

- 같은 provider에서 인증 프로필이 다른 에이전트로 fallback을 허용한다. 계정 격리를 지원하지 않는 provider는 거절.
- owner/fallback profile ID를 durable recovery context에 고정. 채널 owner 계정이 fallback 계정을 덮어쓰지 않는다.
- HTTP 429, rate/usage limit, exhausted quota, stream idle timeout에서 미완료 요청과 부분 출력을 저장한 뒤 기존 watchdog을 즉시 깨운다.
- generation fencing으로 stale/중복 오류 거절. 복귀 직후 60초 cooldown으로 계정 ping-pong을 막는다.
- 권한·인증·context-length 실패는 기존 오류 처리. recovery는 tool permission을 우회하지 않는다.

포함하지 않는 것:

- 제3의 fallback 체인.
- 복구 중 새 일반 채팅을 fallback 역할로 자동 재배정.
- AGY OAuth 저장소 격리.

주요 표면:

- `src/services/agent_recovery/**`
- `src/services/discord/health/recovery/live_agent_recovery.rs`
- `src/services/discord/turn_bridge/stream_loop/provider_recovery.rs`
- `src/services/claude/{process,tui}_session_launch.rs`
- `src/services/codex/{process,tui}_session_launch.rs`

### 3. AGY StreamJson 빈 SUCCESS / 권한 오류 전파 — 독립 제품 수정

| 항목 | 내용 |
| --- | --- |
| 포크 PR | [#1841](https://github.com/kunkunGames/AgentDesk/pull/1841) `fix(stream-json): AGY headless 권한 오류와 빈 응답 전파` |
| 의존 | extra-account와 무관. auth/recovery보다 먼저 올려도 된다. |
| 문서 | [`docs/stream-json-error-contract.md`](stream-json-error-contract.md) |

AGY dialect(`antigravity` / `agy` / `-ag`)는 업스트림에 이미 있다. 올리는 것은 dialect가 아니라 에러 계약이다.

무엇을 올리는가:

- `result.status=SUCCESS` + 빈 assistant 출력을 성공으로 보지 않는다.
- permission denial은 마지막 실패 step과 bounded stderr prefix를 포함한다. 터미널 이벤트에 session ID가 없어도 유지.
- `Done`은 SUCCESS + non-empty conversation ID + non-blank output + 성공 process exit 이후에 한 번만 낸다.
- 실패한/시그널 종료 프로세스가 부분 텍스트를 성공으로 바꾸지 못한다.
- 권한 우회를 추가하지 않는다. 거절은 운영자가 project permission rule을 고칠 수 있게 보이는 오류로 남긴다.

주요 표면:

- `src/services/stream_json_cli/codec.rs`
- 공유 runner stderr drain (최대 16 KiB, 로그에 stderr body 미포함)
- `cargo test --lib stream_json_cli`, `services::provider::`, `agy_discord_dispatch`

## 이식 가능한 유지보수

제품 기능은 아니지만 업스트림에도 도움이 되는 작은 변경. 충돌이 적으면 제품 PR보다 먼저 올린다. 포인터 전에 최신 `itismyfield/main`과 중복 여부를 확인한다.

| 우선 | 포크 PR | 제목 | 왜 올리는가 |
| --- | --- | --- | --- |
| 보안 | [#1890](https://github.com/kunkunGames/AgentDesk/pull/1890) | xmldom advisory dependency | 보안 권고 대응 |
| a11y | [#1889](https://github.com/kunkunGames/AgentDesk/pull/1889) | EmojiPicker label semantics | 라벨 의미 정리 |
| a11y | [#1990](https://github.com/kunkunGames/AgentDesk/pull/1990) | 유지보수 PR 통합 | AgentForm/OfficeManager a11y, Vitest 4.1.11, scratch-file guard, docs-only 검증, voice `expand_tilde` |
| a11y | [#2008](https://github.com/kunkunGames/AgentDesk/pull/2008) | modal sprite picker `aria-valuemax` | 모달 범위 표시 |
| a11y | [#2019](https://github.com/kunkunGames/AgentDesk/pull/2019) | sprite selector keyboard | 키보드 접근 |
| 성능 | [#2009](https://github.com/kunkunGames/AgentDesk/pull/2009) | dispatch serde allocation | dispatch 생성 할당 감소 |
| 성능 | [#1880](https://github.com/kunkunGames/AgentDesk/pull/1880) | `list_run_history_pg` LATERAL | 히스토리 쿼리 |
| 성능 | [#2028](https://github.com/kunkunGames/AgentDesk/pull/2028) | phase-gate context 지연 | outbox 필요 시에만 조회 |
| 구조 | [#1861](https://github.com/kunkunGames/AgentDesk/pull/1861) | typed card facade | reconciliation raw SQL 제거 |
| 경로 | [#1870](https://github.com/kunkunGames/AgentDesk/pull/1870) [#1881](https://github.com/kunkunGames/AgentDesk/pull/1881) [#1882](https://github.com/kunkunGames/AgentDesk/pull/1882) | `expand_user_path` | tilde 확장 중복 제거 |
| 관측 | [#1876](https://github.com/kunkunGames/AgentDesk/pull/1876) | worker `observability_target` | 워커 수명주기 로그 |
| 라우팅 | [#2012](https://github.com/kunkunGames/AgentDesk/pull/2012) | analytics domain 이동 | 라우트 경계 |
| CI 규칙 | [#2001](https://github.com/kunkunGames/AgentDesk/pull/2001) | docs-only verification | 문서-only PR 검증 가드 |
| Doctor | [#1853](https://github.com/kunkunGames/AgentDesk/pull/1853) [#1887](https://github.com/kunkunGames/AgentDesk/pull/1887) | Doctor 안내 영어화 | 업스트림 기본 언어와 맞춤 |
| 위생 | [#1886](https://github.com/kunkunGames/AgentDesk/pull/1886) | no-change / scratch hygiene | PR 가드 |
| 위생 | [#1997](https://github.com/kunkunGames/AgentDesk/pull/1997) | Doctor SQLite 문구 제거 | 오래된 안내 |
| 로그 | [#2005](https://github.com/kunkunGames/AgentDesk/pull/2005) | Discord duplicate log context | 중복 메시지 로그 |
| 정리 | [#2004](https://github.com/kunkunGames/AgentDesk/pull/2004) | dead `allow` 제거 | `resolve_parent_dispatch_context` |

#1990이 묶은 원본: #1987, #1968, #1980, #1978, #1975, #1976, #1972. 업스트림에 개별 PR로 올리지 말고 #1990 단위로 본다.

선택 후보 — SQL inventory LF 정규화 헬퍼 (#2025의 `_file_bytes_for_fingerprint()`): Windows CRLF 작업 트리와 Linux git blob 해시 불일치를 막는다. **헬퍼만** 검토하고, 포크 baseline SHA rewrite는 가져가지 않는다.

## 올리지 않는 것

이 커밋/변경은 포크 운영 잔여물이거나 반대 방향 싱크이므로 업스트림에 가져가지 않는다.

| 항목 | 이유 |
| --- | --- |
| [#2024](https://github.com/kunkunGames/AgentDesk/pull/2024) merge commit | itismyfield → kunkunGames 흡수. 반대 방향. |
| [#2025](https://github.com/kunkunGames/AgentDesk/pull/2025) baseline rewrite | 포크 마이그레이션 해시 재측정. 헬퍼와 별개. |
| [#2026](https://github.com/kunkunGames/AgentDesk/pull/2026) `stream_loop.rs` 979줄 압축 | 포크가 recovery+업스트림을 한 파일에 합친 뒤 cap을 넘긴 결과. 업스트림 파일 크기가 다르다. cap을 올리지 말고 packing도 복사하지 않는다. |
| `a39eb8c24` giant-file fixture를 `kunkunGames`로 맞춤 | 포크 저장소 이름 pin. |
| `051e89cbd` antigravity channel suffix 테스트 | 포크 테스트 정렬. dialect는 업스트림에 이미 있음. |
| `3889b456d` destructive ratchet baseline | 포크 호출 지점 기준. |
| [#1875](https://github.com/kunkunGames/AgentDesk/pull/1875) giant-file closed-issue ratchet | 포크 이슈 번호. |
| [#2021](https://github.com/kunkunGames/AgentDesk/pull/2021) maintainability audit refresh | 생성물. 업스트림에서 생성기를 다시 돌린다. |
| [#2020](https://github.com/kunkunGames/AgentDesk/pull/2020) [#1982](https://github.com/kunkunGames/AgentDesk/pull/1982) MemoryCustodian stale issue 문서 | 포크 이슈 번호 청소. |
| [#2022](https://github.com/kunkunGames/AgentDesk/pull/2022) api-overhaul audit 문구 | 포크 보고서 시점. |
| CI SHA pin / SQL inventory / lib test inventory 재생성 커밋 | 대상 트리에서 생성기를 다시 실행한다. |
| 카카오 연동 | 업스트림 `main`에 이미 있다. |

## 포트 시 제약

1. **큰 제품 PR 전에 reverse-sync.** origin은 `fa5f61452`보다 1 커밋 뒤다. 충돌을 포크에서 먼저 흡수한다.
2. **마이그레이션 재번호.** 업스트림 최신은 `0115`다. 포크 `0117`–`0120`은 포트 시 `0116`부터 다시 붙인다. 그 사이 업스트림이 `0116+`를 쓰면 그때 번호에 맞춘다. squash로 포크 SHA를 업스트림에 맞추지 않는다.
3. **기능 단위로 포트.** #1892 후속 auth 수정 커밋, #1893 검토 보완, #1948은 각각 원 PR 단위로 재구성한다. merge commit을 cherry-pick하지 않는다.
4. **생성물은 다시 만든다.** `ARCHITECTURE.md`, route/SQL/lib-test inventory, maintainability audit는 업스트림 트리에서 공식 생성기를 돌린다.
5. **AGY extra-account를 약속하지 않는다.** named overlay가 있어도 Gemini/AGY OAuth 격리는 별 문제다.
6. **자격 증명을 커밋하지 않는다.** recovery state/WAL에도 credential을 쓰지 않는 계약을 유지한다.

## 포크에만 추가된 파일 (제품)

`git diff --diff-filter=A 23eb9bba1..origin/main` 기준. 유지보수 PR의 테스트 파일은 생략한다.

```
dashboard/src/components/settings/SettingsProvidersPanel.tsx
dashboard/src/components/settings/SettingsProvidersModel.ts
src/services/provider_auth_profile.rs
src/server/routes/provider_auth_profiles.rs
src/services/agent_recovery/**
src/services/claude/process_session_launch.rs
src/services/claude/tui_session_launch.rs
src/services/codex/process_session_launch.rs
src/services/codex/tui_session_launch.rs
src/services/opencode/server_launch.rs
migrations/postgres/0117_rate_limit_cache_profile_id.sql
migrations/postgres/0118_agent_recovery_channel_state.sql
migrations/postgres/0119_agent_recovery_fencing_generation.sql
migrations/postgres/0120_agent_recovery_pending_intents.sql
docs/account-aware-recovery.md
docs/stream-json-error-contract.md
```

## 비목표

- 이 문서로 업스트림 PR을 자동 생성하지 않는다.
- 포크 `main`을 `itismyfield/main`에 직접 push하지 않는다.
- 카카오·AGY dialect 재포트를 다시 열지 않는다.
