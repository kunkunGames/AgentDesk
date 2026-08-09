---
doc_type: "prd"
schema_version: "2"
status: "draft"
topic_slug: "kakao-friend-message"
topic_folder: "integrations"
linked_spec: "./kakao-friend-message-spec.md"
generated_by: "grok"
updated_by: "codex"
created_at: "2026-08-09"
updated_at: "2026-08-09"
target_repo: "https://github.com/kunkunGames/AgentDesk"
upstream_ref: "https://github.com/itismyfield/AgentDesk"
survey_pass: "2026-08-09-cohesion-roi-safety"
implementation_stage: "implemented-local-review"
implementation_gate: "rollout-blocked-on-live-e2e"
external_evidence_status: "official-contracts-verified-live-account-pending"
---

# PRD: 카카오톡 친구에게 텍스트 수동 공유

> 상태: `draft` — 기본 비활성화 구현과 로컬 검증은 존재하며, **Kakao 콘솔 설정·실계정 E2E·외부 랜딩 페이지 검토 전에는 운영 활성화하지 않는다.**
> 상세 계약: [kakao-friend-message-spec.md](./kakao-friend-message-spec.md)

## 0. 한눈에 보는 결정

### 0.1 제품 약속

v1이 제공하는 기능은 다음 한 문장으로 고정한다.

> **연결된 AgentDesk 운영자가 Settings에서 카카오톡 친구 1~5명을 선택해, 200자 이하의 텍스트와 고정된 AgentDesk 랜딩 링크를 확인 후 한 번 전송한다.**

v1은 AgentDesk 결과나 카드를 외부에 공개하지 않는다. 고정 랜딩 링크는 특정 결과를 가리키지 않으며, 수신자에게 사설 AgentDesk 데이터 접근 권한을 부여하지 않는다.

### 0.2 이번 개정의 핵심 결정

| 영역 | 결정 | 이유 |
|---|---|---|
| 첫 사용자 표면 | Settings의 connector 인접 `테스트로 공유`만 | 실제 가치 검증 전에 전역 버튼과 결과 화면을 오염시키지 않음 |
| 콘텐츠 | 텍스트 + 서버 고정 랜딩 링크 | 공개 artifact·권한·만료 링크 범위를 분리 |
| 전송 보장 | 영속 `external_share_operations` fence 기반 at-most-once | Kakao POST가 비멱등이어도 crash 후 자동 재전송하지 않음 |
| 기존 idempotency | fingerprint helper만 재사용 가능; claim/TTL 재claim은 사용 금지 | 현재 계약은 replay-safe mutation 전용 |
| 기존 delivery journal | 개념만 참고; 테이블·writer 재사용 금지 | `0105_delivery_journal`은 Discord 전용 hot surface와 제약을 가짐 |
| connector | 기존 Settings connector 표면을 DB-aware로 확장 | 새 Integrations 탭 없이 상태 원천을 하나로 유지 |
| 채널 추상화 | v1에는 범용 `ExternalShareChannel` trait 없음 | 구현체 하나로 잘못된 공통 계약을 선고정하지 않음 |
| 다중 노드 | DB unique fence와 refresh lease로 안전하게 처리 | process-local guard를 운영 보장으로 오인하지 않음 |
| 외부 사실 | 공식 문서로 wire 계약을 고정하고 실계정 E2E를 rollout gate로 분리 | 자격증명 없이 검증 가능한 코드 계약과 실제 운영 승인을 혼동하지 않음 |

### 0.3 구현 상태

| 구분 | 상태 |
|---|---|
| AgentDesk HEAD 코드·DB·UI 계약 | 구현 및 로컬 검증 완료 |
| Kakao 최신 정책·권한·응답 계약 | 공식 문서 확인 완료; 콘솔·실계정 확인 대기 |
| 제품 수요와 반복 사용성 | 검증 대기 |
| 아키텍처 안전성 계약 | 본 문서와 Spec에 반영 |
| 코드 구현 | 기본 비활성화 수직 슬라이스 완료; PR 검토 중 |

---

## 1. 문제와 제품 가설

### 1.1 문제

AgentDesk 운영자가 짧은 작업 요약을 카카오톡 친구에게 전달하려면 현재는 텍스트를 복사하고 카카오톡에서 수신자를 다시 찾아 붙여 넣어야 한다. 반복적으로 이 흐름을 사용하는 운영자에게는 컨텍스트 전환과 오발송 확인 비용이 생긴다.

아직 확인되지 않은 것은 다음과 같다.

- 실제 대상 운영자가 이 흐름을 얼마나 자주 수행하는가
- Kakao 친구 API에 의도한 수신자가 실제로 노출되는가
- 앱 권한·동의·운영 심사 조건을 현재 배포 형태가 충족하는가
- 고정 랜딩 링크가 수신자에게 유용한가, 텍스트만으로 충분한가

따라서 본 기능은 완성형 결과 공유가 아니라 **낮은 비용으로 반복 수요를 확인하는 수동 전송 실험**으로 시작한다.

### 1.2 가설

> 운영자가 주기적으로 AgentDesk 요약을 동일한 카카오톡 친구 집합에 전달한다면, Settings의 확인형 수동 전송은 복사·전환 비용과 수신자 선택 오류를 줄인다.

### 1.3 반증 가능성

다음 중 하나면 가설은 약하거나 기각된 것으로 본다.

- 의도한 친구가 Kakao API의 노출 조건을 충족하지 못한다.
- 앱 권한이나 운영 정책 때문에 실제 배포에서 사용할 수 없다.
- 운영자가 14일 동안 반복적으로 사용하지 않는다.
- `unknown` 결과와 수동 확인 부담이 복사·붙여넣기보다 크다.
- 실제 가치는 결과별 공개 링크인데, 고정 랜딩 링크 실험으로는 검증되지 않는다.

---

## 2. ROI Gate

### 2.1 Gate 0 — 운영 활성화 전 실현 가능성

다음 항목이 모두 통과되어야 `integrations.kakao_friend_share.enabled`를 운영 환경에서 `true`로 바꿀 수 있다. 자격증명 없는 로컬 구현·fixture 검증은 기본 비활성화 상태에서 먼저 수행할 수 있지만, 미통과 항목을 운영 준비 완료로 표현해서는 안 된다.

| ID | 통과 조건 | 증거 |
|---|---|---|
| G0-01 | Kakao Login, 친구 목록, 친구 메시지, 템플릿 공식 문서의 직접 URL·확인 날짜·핵심 제약을 Spec Evidence Ledger에 기록 | 공식 문서 |
| G0-02 | 필요한 제품·동의 항목·앱 멤버/운영 권한을 Kakao 콘솔에서 활성화할 수 있음 | 콘솔 체크 결과 |
| G0-03 | 개발 계정 3~4명으로 로그인 → 친구 조회 → 텍스트 메시지 전송 E2E 성공 | 수동 E2E 체크리스트; 토큰/UUID 원문 저장 금지 |
| G0-04 | `state`, 쉼표 scope 직렬화, client secret 전송, refresh token 생략 시 기존 값 유지 규칙을 공식 문서·oauth2-rs 소스·fixture로 확정 | Evidence Ledger + fixture |
| G0-05 | 최소 한 운영자가 “주 1회 이상 반복 사용할 상황”을 구체적인 예로 확인 | 제품 결정 기록 |
| G0-06 | `landing_url`이 외부 수신자에게 열어도 안전한 고정 페이지이며 AgentDesk 사설 데이터가 노출되지 않음 | 보안 검토 |
| G0-07 | 제공자 요청 결과를 definitive failure와 ambiguous outcome으로 구분할 근거가 있음 | 공식 오류 계약 또는 fixture |

현재 G0-01·G0-04·G0-07의 코드 계약은 확인됐고, G0-02·G0-03·G0-05·G0-06은 실제 Kakao 앱·운영자·외부 랜딩 페이지가 필요해 대기 중이다. Kakao REST 공식 문서에서 PKCE 지원을 확인하지 못했으므로 v1은 PKCE를 억지로 추가하지 않고, 고엔트로피 단회 `state`와 confidential client secret을 사용한다. HTTP 200의 성공/실패 UUID 집합이 요청 전체를 완전히 설명할 때만 확정 결과로 분류하며, 그 외 provider/transport 결과는 `unknown`이다.

### 2.2 MVP Pilot — 반복 가치 검증

MVP가 배포되면 `external_share_operations`의 비식별 aggregate만 사용해 다음을 관찰한다. 별도 analytics/audit 테이블은 만들지 않는다.

| 지표 | 정의 |
|---|---|
| 연결 성공 | OAuth 시작 대비 `connected` 전환 |
| 발송 시도 | 새 operation fence 생성 수 |
| 알려진 결과율 | `(success + partial_success + failed) / 전체 terminal` |
| 모호한 결과율 | `unknown / 전체 terminal` |
| 반복 사용 | 서로 다른 날짜에 수행한 의도적 발송 수 |
| 중복 사고 | 동일 사용자 의도의 중복 메시지 보고 수 |

기본 승격 기준은 다음과 같다. 제품 책임자는 Gate 0 완료 전에 다른 수치를 명시적으로 기록할 수 있다.

- 14일 안에 서로 다른 3일 이상에서 총 5회 이상 의도적 발송
- 중복 사고 0건
- `unknown`이 발생했을 때 자동 재전송 없이 운영자가 의미를 이해하고 처리 가능
- 운영자가 결과 화면의 직접 진입점이 실제로 시간을 더 절약한다고 확인

승격 기준을 충족해야 결과 화면이나 전역 toolbar에 공유 진입점을 추가한다. 사용이 반복되지 않으면 Settings 시험 기능 이상으로 확장하지 않는다.

### 2.3 비용 상한

v1에서 다음 비용을 의도적으로 만들지 않는다.

- 결과별 공개 링크 저장소
- 범용 채널 registry와 동적 plugin 계약
- 메시지 템플릿 DSL
- 다중 Kakao account UI
- 장기 감사/분석 이벤트 파이프라인
- Discord outbox 또는 delivery journal 변경
- 친구 thumbnail proxy/cache

---

## 3. 범위

### 3.1 v1 포함

1. Settings connector row에서 Kakao OAuth 연결·재연결·로컬 연결 해제
2. 연결된 계정의 메시지 가능 친구 목록 조회
3. 친구 1~5명 선택
4. 1~200자 텍스트 편집
5. 고정 랜딩 링크를 포함한 미리보기
6. 명시적 확인 후 단 한 번의 provider POST
7. `success`, `partial_success`, `failed`, `unknown` 결과
8. 동일 `Idempotency-Key` replay와 payload mismatch 방지
9. 다중 노드에서 동일 operation 중복 POST 방지

### 3.2 v1 제외

- AgentDesk 결과/카드의 공개 공유
- 자동·예약·이벤트 기반 메시지
- 알림톡, 전화번호 발송, 전체 친구 발송
- feed/list/commerce 등 추가 Kakao 템플릿
- 친구 검색 인덱스, 즐겨찾기 동기화, thumbnail 저장
- Kakao 계정 여러 개 연결
- 원격 Kakao unlink; v1 disconnect는 AgentDesk 로컬 자격증명 삭제
- 두 번째 외부 채널
- `ExternalShareChannel` trait/registry
- Discord `message_outbox`, `outbound/*`, `delivery_journal_events`

### 3.3 향후 결과 공유가 필요할 때

결과별 링크가 제품 핵심으로 확인되면 별도 PRD로 다음을 설계한다.

- 추측 불가능한 토큰
- 만료·취소·삭제 전파
- 공개 필드 allowlist와 redaction
- 조직/사용자 경계
- 외부 접근 가능한 base URL
- 접근 로그의 개인정보·보존 정책

현재 `landing_url`을 결과 공유 링크처럼 표현해서는 안 된다.

---

## 4. 사용자 흐름

### 4.1 연결

```text
Settings → Operator connectors → Kakao Friend Share
  → Connect
  → Kakao 동의 화면
  → 고정 callback
  → /settings?connector=kakao_friend_share&oauth=ok
  → 동일 connector 응답을 다시 조회
```

### 4.2 시험 발송

```text
Ready connector → 테스트로 공유
  → 친구 목록 조회
  → 1~5명 선택
  → 텍스트 작성
  → 고정 landing URL 미리보기
  → "선택한 친구에게 지금 전송" 확인
  → 한 번 전송
  → success | partial_success | failed | unknown
```

### 4.3 `unknown` UX

`unknown`은 “실패”가 아니라 “전달 여부를 확인할 수 없음”이다.

- 같은 payload의 명시적 결과 재확인은 component memory에 남은 같은 `Idempotency-Key`를 사용한다. 서버는 저장된 결과만 replay하며 provider POST를 다시 하지 않는다.
- UI와 HTTP client는 자동 retry를 수행하지 않는다.
- payload를 바꾸거나 새 key로 다시 보내려면 “이미 전달되었을 수 있어 중복될 수 있음”을 별도로 확인한다.
- 운영자가 카카오톡에서 실제 전달 여부를 확인할 수 없다면 재전송하지 않는 것이 기본이다.

---

## 5. 응집된 아키텍처

```text
Settings connector UI
  ├─ connection actions ───────────────┐
  └─ inline test-share composer        │
                                       ▼
public callback ── routes/kakao ── KakaoFriendShareService
protected routes ────────────────┬─ services/kakao (oauth, friends, messages)
operator connectors ─────────────┤
                                 ├─ oauth_connection (repository, vault, refresh lease)
                                 └─ external_share (operation store)

FORBIDDEN: kakao/external_share → Discord outbox/outbound/delivery journal
```

### 5.1 모듈 책임

| 모듈 | 단일 책임 | 포함하지 않음 |
|---|---|---|
| `oauth_connection` | provider-keyed session/account 저장, AEAD vault, refresh lease | Kakao 친구·메시지 JSON |
| `kakao` | OAuth endpoint/scope, 친구·메시지 DTO와 오류 분류 | UI, Discord, operation retention |
| `external_share` | confirmed command, at-most-once operation, 결과 집계 | authorize URL, provider raw DTO |
| `operator_connectors` | config + DB connection 상태의 canonical projection | token 복호화 결과 원문 |
| routes | 인증 경계, 입력/출력, AppError 변환 | retry 정책과 SQL 상태 전이 |

### 5.2 재활용 판단

| 기존 자산 | 결정 | 근거 |
|---|---|---|
| 기존 idempotency fingerprint 원칙 | 의미만 재사용 | 수신자 순서 비의존·길이 구분 hash가 필요해 Kakao command 전용 fingerprint를 사용 |
| `idempotency_keys` claim/TTL | **재사용 금지** | 만료 후 재실행은 replay-safe mutation 전용 |
| `delivery_journal_events` | **재사용 금지** | Discord obligation/attempt/message receipt 전용 제약과 writer guard |
| `operator_connectors`와 Settings panel | 확장 | 기존 운영자 connector 표면을 보존 |
| `/api/settings/operator-connectors` | DB-aware async projection으로 확장 | 별도 status 체계 제거 |
| `public_api_domain` / `protected_api_domain` | 재사용 | callback과 운영 API의 인증 경계가 일치 |
| `AppError`와 기존 ErrorCode | 재사용 | envelope와 공통 오류 어휘 유지 |
| `utils::redact`, config secret 비직렬화 | 재사용 | secret 노출 방지 |
| reqwest rustls stack | 재사용 | 별도 HTTP/TLS stack 불필요 |
| Discord outbox/rate/delivery | 사용 금지 | 자동 전달과 재시도 의미가 다름 |

### 5.3 최소 신규 자산

| 신규 자산 | 신규가 필요한 이유 |
|---|---|
| OAuth session/account 테이블 | 현재 provider OAuth 자격증명 저장소 없음 |
| `external_share_operations` | 비멱등 provider POST 앞의 영속 at-most-once fence가 없음 |
| token vault | 평문 token 저장 금지; 기존 AEAD 모듈 없음 |
| connector runtime projection | 현재 connector가 env/filesystem 동기 상태만 표현 |

범용 채널 trait, 새 Settings 탭, 별도 status API, 장기 audit 테이블은 만들지 않는다.

---

## 6. 안전성과 운영 계약

### 6.1 전송 보장

- 순수 validation, 연결 확인, token refresh, rate check를 먼저 끝낸다.
- provider POST 직전에 DB operation을 `dispatching`으로 영속화한다.
- `dispatching`이 영속화된 뒤에는 어떤 crash/timeout에서도 같은 operation을 다시 POST하지 않는다.
- crash가 INSERT 직후 POST 전에 발생하면 실제 미발송이 `unknown`으로 남을 수 있다. 중복 방지를 위한 의도된 at-most-once trade-off다.
- terminal 결과 저장이 실패해도 operation은 나중에 `unknown`으로 고정하며 재claim하지 않는다.
- operation row는 v1에서 자동 삭제하지 않는다. 향후 response를 압축하더라도 request tombstone은 유지한다.

### 6.2 연결과 token

- OAuth state는 128-bit 이상 CSPRNG 원문을 브라우저에 전달하고 DB에는 SHA-256만 저장한다.
- session TTL은 10분이며 한 번만 소비한다.
- callback의 return 위치는 v1에서 `/settings`로 고정한다. 사용자 입력 return URL은 받지 않는다.
- access/refresh token JSON은 XChaCha20-Poly1305로 암호화한다. v1은 확인되지 않은 PKCE verifier를 생성·저장하지 않는다.
- disconnect는 AgentDesk account row를 삭제한다. Kakao 원격 unlink로 표현하지 않는다.
- refresh는 DB lease로 직렬화한다. 모호한 refresh 결과나 만료된 lease는 자동 반복하지 않고 `reauth_required`로 전환한다.

### 6.3 개인정보

- 친구 UUID는 friends/send request·response에서만 허용한다.
- UUID, nickname, token, authorization code, state를 로그·metric label·AppError context에 넣지 않는다.
- operation table에는 raw UUID와 message text를 저장하지 않는다. request fingerprint, recipient count, 안전한 결과만 저장한다.
- thumbnail은 v1 응답에 포함하지 않는다.
- provider 오류 message는 allowlist된 내부 code로 변환하고 원문을 클라이언트에 그대로 전달하지 않는다.

### 6.4 다중 노드

- operation PK와 DB transaction이 중복 POST를 막는 권위다.
- refresh lease가 동시 token refresh를 막는 권위다.
- process-local mutex나 rate limiter는 정확성 근거로 사용하지 않는다.
- send safety limit는 operation table의 시간 구간 count로 계산해 모든 노드에 동일하게 적용한다.

---

## 7. 설정과 connector

### 7.1 설정 소유권

```yaml
integrations:
  kakao_friend_share:
    enabled: false
    redirect_uri: "http://127.0.0.1:8791/api/kakao/oauth/callback"
    landing_url: "https://example.com/agentdesk"
    send_limit_per_hour: 30
```

- 이 블록은 restart-required integration config다.
- company settings와 `kv_meta['runtime-config']`에 복제하지 않는다.
- 구현 PR은 `docs/config-domains.md`와 config hot-reload 설명을 같은 PR에서 갱신한다.
- recipient/text provider 한계와 endpoint는 사용자 knob로 만들지 않는다.

고정 secret 환경변수:

```text
KAKAO_REST_API_KEY
KAKAO_CLIENT_SECRET
AGENTDESK_OAUTH_TOKEN_KEY_V1
```

### 7.2 connector 상태

`KakaoConnectionStatus`가 유일한 application 상태이며 다음 두 소비자에게 projection된다.

- `/api/settings/operator-connectors`
- `KakaoFriendShareService`의 실행 gate

별도 `/api/kakao/status` endpoint는 만들지 않는다.

기존 connector 응답은 다음 방향으로 **추가형 계약 변경**을 한다. 기존 filesystem 소비자의 `env_var` 계약은 유지하고, 여러 환경변수와 OAuth 상태를 표현하는 필드를 같은 PR에서 원자적으로 추가한다.

- `kind`: `filesystem | oauth`
- `env_var`: 기존 단일 대표값 유지
- `env_vars`: connector가 요구하는 환경변수 목록
- `connection`: OAuth connector에만 존재
- `actions`: `connect | reconnect | disconnect | test_send`
- 기존 `id`, `name`, `state`, `reason`, `detail`, `capabilities`, `summary` 유지

공통 행 렌더링은 `kind`와 `actions`를 사용하고, Kakao 전용 친구 선택 composer만 concrete connector ID에 결합한다. doctor는 기존 정적 connector 진단을 유지하며 DB 연결 여부를 Ready라고 주장하지 않는다.

---

## 8. 구현 순서

### Provider contract — 완료

- 공식 근거 ledger와 oauth2-rs 5.0.0 소스 검증
- 쉼표 scope·refresh 유지·partial/ambiguous 분류 테스트
- 확인되지 않은 PKCE 제거

### 현재 PR — 기본 비활성화 end-to-end vertical slice

- vault와 OAuth session/account migration
- Kakao OAuth adapter와 refresh lease
- public callback, protected start/disconnect
- DB-aware connector projection
- Settings Connect/Reconnect/Disconnect
- friends adapter
- text template/message adapter
- `external_share_operations` fence
- protected friends/send routes
- Settings inline test-share composer
- 단위·빌드·정적 gate 검증
- 같은 PR에서 route inventory, config taxonomy, migration checksum 갱신

### Rollout Gate — 아직 대기

- Kakao 콘솔 권한과 동의 항목 확인
- 개발 계정 3~4명 실계정 E2E
- 외부 `landing_url` 보안 확인
- 반복 수요 확인

### Pilot

- 14일 aggregate 관찰
- 중복·unknown 운영성 검토
- 반복 사용 여부 결정

### PR-C — 승격 조건부 제품 진입점

- 반복 가치가 확인된 경우에만 기존 결과 action surface 하나에 진입점 추가
- 공개 결과 링크가 필요하면 별도 PRD 선행
- 두 번째 채널이 실제로 등장할 때만 공통 interface 추출

---

## 9. 비기능 요구

| 영역 | 요구 |
|---|---|
| 정확성 | 같은 operation은 provider POST 최대 1회 |
| 보안 | token AEAD, state hash, secret 비직렬화, 고정 callback, redirect 비활성화 |
| 개인정보 | UUID·nickname·text 영속/로그 최소화 |
| 가용성 | provider 장애를 Discord/AgentDesk core runtime 장애로 승격하지 않음 |
| 관측성 | PII 없는 상태·latency·outcome aggregate만 |
| 확장성 | provider DTO와 application command 분리; 두 번째 구현 전 trait 금지 |
| 유지보수 | route/migration/inventory/config 문서를 변경 PR과 원자적으로 갱신 |

---

## 10. 리스크와 완화

| 리스크 | 완화 |
|---|---|
| Kakao 권한·친구 노출 조건이 실제 사용자와 맞지 않음 | Gate 0 실계정 E2E 실패 시 기능을 비활성 상태로 유지 |
| provider POST 후 결과를 잃음 | `unknown` terminal + 자동 재전송 금지 |
| fence 후 POST 전에 crash하여 실제 미발송 | at-most-once trade-off를 UX와 운영 문서에 명시 |
| refresh token 회전 경쟁 | DB lease; ambiguous refresh는 reauth |
| connector API가 filesystem 가정에 묶임 | optional field + typed kind/action으로 한 번에 일반화 |
| 결과 공유로 범위가 팽창 | v1 약속을 text + fixed landing으로 고정 |
| 추상화가 실제 채널 차이를 가림 | Kakao concrete service로 시작, 두 번째 구현 때 추출 |
| operation table 증가 | v1은 저빈도·비식별 tombstone 유지; pilot 후 별도 retention 결정 |
| 외부 공식 사실의 변경 | 확인 날짜가 있는 Evidence Ledger를 구현 gate로 사용 |

---

## 11. 문서 완료 기준

다음을 모두 만족해야 `implementation_gate`를 `rollout-ready`로 바꿀 수 있다.

- Gate 0 전 항목 통과
- Spec의 모든 provider fact에 직접 공식 URL·확인 날짜·근거가 있음
- `unknown`을 포함한 crash matrix가 테스트와 1:1 연결됨
- connector canonical status와 API 호환 확장이 고정됨
- 암호화 algorithm/nonce/AAD/key-version 계약이 고정됨
- OAuth disconnect/refresh 불변식이 SQL과 일치함
- 모든 HTTP status와 ErrorCode가 단일 값으로 고정됨
- 모든 REQ가 task와 test 또는 명시적 수동 evidence에 연결됨
- route·migration·config taxonomy 갱신이 각 PR의 Done Criteria에 포함됨
- pilot 승격/중단 기준이 제품 책임자에게 승인됨

---

## 12. 근거 문서

- 상세 Spec: [kakao-friend-message-spec.md](./kakao-friend-message-spec.md)
- 설정 도메인: [config-domains.md](../config-domains.md)
- 변경 표면: [change-surfaces.md](../agent-maintenance/change-surfaces.md)
- 현재 idempotency 계약: [`src/db/idempotency.rs`](../../src/db/idempotency.rs)
- 현재 connector 계약: [`src/services/operator_connectors.rs`](../../src/services/operator_connectors.rs)
- 현재 Discord delivery journal schema: [`0105_delivery_journal.sql`](../../migrations/postgres/0105_delivery_journal.sql)

외부 Kakao·oauth2-rs 근거와 확인 상태는 Spec의 **Provider Evidence Gate**를 정본으로 사용한다.

---

## 변경 이력

| 날짜 | 내용 |
|---|---|
| 2026-08-09 | 초기 초안과 AgentDesk 재사용 조사 |
| 2026-08-09 | wire/idempotency/connectors 상세화 |
| 2026-08-09 | **응집도·재활용·확장성·ROI 안전성 개정**: text-only Settings MVP, Gate 0, durable at-most-once operation fence, DB-aware connector projection, concrete Kakao service, pilot 승격 기준으로 재설계. 기존 idempotency TTL 재claim과 Discord delivery journal 재사용을 금지하고, 외부 미검증 사실을 blocking evidence로 격하. |
| 2026-08-09 | **구현 동기화**: oauth2-rs 5.0.0과 Kakao 공식 REST 계약을 확인하고, 미확인 PKCE를 제거했다. 기본 비활성화 OAuth/vault/friends/send 수직 슬라이스와 inline Settings composer를 반영하고, 콘솔·실계정·landing URL 검증을 rollout gate로 분리했다. |
