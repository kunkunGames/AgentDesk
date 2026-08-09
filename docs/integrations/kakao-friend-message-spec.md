---
doc_type: "spec_source"
schema_version: "3"
status: "draft"
topic_slug: "kakao-friend-message"
topic_folder: "integrations"
linked_prd: "./kakao-friend-message-prd.md"
traceability_mode: "req-task-test-evidence"
generated_by: "grok"
updated_by: "codex"
created_at: "2026-08-09"
updated_at: "2026-08-09"
target_repo: "https://github.com/kunkunGames/AgentDesk"
upstream_ref: "https://github.com/itismyfield/AgentDesk"
code_survey_date: "2026-08-09"
survey_pass: "2026-08-09-cohesion-roi-safety"
implementation_stage: "implemented-local-review"
implementation_readiness: "rollout-blocked-on-live-e2e"
external_evidence_status: "official-contracts-verified-live-account-pending"
---

# SPEC SOURCE: 카카오톡 친구 텍스트 수동 공유

## Linked Document

- PRD: [kakao-friend-message-prd.md](./kakao-friend-message-prd.md)

## State and Normative Language

- status: `draft`
- implementation stage: **default-disabled vertical slice implemented**
- rollout gate: **Kakao 콘솔·실계정 E2E·landing page 승인 미완료 — 운영 활성화 금지**
- source-of-truth scope: 제품 범위, 모듈 책임, DB 상태 전이, HTTP wire, 보안, 요구사항·작업·테스트·외부 근거
- sync policy: PRD의 범위 또는 gate가 바뀌면 REQ/TSK/TEST/EVIDENCE/Traceability를 같은 변경에서 갱신한다.

이 문서에서 `MUST`, `MUST NOT`, `SHOULD`는 구현 계약이다. 공식 문서·oauth2-rs 5.0.0 소스·로컬 fixture로 확인한 wire 계약과, Kakao 콘솔·실계정이 필요한 rollout evidence를 명시적으로 구분한다.

---

## 1. Critical Decisions

1. **v1 제품은 결과/카드 공유가 아니라 Settings의 텍스트 시험 발송이다.**
2. **Kakao POST는 비멱등 외부 side effect로 취급한다.** 기존 `idempotency_keys`의 TTL 재claim을 사용하지 않는다.
3. **중복 방지 권위는 `external_share_operations`의 영속 `dispatching` fence다.** fence 이후에는 같은 operation을 자동 재전송하지 않는다.
4. 기존 idempotency의 fingerprint 원칙만 따른다. 수신자 순서 비의존·길이 구분 Kakao command fingerprint를 별도로 사용하고 `claim`, `release_unclaimed`, `record_response`, 24시간 TTL은 사용하지 않는다.
5. `delivery_journal_events`는 Discord obligation/transport receipt 전용이며 raw-writer guard가 있으므로 확장하거나 import하지 않는다.
6. connector 표면은 재사용하되 `/api/settings/operator-connectors`를 DB-aware async projection으로 확장한다. Kakao 상태의 별도 `/api/kakao/status`는 만들지 않는다.
7. v1은 concrete `KakaoFriendShareService`만 둔다. `ExternalShareChannel` trait/registry는 두 번째 실제 채널이 생길 때 추출한다.
8. token은 XChaCha20-Poly1305로 암호화하고, refresh는 DB lease로 직렬화한다.
9. disconnect는 **AgentDesk 로컬 account row 삭제**다. Kakao 원격 unlink라고 표현하지 않는다.
10. route status, ErrorCode, callback redirect, `unknown` 의미를 선택지 없이 고정한다.
11. route/migration/config 변경은 inventory, immutable checksum, taxonomy 문서를 같은 PR에서 갱신한다.
12. 후속 예약 기능은 `deliveryKind='push'`와 명시적 `providerTargets.kakaoFriendShare.confirmed=true`에서만 Discord + Kakao fan-out을 허용한다. Discord와 Kakao obligation은 같은 transaction으로 만들되 transport 상태와 retry는 공유하지 않는다.
13. 예약 recipient target과 건별 Kakao payload는 vault로 암호화하고 각각 plan/outbox UUID를 AAD에 결합한다. terminal row에는 count summary만 남긴다.
14. Kakao REST 공식 wire 계약과 oauth2-rs 5.0.0 소스는 확인됐다. 공식 문서에서 확인되지 않은 PKCE는 v1에 넣지 않으며, 콘솔·실계정·제품 수요 evidence가 완료되기 전에는 기능을 활성화하지 않는다.

---

## 2. Evidence Model

### 2.1 Evidence labels

| Label | 의미 |
|---|---|
| `LOCAL-VERIFIED` | 2026-08-09 현재 AgentDesk HEAD에서 파일·실행 계약을 직접 확인 |
| `EXTERNAL-VERIFIED` | 직접 공식 URL, 확인 날짜, 해당 절, 필요 시 재현 fixture까지 있음 |
| `PROVISIONAL` | 이전 조사 또는 후보 설계이며 최신 공식 근거 미확정 |
| `DESIGN` | AgentDesk가 선택한 내부 계약 |
| `BLOCKING` | 미확정 상태로는 구현할 수 없음 |

### 2.2 AgentDesk HEAD evidence

| ID | 확인 사실 | Anchor | 상태 | 설계 영향 |
|---|---|---|---|---|
| L-001 | 현재 `idempotency_keys` consumer는 replay-safe phase-gate repair이며 만료 후 재실행은 replay-safe 경로만 허용 | [`idempotency.rs`](../../src/db/idempotency.rs), [`control_routes.rs`](../../src/services/auto_queue/control_routes.rs) | LOCAL-VERIFIED | Kakao send에 claim/TTL 재사용 금지 |
| L-002 | `record_response`는 별도 외부 side-effect 원자성을 제공하지 않는 UPDATE | [`idempotency.rs`](../../src/db/idempotency.rs) | LOCAL-VERIFIED | POST 후 DB 실패 창을 별도 fence로 다룸 |
| L-003 | `0105_delivery_journal`은 obligation/attempt/channel/message receipt 이벤트 schema | [`0105_delivery_journal.sql`](../../migrations/postgres/0105_delivery_journal.sql) | LOCAL-VERIFIED | Kakao용 범용 테이블로 재사용 금지 |
| L-004 | delivery journal raw writer는 Discord journal 내부 한 곳으로 제한됨 | [`journal/pg_store.rs`](../../src/services/discord/session_relay_sink/journal/pg_store.rs), [`test_delivery_journal_raw_writer.py`](../../tests/test_delivery_journal_raw_writer.py) | LOCAL-VERIFIED | Discord hot surface 절연 |
| L-005 | connector backend는 기존 filesystem 필드를 보존하면서 `kind`, `env_vars`, OAuth `connection`, `actions`를 투영함 | [`operator_connectors.rs`](../../src/services/operator_connectors.rs) | LOCAL-VERIFIED | 기존 소비자 호환 추가형 확장 |
| L-006 | connector endpoint가 `AppState`를 받아 DB-aware async projection을 반환함 | [`settings.rs`](../../src/server/routes/settings.rs) | LOCAL-VERIFIED | 별도 Kakao status endpoint 불필요 |
| L-007 | connector UI는 공통 kind/actions를 렌더링하고 Kakao composer를 인접 배치함 | [`SettingsOperatorConnectorsPanel.tsx`](../../dashboard/src/components/settings/SettingsOperatorConnectorsPanel.tsx), [`KakaoFriendShareControls.tsx`](../../dashboard/src/components/settings/KakaoFriendShareControls.tsx) | LOCAL-VERIFIED | Settings-only entrypoint |
| L-008 | connector TS 타입과 Kakao wire DTO가 backend shape와 함께 갱신됨 | [`settingsRuntime.ts`](../../dashboard/src/api/settingsRuntime.ts) | LOCAL-VERIFIED | 단일 PR 원자성 |
| L-009 | public access domain에는 health/auth/session과 Kakao callback만 있음 | [`access.rs`](../../src/server/routes/domains/access.rs) | LOCAL-VERIFIED | callback만 public |
| L-010 | Kakao start/disconnect/friends/send는 protected integration domain에 있음 | [`integrations.rs`](../../src/server/routes/domains/integrations.rs) | LOCAL-VERIFIED | 운영 API 인증 경계 유지 |
| L-011 | AppError body는 `{error, code, context}`이고 `Config`, `Conflict`, `Database`, `Dispatch`, `Internal`, `Policy`, `Validation`이 존재 | [`error.rs`](../../src/error.rs) | LOCAL-VERIFIED | 새 ErrorCode 없이 정확한 mapping 가능 |
| L-012 | reqwest 0.12 + rustls를 재사용하고 oauth2 5.0.0, chacha20poly1305, zeroize만 추가함 | [`Cargo.toml`](../../Cargo.toml) | LOCAL-VERIFIED | 중복 HTTP/TLS stack 없음 |
| L-013 | Config는 cluster/multi-node를 지원함 | [`config.rs`](../../src/config.rs), [`node_registry.rs`](../../src/services/cluster/node_registry.rs) | LOCAL-VERIFIED | process-local correctness guard 금지 |
| L-014 | `0107_kakao_friend_share.sql`과 immutable checksum manifest가 추가됨 | [`0107_kakao_friend_share.sql`](../../migrations/postgres/0107_kakao_friend_share.sql), [`immutable-checksums.json`](../../migrations/postgres/immutable-checksums.json) | LOCAL-VERIFIED | OAuth·operation schema를 한 수직 migration으로 관리 |
| L-015 | CI가 inventory를 생성하고 tracked route/worker docs의 drift를 거부 | [`ci-script-checks.sh`](../../scripts/ci-script-checks.sh) | LOCAL-VERIFIED | route 변경 PR마다 inventory 동시 갱신 |
| L-016 | 결과별 public artifact/deep link 계약이 현재 없음 | [`access.rs`](../../src/server/routes/domains/access.rs) | LOCAL-VERIFIED | v1 landing link는 결과 공유가 아님 |

### 2.3 Provider Evidence Gate

공식 문서와 oauth2-rs 5.0.0 배포 소스는 2026-08-08 UTC에 확인했다. 비밀값과 실제 사람 관계가 필요한 콘솔·실계정 검증은 별도 rollout evidence로 남아 있다.

| Evidence ID | 확인할 주장 | 1차 검증 대상 | 현재 상태 |
|---|---|---|---|
| EVIDENCE-G0-001 | authorize/token endpoint, REST API key client ID, client secret request body, token/refresh fields | <https://developers.kakao.com/docs/latest/ko/kakaologin/rest-api#request-code>, <https://developers.kakao.com/docs/latest/ko/kakaologin/rest-api#request-token>, <https://developers.kakao.com/docs/latest/ko/kakaologin/rest-api#refresh-token> | EXTERNAL-VERIFIED; 2026-08-08 UTC |
| EVIDENCE-G0-002 | 앱 설정, 동의 항목, 운영 전제 | <https://developers.kakao.com/docs/latest/ko/kakaologin/prerequisite>, <https://developers.kakao.com/docs/latest/ko/kakaologin/prerequisite#consent-item>, <https://developers.kakao.com/docs/latest/ko/message/prerequisite> | EXTERNAL-VERIFIED / CONSOLE-PENDING; 2026-08-08 UTC |
| EVIDENCE-G0-003 | 친구 목록 endpoint, 노출 자격, UUID, pagination, limit 100 | <https://developers.kakao.com/docs/latest/en/kakaotalk-social/rest-api> | EXTERNAL-VERIFIED / LIVE-FRIEND-PENDING; 2026-08-08 UTC |
| EVIDENCE-G0-004 | 친구 메시지 endpoint, 수신자 최대 5, HTTP 200 partial schema, quota code | <https://developers.kakao.com/docs/latest/en/message/rest-api> | EXTERNAL-VERIFIED / LIVE-SEND-PENDING; 2026-08-08 UTC |
| EVIDENCE-G0-005 | default text template 200자와 등록 도메인 link 요구 | <https://developers.kakao.com/docs/latest/ko/message/message-template> | EXTERNAL-VERIFIED; 2026-08-08 UTC |
| EVIDENCE-G0-006 | Kakao REST 공식 문서에 PKCE 계약이 명시되지 않음 | <https://developers.kakao.com/docs/latest/ko/kakaologin/rest-api> | EXTERNAL-VERIFIED limitation; v1 PKCE 제외; 2026-08-08 UTC |
| EVIDENCE-G0-007 | oauth2-rs 배포 5.0.0 typestate API, `AuthType::RequestBody`, one-scope comma serialization, redirect 금지 | <https://docs.rs/oauth2/5.0.0/oauth2/>, <https://github.com/ramosbugs/oauth2-rs/tree/5.0.0> | EXTERNAL-VERIFIED + local crate source; 2026-08-08 UTC |
| EVIDENCE-G0-008 | 개발 앱 멤버 3~4명의 실제 friends/send E2E | 비밀값을 제거한 수동 실행 기록 | BLOCKING |
| EVIDENCE-G0-009 | HTTP 200의 완전한 success/failure 집합만 확정; non-200/transport/parse는 보수적으로 unknown | <https://developers.kakao.com/docs/latest/en/message/rest-api>, [`kakao.rs`](../../src/services/kakao.rs) unit fixtures | EXTERNAL/LOCAL-VERIFIED; live side effect pending |
| EVIDENCE-G0-010 | 반복 사용 수요와 안전한 landing page | 제품·보안 결정 기록 | BLOCKING |

향후 evidence 갱신은 다음 형식을 유지한다.

```text
claim_id | exact claim | direct URL | page section | checked_at (UTC)
         | fixture/test | verified_by | result | limitations
```

검색 결과 페이지나 문서 홈만으로는 `EXTERNAL-VERIFIED`가 아니다. DevTalk만으로 PKCE 같은 보안 기능을 구현 계약에 추가하지 않는다.

### 2.4 Verified provider constants and conservative v1 limits

다음 값은 기존 초안에서 가져온 보수적 후보다. Gate 0가 더 낮은 provider 한계를 확인하면 Spec을 먼저 수정한다. provider가 더 높은 값을 허용해도 v1 내부 상한은 늘리지 않는다.

| 이름 | v1 후보 | 상태 |
|---|---:|---|
| `MAX_RECIPIENTS` | 5 | EXTERNAL-VERIFIED + DESIGN |
| `MAX_TEXT_CHARS` | 200 Unicode scalar values | EXTERNAL-VERIFIED + conservative validation |
| friends page limit | 100 이하 | EXTERNAL-VERIFIED; UI 기본 20 |
| scopes | one OAuth scope value `friends,talk_message` | EXTERNAL/LOCAL-VERIFIED |
| OAuth dependency | `oauth2 = 5.0.0` | LOCAL-VERIFIED against published source |
| message template | Kakao default friend text template | EXTERNAL-VERIFIED |

---

## 3. Product Contract

### 3.1 Normative v1 promise

- UI entry is Settings connector adjacent only.
- The operator manually selects 1..=5 recipients.
- The operator enters 1..=200 characters.
- The server supplies a fixed `landing_url`; the client cannot override it.
- The operator must set `confirmed=true` immediately before send.
- One operation causes at most one provider message POST.
- The result is one of `success`, `partial_success`, `failed`, `unknown`.
- v1 does not expose an AgentDesk result, card, attachment, or private route to the recipient.

### 3.2 Application command

Provider DTOs MUST NOT cross into routes/UI. The application command is provider-neutral enough for the current use case without becoming a trait.

```rust
pub struct KakaoFriendShareCommand {
    pub receiver_uuids: Vec<String>,
    pub text: String,
    pub confirmed: bool,
}
```

`Idempotency-Key` is an HTTP header and operation identity, not a body field.

### 3.3 Deferred extraction rule

`ExternalShareChannel` MAY be introduced only when a second production channel exists and both implementations demonstrate the same stable operations. Extraction must answer:

- recipient identity and pagination differences
- connection status ownership
- at-most-once/idempotency support differences
- partial failure representation
- message/template capability negotiation
- object-safety or static dispatch requirement

Until then, concrete modules and application DTOs provide modularity.

---

## 4. Architecture Contract

### 4.1 Module layout

```text
src/services/oauth_connection.rs  # sessions/accounts, XChaCha vault, refresh lease
src/services/kakao.rs              # concrete OAuth/friends/message service and status
src/services/external_share.rs     # provider-neutral durable at-most-once operation store
src/server/routes/kakao.rs         # thin HTTP/error-boundary handlers
```

### 4.2 Dependencies

```text
routes/kakao
  ├─ KakaoFriendShareService
  └─ oauth_connection

KakaoFriendShareService
  ├─ external_share
  └─ oauth_connection

operator_connectors
  └─ kakao::connection_status

external_share
  └─ sqlx + sha2 (no Kakao/Discord dependency)

scheduled_messages
  ├─ message_outbox (existing Discord handoff)
  └─ external_share_outbox
       └─ kakao (stable outbox-derived Idempotency-Key)
```

Forbidden dependency edges:

```text
kakao/external_share/oauth_connection
  -X-> message_outbox
  -X-> discord::outbound
  -X-> discord::session_relay_sink::journal
  -X-> turn_bridge
```

### 4.3 Reuse inventory

#### MUST reuse

| Asset | Use |
|---|---|
| Existing idempotency fingerprint principle | dedicated length-prefixed, recipient-order-independent SHA-256 fingerprint |
| `operator_connectors` response and Settings panel | connector catalog and operator surface |
| `public_api_domain` | OAuth callback |
| `protected_api_domain` | OAuth start/disconnect, friends, send |
| `AppError` + existing ErrorCode | all protected-route errors |
| `utils::redact::register_known_secret` | environment secret material registration |
| environment-only secrets and redacted token wrapper | key/client secret safety |
| existing reqwest 0.12 rustls client stack | outbound HTTPS |
| Postgres primary/unique constraints | multi-node operation authority |

#### MUST NOT reuse

| Asset | Reason |
|---|---|
| `idempotency::claim` and expiry reclaim | Kakao POST is not replay-safe |
| `idempotency::release_unclaimed` after a send fence | deleting the fence could allow duplicate POST |
| `delivery_journal_events` and its raw writer | Discord-specific schema, ownership, hot surface |
| Discord `message_outbox` transport/retry/rate paths | Kakao transport와 quota에는 재사용 금지. 단, 예약 오케스트레이터는 기존 Discord enqueue primitive를 sibling obligation으로 그대로 사용 |
| `kv_meta['settings']` | company settings is not daemon integration config |
| process-local mutex/rate map as correctness authority | cluster mode exists |
| provider ID branches throughout the generic row | common fields/actions stay generic; one Kakao composer owns provider-specific UX |

---

## 5. Data Model

구현 migration은 현재 HEAD의 next-free 번호인 [`0107_kakao_friend_share.sql`](../../migrations/postgres/0107_kakao_friend_share.sql)이며 immutable checksum manifest에 포함된다.

### 5.1 OAuth sessions

```sql
CREATE TABLE oauth_connection_sessions (
    id                        UUID        PRIMARY KEY,
    provider                  TEXT        NOT NULL,
    state_hash                BYTEA       NOT NULL,
    expires_at                TIMESTAMPTZ NOT NULL,
    consumed_at               TIMESTAMPTZ,
    created_at                TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (provider, state_hash),
    CHECK (octet_length(state_hash) = 32),
    CHECK (length(provider) BETWEEN 1 AND 64)
);

CREATE INDEX oauth_connection_sessions_expiry_idx
    ON oauth_connection_sessions (expires_at);
```

Contract:

- `state` raw value is never stored.
- state entropy is at least 128 bits from an OS CSPRNG.
- TTL is exactly 10 minutes.
- callback consumes the matching unexpired row atomically using `UPDATE ... WHERE consumed_at IS NULL ... RETURNING`.
- callback return target is fixed; no `return_path` column or user-supplied redirect path exists in v1.
- expired/consumed sessions are safe to delete.

### 5.2 OAuth accounts

```sql
CREATE TABLE oauth_connection_accounts (
    provider                  TEXT        NOT NULL,
    account_key               TEXT        NOT NULL,
    token_ciphertext          BYTEA       NOT NULL,
    token_nonce               BYTEA       NOT NULL,
    key_version               SMALLINT    NOT NULL DEFAULT 1,
    scopes                    TEXT[]      NOT NULL DEFAULT '{}',
    access_expires_at         TIMESTAMPTZ,
    refresh_expires_at        TIMESTAMPTZ,
    status                    TEXT        NOT NULL,
    refresh_lease_id          UUID,
    refresh_lease_expires_at  TIMESTAMPTZ,
    created_at                TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at                TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (provider, account_key),
    CHECK (octet_length(token_nonce) = 24),
    CHECK (key_version > 0),
    CHECK (status IN ('active', 'consent_incomplete', 'reauth_required')),
    CHECK ((refresh_lease_id IS NULL) = (refresh_lease_expires_at IS NULL))
);
```

Contract:

- v1 `account_key` is always `primary`.
- access/refresh token live inside one encrypted envelope.
- provider external user ID is not stored in v1 because it is not needed for the product contract.
- token expiry columns are nullable because provider/token responses may omit them; Gate 0 fixes exact parsing.
- no `disabled` row exists. Local disconnect deletes the account row and outstanding provider sessions.
- remote Kakao unlink is out of scope.

Encrypted token envelope:

```json
{
  "access_token": "secret",
  "refresh_token": "secret-or-null"
}
```

The Rust type MUST NOT implement `Debug`, MUST zeroize on drop, and may serialize only into an immediately encrypted zeroizing buffer. No serialized plaintext crosses a service boundary.

### 5.3 External share operation fence

```sql
CREATE TABLE external_share_operations (
    operation_id          UUID        PRIMARY KEY,
    provider              TEXT        NOT NULL,
    channel_id            TEXT        NOT NULL,
    account_key           TEXT        NOT NULL,
    idempotency_key_hash  BYTEA       NOT NULL,
    request_fingerprint   BYTEA       NOT NULL,
    state                 TEXT        NOT NULL,
    safe_summary          JSONB,
    dispatch_deadline     TIMESTAMPTZ NOT NULL,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (provider, channel_id, account_key, idempotency_key_hash),
    CHECK (state IN ('dispatching', 'success', 'partial_success', 'failed', 'unknown')),
    CHECK (octet_length(idempotency_key_hash) = 32),
    CHECK (octet_length(request_fingerprint) = 32),
    CHECK (
      (state = 'dispatching' AND safe_summary IS NULL)
      OR (state <> 'dispatching' AND safe_summary IS NOT NULL)
    )
);

CREATE INDEX external_share_operations_rate_idx
    ON external_share_operations (provider, channel_id, account_key, created_at DESC);
```

Contract:

- `provider = 'kakao'`, `channel_id = 'kakao_friend_share'`, `account_key = 'primary'` in v1.
- row insert is the durable at-most-once fence.
- raw recipient UUIDs, nickname, text, landing URL, token, provider raw response are not stored.
- `idempotency_key_hash` and `request_fingerprint` are 32-byte SHA-256 values; the fingerprint length-prefixes text, landing URL, and sorted recipient UUIDs.
- `safe_summary` contains only requested/success/failed counts. Replay flags are reconstructed at read time.
- `dispatching` is never deleted or reclaimed for a second POST.
- an expired dispatch deadline may be atomically converted to terminal `unknown`; it is never converted back to `dispatching`.
- terminal rows are not automatically deleted in v1. Future retention may compact `safe_summary`, but MUST retain the key hash, fingerprint, and terminal tombstone.
- this table is a correctness fence and replay cache, not a general analytics event log.

---

## 6. State Machines

### 6.1 OAuth session

```text
absent
  → created(unconsumed, expires_at=now+10m)
  → consumed
  ↘ expired
```

- Only `created` and unexpired may exchange a code.
- Two callbacks racing on the same state: exactly one consumes; the other redirects with `invalid_state`.
- Error query callbacks consume the state when it exists so the same session cannot be reused.

### 6.2 Account connection

```text
row absent (disconnected)
  → active
  → consent_incomplete
  → reauth_required
  → DELETE (local disconnect)
```

State derivation:

- `active`: token decrypts, required scopes are present, refresh is not known-invalid.
- `consent_incomplete`: token is valid but required consent/scope is missing.
- `reauth_required`: token is revoked/expired without safe refresh or a refresh outcome is ambiguous.
- decryption integrity failure is reported as `invalid_config` externally and MUST NOT log ciphertext/token material.

### 6.3 Refresh lease

1. Read account; if access token remains valid outside a small skew window, return it.
2. In one short update, claim `refresh_lease_id` only from `active` and pre-mark the row `reauth_required`. A crash or persistence ambiguity can therefore never make the old refresh token automatically eligible again.
3. A concurrent lease loser returns a bounded conflict/`Retry-After`; it never calls provider refresh concurrently.
4. Lease owner performs exactly one refresh request.
5. On a definitive success, replace encrypted envelope with `WHERE refresh_lease_id = $lease` fencing and clear lease.
6. If the response omits a new refresh token, preserve the old one **only after EVIDENCE-G0-001/EVIDENCE-G0-007 confirm this contract**.
7. On definitive invalid-grant/revocation, set `reauth_required` and clear lease.
8. On timeout, reset, ambiguous 5xx, process crash at any point after the lease update, or expired lease, the pre-marked `reauth_required` state remains sticky; do not automatically retry the old refresh token.

Holding a process-local mutex or a DB transaction open across the HTTP request is not the contract.

### 6.4 External share operation

```text
absent
  → dispatching
      → success
      → partial_success
      → failed
      → unknown
```

No terminal state transitions back to `dispatching`.

Normative flow:

1. Authenticate caller.
2. Validate `Idempotency-Key` as 8..=128 safe-ASCII bytes. The Dashboard uses a UUID, but the wire contract is deliberately provider-neutral.
3. Validate `confirmed`, recipient count/uniqueness, text length, and config.
4. Resolve canonical connection status and ensure/refresh token.
5. Load the DB-backed send safety-limit policy.
6. Compute request fingerprint.
7. In one short transaction, acquire a stable per-account transaction advisory lock, count the trailing-hour operations, enforce the cap, and insert the `dispatching` row.
8. On PK conflict:
   - different fingerprint → 422;
   - terminal state → replay terminal response;
   - `dispatching` before deadline → 409 in-flight;
   - `dispatching` after deadline → atomically mark/replay `unknown`.
9. Only the successful inserter may issue one Kakao message POST. The outbound client has no automatic POST retry middleware.
10. Classify response and compare-and-set `dispatching → terminal`.
11. If terminal persistence fails or the row was already marked `unknown`, return `unknown`; never issue a second POST.

An explicit same-payload result check reuses the in-memory key and can only replay or begin the not-yet-created operation. An intentional resend after `failed`, `partial_success`, or `unknown` requires a newly generated key; after a result that may have delivered, UI MUST show a duplicate-risk confirmation first.

---

## 7. Cryptographic Contract

### 7.1 Algorithm and key

- Algorithm: `XChaCha20-Poly1305` only.
- Key: strict base64 decoding of exactly 32 bytes from `AGENTDESK_OAUTH_TOKEN_KEY_V1`.
- Nonce: exactly 24 fresh random bytes for every seal operation.
- `key_version`: exactly `1` in v1.
- Unknown key version, invalid base64, invalid key length, bad nonce length, or authentication failure: fail closed.

### 7.2 AAD

AAD is UTF-8 with exact format:

```text
agentdesk/oauth-account/v1/{provider}/{account_key}
```

Ciphertext copied between providers or account keys must not decrypt. OAuth sessions contain no secret material in v1.

### 7.3 Rotation

v1 does not claim live key rotation. Rotating `V1` requires local disconnect/reconnect of stored accounts before the old key is removed. A later multi-key keyring needs its own migration and runbook; `key_version` alone is not a rotation mechanism.

### 7.4 Secret handling

- Register environment secret values with the existing redaction utility during bootstrap.
- Config serialization and Debug MUST omit secret values.
- HTTP clients MUST disable redirects for token/API requests unless a specific endpoint contract requires otherwise.
- Authorization code, raw state, token, and provider request bodies MUST NOT appear in tracing fields.

---

## 8. Config Contract

### 8.1 YAML

```yaml
integrations:
  kakao_friend_share:
    enabled: false
    redirect_uri: "http://127.0.0.1:8791/api/kakao/oauth/callback"
    landing_url: "https://example.com/agentdesk"
    send_limit_per_hour: 30
```

This requires a new optional `IntegrationsConfig` owned by daemon bootstrap. It is restart-required in v1.

Validation:

- `redirect_uri`: absolute HTTPS, or HTTP only on loopback; no credentials or fragment. The exact value is used in authorize and token exchange.
- `landing_url`: absolute HTTPS, or HTTP only on loopback; no credentials or fragment. Rollout requires an approved externally reachable HTTPS page.
- `send_limit_per_hour`: integer `>= 1`.
- missing secret env does not crash core AgentDesk; connector becomes `missing_config` when integration is enabled.
- changing integration config updates the shared snapshot but feature consumers require restart in v1.

### 8.2 Environment secrets

```text
KAKAO_REST_API_KEY
KAKAO_CLIENT_SECRET
AGENTDESK_OAUTH_TOKEN_KEY_V1
```

The environment variable names are fixed code constants, not YAML indirection. No secret value enters YAML, dashboard company settings, runtime-config, connector JSON, or logs.

### 8.3 Config taxonomy update

The PR adding `IntegrationsConfig` MUST update:

- `docs/config-domains.md`
- `docs/adr-settings-precedence.md` if ownership/precedence needs a new row
- Config hot-reload comment/tests
- config audit/example fixtures

The integration block must have exactly one authoritative home.

---

## 9. Canonical Connection and Connector Contract

### 9.1 Internal status

```rust
enum KakaoConnectionState {
    Disabled,
    MissingConfig,
    NotConnected,
    ConsentIncomplete,
    ReauthorizationRequired,
    Connected,
    StorageUnavailable,
    InvalidConfig,
}
```

`KakaoConnectionStatusResolver` reads config, presence of required env values, account row, scopes, expiry, and vault-open health. It returns safe metadata only. Both the connector endpoint and share service use this resolver.

### 9.2 Mapping

| Kakao state | OptionalConnectorState | Actions |
|---|---|---|
| Disabled | `Skipped` | none |
| MissingConfig | `MissingConfig` | none |
| NotConnected | `MissingConfig` | `connect` |
| ConsentIncomplete | `InvalidConfig` | `reconnect`, `disconnect` |
| ReauthRequired | `InvalidConfig` | `reconnect`, `disconnect` |
| Connected | `Ready` | `reconnect`, `disconnect`, `test_send` |
| StorageUnavailable | `MissingProvider` | none |
| InvalidConfig | `InvalidConfig` | `reconnect`, `disconnect` when an account exists |

### 9.3 Coordinated response evolution

`GET /api/settings/operator-connectors` remains the catalog endpoint but becomes async and accepts `State<AppState>`.

```json
{
  "id": "kakao_friend_share",
  "name": "Kakao Friend Share",
  "kind": "oauth",
  "state": "ready",
  "optional": true,
  "env_var": "KAKAO_REST_API_KEY",
  "env_vars": ["KAKAO_REST_API_KEY", "KAKAO_CLIENT_SECRET", "AGENTDESK_OAUTH_TOKEN_KEY_V1"],
  "source": null,
  "reason": null,
  "detail": "state=ready provider=kakao",
  "setup_actions": [],
  "capabilities": ["kakao_friend_list", "kakao_friend_message"],
  "connection": {
    "state": "connected",
    "reason": null,
    "scopes": ["friends", "talk_message"],
    "access_expires_at": "2026-08-09T12:00:00Z",
    "landing_url": "https://example.com/agentdesk"
  },
  "actions": ["reconnect", "disconnect", "test_send"]
}
```

Contract changes are intentional and MUST land atomically in backend DTO, Dashboard TypeScript, component rendering, and fixtures:

- add `kind: filesystem | oauth` to all connector rows;
- retain `env_var` and add `env_vars` for multi-secret connectors;
- retain nullable `source`;
- add nullable `connection`;
- add `actions` array;
- existing summary fields and existing connector states remain stable;
- filesystem rows keep their current semantics and set `connection=null`.

Dashboard rules:

- generic `ConnectorRow` conditionally renders fields based on presence/kind;
- generic row renders common fields based on `kind`; the adjacent Kakao control owns its concrete actions and composer;
- provider-specific API dispatch lives in one typed connector action adapter, not inline `id === ...` branches throughout the component;
- Recheck reloads the same canonical endpoint after OAuth/disconnect;
- doctor may report static configuration readiness, but MUST label DB connection state as `not_probed` unless it is given DB access.

No `/api/kakao/status` endpoint is added.

---

## 10. HTTP Wire Contract

All paths below are relative to `/api`.

### 10.1 Public OAuth callback

#### `GET /kakao/oauth/callback`

Query inputs accepted from provider:

- success: `code`, `state`
- error: `error`, optional `error_description`, `state`

Raw query values MUST NOT be logged.

Success response:

```http
HTTP/1.1 303 See Other
Location: /settings?connector=kakao_friend_share&oauth=ok
```

Failure response:

```http
HTTP/1.1 303 See Other
Location: /settings?connector=kakao_friend_share&oauth=error&reason=invalid_state
```

Allowed safe reasons:

```text
denied | invalid_state | expired | token_exchange | consent | internal
```

There is no dynamic return path and no HTML 200 fallback.

### 10.2 Protected OAuth start

#### `POST /kakao/oauth/start`

Request body: none.

Response `200`:

```json
{
  "authorize_url": "https://kauth.kakao.com/oauth/authorize?...",
  "expires_in_seconds": 600
}
```

The dashboard navigates to the returned URL. The protected route itself does not redirect.

### 10.3 Protected local disconnect

#### `DELETE /kakao/connection`

Response `200`:

```json
{
  "ok": true,
  "connector_id": "kakao_friend_share",
  "connection_state": "disconnected",
  "remote_unlinked": false
}
```

The handler deletes the local account row and outstanding Kakao OAuth sessions in one transaction. It does not claim to revoke the Kakao account grant.

### 10.4 Protected friends list

#### `GET /kakao/friends?offset=0&limit=20`

- default internal limit: 20
- verified upper limit: 100; AgentDesk UI defaults to 20
- v1 omits thumbnails and does not cache friends

Response `200`:

```json
{
  "total_count": 11,
  "offset": 0,
  "limit": 20,
  "next_offset": null,
  "friends": [
    {
      "uuid": "provider-required-opaque-id",
      "display_name": "이수민"
    }
  ]
}
```

UUID policy:

| Surface | Rule |
|---|---|
| friends/send request and response | allowed because selection requires it |
| server logs, metrics, AppError context | forbidden |
| OAuth/account table | forbidden |
| operation table | forbidden; fingerprint only |
| browser persistence | forbidden; component memory only |

### 10.5 Protected send

#### `POST /kakao/messages/send`

Required header:

```http
Idempotency-Key: 550e8400-e29b-41d4-a716-446655440000
```

Request:

```json
{
  "receiver_uuids": ["opaque-1", "opaque-2"],
  "text": "AgentDesk 공유: 작업 요약",
  "confirmed": true
}
```

Validation before operation insert:

- header is 8..=128 ASCII alphanumeric/`-_.:` characters; Dashboard generates a UUID;
- `confirmed == true`;
- 1..=5 unique non-empty UUID strings;
- text trim result is 1..=200 Unicode scalar values;
- connector is Connected;
- landing URL is valid;
- token refresh is complete;
- hourly safety limit is available.

Success response `200`:

```json
{
  "request_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "success",
  "requested_count": 2,
  "successful_count": 2,
  "failed_count": 0,
  "replayed": false,
  "delivery_may_have_occurred": true,
  "automatic_retry_allowed": false
}
```

Partial response `200`:

```json
{
  "request_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "partial_success",
  "requested_count": 3,
  "successful_count": 2,
  "failed_count": 1,
  "replayed": false,
  "delivery_may_have_occurred": true,
  "automatic_retry_allowed": false
}
```

Definitive failure response `200`:

```json
{
  "request_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "failed",
  "requested_count": 2,
  "successful_count": 0,
  "failed_count": 2,
  "replayed": false,
  "delivery_may_have_occurred": false,
  "automatic_retry_allowed": false
}
```

Unknown response `200`:

```json
{
  "request_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "unknown",
  "requested_count": 2,
  "successful_count": 0,
  "failed_count": 0,
  "replayed": false,
  "delivery_may_have_occurred": true,
  "automatic_retry_allowed": false
}
```

`request_id` is the server-generated operation UUID, not the raw Idempotency-Key. Replay reconstructs the safe count-only terminal body with `replayed=true`; the manual-send operation store contains no message preview, raw text, recipient UUID, or raw key. Scheduled fan-out keeps the existing `scheduled_messages.content` as the single authoritative Discord reservation payload, but adds no provider-specific plaintext text copy: recipient targets and pending provider snapshots remain AEAD ciphertext.

### 10.6 Exact protected-route errors

| Case | HTTP | ErrorCode | Required context/headers |
|---|---:|---|---|
| invalid/missing body or Idempotency-Key | 400 | `Validation` | `operation` only |
| operation key reused with different payload | 422 | `Validation` | `operation`, `reason=fingerprint_mismatch` |
| same operation currently before dispatch deadline | 409 | `Conflict` | `operation`, safe `retry_after_seconds` |
| disconnected/consent/reauth required | 409 | `Conflict` | `operation`, safe connection state |
| integration disabled or required env absent | 503 | `Config` | `operation`, safe reason |
| local send safety limit exceeded | 429 | `Policy` | `Retry-After` |
| friends provider failure before a send fence | 502 | `Dispatch` | `operation` only |
| DB failure before operation insert | 500 | `Database` | `operation` |
| unexpected local failure before dispatch | 500 | `Internal` | `operation` |

After the operation fence is inserted, ambiguous provider/DB failures are a `200 unknown` business result, not a 5xx that invites automatic client retry.

AppError envelope remains:

```json
{
  "error": "safe operator message",
  "code": "validation",
  "context": { "operation": "kakao.messages.send" }
}
```

No provider raw error message, UUID, token, state, code, or ciphertext enters `context`.

---

## 11. Provider Adapter Contract

The wire constants below are verified against EVIDENCE-G0-001..009; live account eligibility remains a rollout gate.

### 11.1 Endpoint allowlist

| Use | URL | Status |
|---|---|---|
| authorize | `https://kauth.kakao.com/oauth/authorize` | EXTERNAL-VERIFIED |
| token | `https://kauth.kakao.com/oauth/token` | EXTERNAL-VERIFIED |
| friends | `https://kapi.kakao.com/v1/api/talk/friends` | EXTERNAL-VERIFIED |
| send | `https://kapi.kakao.com/v1/api/talk/friends/message/default/send` | EXTERNAL-VERIFIED |

No endpoint is configurable by the client. Any test base URL override is test-only and unavailable in production config.

### 11.2 OAuth behavior

- `state` is mandatory, random, hashed at rest, and single-consume.
- Kakao REST official docs do not establish a PKCE contract; v1 does not generate or persist a verifier.
- authorize uses one `oauth2::Scope` whose value is `friends,talk_message`, producing the comma-delimited Kakao parameter instead of OAuth's default space join.
- `KAKAO_REST_API_KEY` is client ID and `KAKAO_CLIENT_SECRET` uses `AuthType::RequestBody`.
- refresh redirect following is disabled.
- token scope and consent are normalized into an unordered set before status derivation.
- a typed oauth2-rs extra-token field captures Kakao `refresh_token_expires_in`; a refresh response that omits the field preserves the prior absolute expiry just as an omitted refresh token preserves the prior token.

The code pins the compatible published major/minor contract at `oauth2 = 5.0.0`, commits `Cargo.lock`, and disables HTTP redirects as recommended by the crate's security guidance.

### 11.3 Message behavior

Candidate form payload:

```text
receiver_uuids=<JSON array string>
template_object=<JSON object string>
```

Candidate text template:

```json
{
  "object_type": "text",
  "text": "<validated text>",
  "link": {
    "web_url": "<server landing_url>",
    "mobile_web_url": "<server landing_url>"
  }
}
```

- client input cannot select object type or link.
- provider response is first parsed into a provider DTO, then mapped to count-only application results.
- the provider message POST has no transport-level or middleware retry; the operation state machine is the only retry authority.
- unknown or oversized provider fields are rejected/bounded; they are not copied into logs or operation JSON.
- only an HTTP 200 response whose failure UUID set completely accounts for every requested recipient may become `failed`.
- timeout, connection reset, truncated body, unparseable success/error body, ambiguous 5xx, and POST-after-response-persistence failure become `unknown`.

---

## 12. Failure Matrix

### 12.1 Send operation

| Boundary | Situation | DB action | Client result | Same key later | Provider POST count |
|---|---|---|---|---|---:|
| before fence | validation/config/connection/refresh/rate failure | none | exact AppError | may retry | 0 |
| fence insert | new key | insert `dispatching` | continue | n/a | 0 then at most 1 |
| fence insert | fingerprint mismatch | none | 422 | always 422 | 0 additional |
| fence insert | active dispatching | none | 409 | 409 until deadline | 0 additional |
| fence recovery | expired dispatching | CAS to `unknown` | 200 unknown | replay unknown | 0 additional |
| before POST crash | row remains dispatching | later unknown | 200 unknown | replay unknown | 0 |
| provider full success | CAS success | 200 success | replay success | 1 |
| provider partial success | CAS partial | 200 partial | replay partial | 1 |
| provider definitive reject | CAS failed | 200 failed | replay failed | 1 |
| provider timeout/reset/ambiguous | CAS unknown | 200 unknown | replay unknown | 1 or unknown at transport |
| after POST, terminal DB write fails | row remains dispatching | 200 unknown | later unknown | at most 1 |
| original completes after another request marked unknown | CAS fails | 200 unknown | replay unknown | at most 1 |

There is no `release_unclaimed`, TTL delete, reclaim, or automatic re-POST after the fence.

### 12.2 OAuth/refresh

| Situation | State/result |
|---|---|
| invalid/expired/consumed callback state | fixed 303 error redirect |
| authorization denied | consume state, fixed 303 denied |
| token exchange definitive invalid code | fixed 303 token_exchange; no account row |
| token exchange ambiguous | fixed 303 token_exchange; no automatic exchange retry |
| incomplete consent | account `consent_incomplete`; connector InvalidConfig |
| access expired, safe refresh success | replace token under lease |
| refresh definitive revoked | `reauth_required` |
| refresh timeout/reset/ambiguous/lease expiry | `reauth_required`; no automatic retry |
| vault integrity/key error | fail closed; connector InvalidConfig; no secret logs |

---

## 13. Rate, Timeout, and Cluster Contract

### 13.1 Send safety limit

- limit source: `integrations.kakao_friend_share.send_limit_per_hour`
- scope: `(provider, channel_id, account_key)`
- count: all newly inserted operations with `created_at` in the trailing hour, including `dispatching` and every terminal state
- replay and fingerprint mismatch do not increment the count
- enforcement reuses the repository's transaction-scoped Postgres advisory-lock pattern: acquire a dedicated namespace plus a stable hash of `(provider, channel_id, account_key)`, count the trailing-hour rows, and insert the operation fence in the same short transaction
- the advisory lock is released at commit before the provider HTTP request; it serializes only rate-check + fence creation, not network I/O
- a hash collision may serialize unrelated accounts but MUST NOT weaken correctness; the namespace constant is allocated and regression-tested with other advisory-lock users

No friends-list or OAuth-start bespoke limiter is added in v1. These routes are protected, operator-only, and provider rate responses are mapped explicitly. A future abuse case must justify a shared limiter rather than a process-local map.

### 13.2 Timeouts

v1 fixes the following conservative transport constraints; live E2E may only tighten them:

- connect timeout is bounded;
- total provider request timeout is 10 seconds, less than the 20-second `dispatch_deadline` window;
- friends/send JSON response bodies have strict byte limits;
- redirects are disabled;
- timeout after fence maps to `unknown`.

### 13.3 Cluster

- correctness relies on Postgres constraints and leases, not node role.
- any authenticated node may serve connector/friends/send routes if it can reach the same DB and secret configuration.
- a missing Postgres pool makes the optional connector unavailable (`503 Database`); it does not fall back to in-memory storage.

---

## 14. Privacy and Observability

### 14.1 v1 observability budget

v1 does not add a Kakao-specific metrics family or a parallel audit pipeline. HTTP health remains visible through the existing server telemetry, while pilot send counts are derived from `external_share_operations` by terminal state. This keeps the first release small and makes the durable operation row the single aggregate source of truth.

If recurring operational demand later justifies provider metrics, labels MUST remain bounded to fixed operation/outcome enums. They MUST NOT contain account ID, UUID, nickname, text, URL, token, request ID, provider raw code, or OAuth state.

### 14.2 Allowed logs

- existing request middleware's route/status/elapsed fields
- fixed operation name or safe state/outcome enum
- recipient count only when needed for aggregate diagnosis
- whether a result was replayed
- the server-generated operation ID, which is not a provider/user identifier

The current implementation adds only a warning for failure to persist a terminal operation state, keyed by the server-generated operation ID. It does not log the provider response, request fingerprint, recipients, or message.

### 14.3 Persistence boundaries

The manual Settings send path and all provider-specific/OAuth/operation stores forbid:

- friends list/cache
- plaintext UUID arrays
- nickname or thumbnail
- message text or preview
- authorization query
- token response JSON
- provider raw failure body

A confirmed scheduled fan-out keeps the pre-existing `scheduled_messages.content` because it is the authoritative Discord reservation payload. It MUST NOT add another plaintext Kakao text copy. Active recipient targets and per-fire provider snapshots are allowed only inside UUID-bound AEAD ciphertext; terminal definition/outbox rows scrub that ciphertext and retain count-only summaries.

The browser keeps friend data only while the inline Settings composer is open and clears friends, selections, and message text when it closes. It does not copy the list to local storage, session storage, query caches, or server persistence.

---

## 15. Dashboard Contract

### 15.1 Connector actions

- `connect`: call start endpoint, then navigate to authorize URL.
- `reconnect`: same as connect; it does not delete the old row until callback succeeds.
- `disconnect`: confirm “AgentDesk의 로컬 연결 정보만 삭제” then DELETE connection.
- `test_send`: toggle the inline Kakao test-share composer in the connector row.
- OAuth return query displays a safe toast, removes the query parameters with history replacement, then reloads connectors.

### 15.2 Inline test-share composer

1. Require connector action `test_send`.
2. Load the first friends page on open, allow an explicit refresh, and append provider pagination only when the operator clicks “more”.
3. Keep selected UUIDs only in component memory.
4. Enforce max 5 and display character count.
5. Show the server landing URL read-only.
6. Require an immediate browser confirmation for every send; the API still requires `confirmed=true` as a second server-side gate.
7. Generate one UUID after confirmation and immediately before the first request for an intent. Keep the key only in component memory while that intent remains ambiguous or partial. The dashboard configures the unsafe POST with zero transport retries.
8. Disable double submit while the request is pending.
9. An operator-triggered check of the identical payload reuses the in-memory key. Server-side reuse may only replay the existing result or create the operation if the first request never reached AgentDesk; it never issues a second provider POST for an accepted key.
10. Network ambiguity, `unknown`, or `partial_success` sets a persistent duplicate-risk guard. A changed-payload/new-key send requires both the duplicate-risk confirmation and the ordinary send confirmation. Editing text or recipients changes the fingerprint but does not silently clear the guard.
11. Closing the composer clears friend rows, selected UUIDs, text, result, and the pending key/fingerprint. A generic duplicate-risk flag survives the close within the mounted Settings view so sensitive payload is discarded without silently removing the safety warning.

There is no global toolbar or result-card entry in v1.

### 15.3 Dashboard response boundary

- Every new Kakao Dashboard API call passes a zod response schema as the third `request` argument; TypeScript types are inferred from those schemas rather than maintained as parallel interfaces.
- OAuth start, local disconnect, friends pagination, and send-result payloads fail closed before malformed data can enter the response cache or UI state.
- The schemas retain the safety bounds from this Spec: HTTPS-shaped authorization URL, local-only `remote_unlinked=false`, at most 100 friends per page, 1..=5 requested recipients, UUID operation ID, the closed send-status set, status/count/delivery-risk consistency, and `automatic_retry_allowed=false`.
- Component-level authorization endpoint allowlisting remains a second, stricter check after schema parsing.

---

## 16. Requirement Registry

- [REQ-001] v1 is Settings-only manual text + fixed landing URL; it is not result/card sharing.
- [REQ-002] Official wire evidence MUST precede provider code. Console feasibility, live E2E, recurring demand, and landing safety MUST pass before the default-disabled feature is activated for rollout.
- [REQ-003] Kakao send is manual by default. The only automatic path is an operator-confirmed `push` scheduled-message fan-out with encrypted targets. A PATCH that materially changes retained Kakao content or timing MUST renew that confirmation; event-driven, implicit background, Kakao-only, and `agent` fan-out are forbidden.
- [REQ-004] Kakao/external_share/oauth modules MUST NOT depend on Discord outbox/outbound/delivery journal/turn bridge.
- [REQ-005] v1 has no public share artifact, recipient authorization, or result deep link.
- [REQ-006] v1 uses a concrete Kakao service; no channel trait/registry until a second implementation exists.
- [REQ-007] Every send uses `external_share_operations` as a durable at-most-once fence.
- [REQ-008] Kakao send MUST NOT use generic idempotency claim/reclaim/release semantics, Discord transport retry, or Discord delivery journal writers. Scheduled orchestration MAY reuse the existing Discord outbox enqueue primitive as an independent sibling handoff.
- [REQ-009] A `dispatching` or terminal operation is never automatically re-dispatched.
- [REQ-010] A validated 8..=128 byte safe-ASCII `Idempotency-Key` plus canonical fingerprint controls replay and mismatch; the dashboard generates UUIDs.
- [REQ-011] Send outcome is exactly success/partial_success/failed/unknown with the fixed meaning in this Spec.
- [REQ-012] Access and refresh tokens use the fixed XChaCha/nonce/AAD/key contract; v1 stores no unverified PKCE material.
- [REQ-013] OAuth state is hashed, 10-minute, single-consume, and callback target is fixed.
- [REQ-014] Token refresh is DB-lease serialized; ambiguous refresh requires reauthentication.
- [REQ-015] Local disconnect deletes account/session rows and does not claim remote unlink.
- [REQ-016] Callback is public; start/disconnect/friends/send are protected.
- [REQ-017] One canonical Kakao connection resolver feeds connector UI and share execution.
- [REQ-018] Connector endpoint/UI preserves existing filesystem fields and adds kind, env-var list, safe connection metadata, and typed actions. Provider UI is confined to one connector-composition adapter rather than branching throughout generic row fields.
- [REQ-019] Integration config has one restart-required YAML home; secret values are fixed env variables.
- [REQ-020] Protected errors use the exact HTTP/ErrorCode table and existing AppError envelope.
- [REQ-021] UUID/nickname/text/token/provider raw body persistence and logging follow the privacy matrix.
- [REQ-022] v1 enforces confirmed, 1..=5 unique recipients, and 1..=200 Unicode scalar text.
- [REQ-023] Link is server-fixed `landing_url`; client cannot send or override a link/template type.
- [REQ-024] Send safety limit is DB-backed and cluster-consistent; process-local guards are not correctness authority.
- [REQ-025] Provider-specific endpoints, comma-delimited scopes, refresh-token omission, and response classification require direct official evidence; live fixtures remain an activation gate. Unverified PKCE behavior MUST NOT be invented.
- [REQ-026] Each route/config/migration PR updates inventory, checksum, and taxonomy in the same PR.
- [REQ-027] HTTP request/response schemas in this Spec are normative and contain no unresolved status alternatives.
- [REQ-028] Crash-before-POST, crash-after-POST, terminal-write failure, and multi-node races are blocking tests for rollout activation; unverified paths stay default-disabled.
- [REQ-029] Missing Postgres or connector configuration fails the optional feature closed without blocking core AgentDesk.
- [REQ-030] Product entrypoints beyond Settings require the PRD pilot promotion gate.
- [REQ-031] Every new Kakao Dashboard endpoint validates its response with a zod parser before caching or rendering; inferred types and runtime schemas have one owner.
- [REQ-032] A scheduled push with Kakao targets MUST create its Discord `message_outbox` row and encrypted `external_share_outbox` row in the same transaction after re-checking the active parent/delivery claim. Failure to enqueue either obligation MUST roll back both.
- [REQ-033] Scheduled Kakao outbox dispatch MUST derive a stable valid `Idempotency-Key` from the durable outbox UUID. A crash after provider dispatch replays the existing external operation and MUST NOT issue another POST.
- [REQ-034] Active scheduled targets and pending/processing outbox payloads MUST be AEAD encrypted. Terminal scheduled definitions and terminal outbox rows retain only PII-free summaries and MUST scrub ciphertext.
- [REQ-035] Scheduled provider outcomes are exposed as `providerDeliveries`; Discord success never causes Kakao compensation by re-sending Discord, and Kakao failure never rewinds an already committed Discord handoff.

---

## 17. Task Registry and Release Gates

The current change is one cohesive, default-disabled vertical slice. Keeping connection and manual send together avoids two temporarily unusable half-features, while the A/B IDs remain stable workstream identifiers for traceability. `LOCAL-IMPLEMENTED` means code exists on this branch; it is not live-provider or rollout evidence.

### Official evidence and rollout gates

- [TSK-G0-001] **EXTERNAL-VERIFIED** — complete the direct official-source ledger for OAuth, friends, send, limits, templates, and oauth2-rs 5.0.0.
- [TSK-G0-002] **ROLLOUT-BLOCKED** — verify console products, consent items, app membership, registered redirect/domain, and operational eligibility.
- [TSK-G0-003] **ROLLOUT-BLOCKED** — run sanitized 3–4 member OAuth/friends/send E2E and retain only non-secret outcomes.
- [TSK-G0-004] **PARTIAL** — unit fixtures cover scope normalization and result classification; add live-derived refresh/partial/ambiguous fixtures without retaining personal data.
- [TSK-G0-005] **ROLLOUT-BLOCKED** — confirm recurring operator need and approve the fixed landing page.
- [TSK-G0-006] **EXTERNAL-VERIFIED / ROLLOUT-BLOCKED** — provider constants are verified; activation readiness still depends on G0-002, G0-003, and G0-005.

### A — Secure connection workstream

- [TSK-A-001] **LOCAL-IMPLEMENTED** — add `IntegrationsConfig`, validation, secret redaction, example config, and taxonomy/hot-reload docs.
- [TSK-A-002] **LOCAL-IMPLEMENTED** — add token vault with XChaCha/AAD/key validation and unit tests.
- [TSK-A-003] **LOCAL-IMPLEMENTED** — add OAuth session/account migration, immutable checksum, atomic state consume, and opportunistic session GC.
- [TSK-A-004] **LOCAL-IMPLEMENTED** — add Kakao OAuth adapter, DB refresh lease, and callback/start/disconnect routes.
- [TSK-A-005] **LOCAL-IMPLEMENTED** — make connector projection async/state-aware; update backend DTO, TypeScript type, generic fields, and bounded Kakao action adapter.
- [TSK-A-006] **LOCAL-VERIFIED** — route inventory regeneration and source-of-truth checks pass on this branch.

### B — Manual share workstream

- [TSK-B-001] **LOCAL-IMPLEMENTED** — add `external_share_operations`, scoped advisory lock, non-reclaiming fence, terminal replay, and sticky unknown.
- [TSK-B-002] **LOCAL-IMPLEMENTED** — add friends and text-message provider adapters with bounded JSON and conservative response classification.
- [TSK-B-003] **LOCAL-IMPLEMENTED** — add concrete `KakaoFriendShareService`, DB-backed send cap, and operation state machine.
- [TSK-B-004] **LOCAL-IMPLEMENTED** — add protected friends/send routes with the normative wire types.
- [TSK-B-005] **LOCAL-IMPLEMENTED** — add the Settings inline composer, unknown/duplicate-risk UX, typed connector actions, and zod-validated Kakao response boundaries.
- [TSK-B-006] **ROLLOUT-BLOCKED** — add PostgreSQL crash/multi-node/privacy integration coverage and live-provider fixtures before activation.

### D — Scheduled Discord + Kakao fan-out workstream

- [TSK-D-001] **LOCAL-IMPLEMENTED** — add optional push-only `providerTargets.kakaoFriendShare` create/PATCH contract with explicit confirmation and shared Kakao validation.
- [TSK-D-002] **LOCAL-IMPLEMENTED** — encrypt active scheduled targets and per-fire payload snapshots with UUID-bound AAD; expose count-only summaries.
- [TSK-D-003] **LOCAL-IMPLEMENTED** — atomically enqueue the existing Discord `message_outbox` row and provider-neutral `external_share_outbox` row under the existing parent/delivery locks.
- [TSK-D-004] **LOCAL-IMPLEMENTED** — add leader-only lease/CAS external outbox worker, bounded safe pre-dispatch retries, stable outbox-derived Kakao idempotency, and PII-free status projection.
- [TSK-D-005] **LOCAL-VERIFIED** — focused PostgreSQL coverage proves dual handoff, cancellation fencing, fire-slot dedupe, stale lease reclaim, list redaction, and terminal ciphertext scrubbing.

### Merge and rollout boundaries

- PR merge requires formatting, focused Rust tests, dashboard build, migration checksum, and generated inventory checks to pass.
- The feature remains disabled by default and missing optional configuration cannot block AgentDesk core startup.
- Rollout additionally requires G0-002, G0-003, G0-005, TSK-B-006, and a real external HTTPS landing URL.
- One operation must never produce a second provider POST automatically; a sticky `unknown` is safer than duplicate delivery.
- No Kakao/external-share/oauth module may import or write Discord outbox, outbound, delivery-journal, or turn-bridge surfaces. The higher-level scheduled-message orchestrator owns the two sibling outbox handoffs.

### Pilot and conditional PR-C

- [TSK-PILOT-001] Observe 14-day aggregate metrics from operation rows without adding an audit pipeline.
- [TSK-PILOT-002] Record promote/hold/stop decision against PRD thresholds.
- [TSK-C-001] Only after promotion, add one existing result action entrypoint.
- [TSK-C-002] If result-specific external access is required, stop and write a separate public-share-artifact PRD before code.
- [TSK-C-003] Only after a second real channel exists, evaluate and extract a common interface.

---

## 18. Test Registry

### Gate/manual evidence

- [TEST-001] Official evidence rows contain direct URL, section, date, fixture, result, and limitation.
- [TEST-002] 3–4 app members complete connect → friends → send; no secret/UUID evidence retained.
- [TEST-003] Product owner confirms recurring need and security owner approves landing page.

### Vault and OAuth

- [TEST-004] Strict base64/key length/nonce length validation; invalid inputs fail closed.
- [TEST-005] XChaCha round trip and unique nonce per seal.
- [TEST-006] Ciphertext tamper, wrong key, AAD provider/account/state mismatch all fail.
- [TEST-007] DB/config/debug/JSON/log captures contain no access/refresh token plaintext; no PKCE material exists in v1.
- [TEST-008] State stores SHA-256 only, expires at 10 minutes, and is consumed once under race.
- [TEST-009] Callback works without Bearer even when server auth token is set; other routes reject missing Bearer.
- [TEST-010] Callback always returns the exact safe 303 location; no open redirect input exists.
- [TEST-011] Local disconnect deletes account and sessions and reports `remote_unlinked=false`.
- [TEST-012] Two nodes refreshing one account perform one provider refresh and fenced token update.
- [TEST-013] Ambiguous refresh/expired lease sets reauth_required without a second refresh request.
- [TEST-014] Typed Kakao token fixtures capture `refresh_token_expires_in`; omitted refresh-token/expiry fields preserve prior values only when the verified contract permits.

### Connector/config

- [TEST-015] Existing filesystem connectors preserve states, summary, source, and setup actions after DTO extension.
- [TEST-016] Kakao connector mapping covers disabled/missing/disconnected/consent/reauth/connected.
- [TEST-017] Connector JSON preserves legacy `env_var` while adding `env_vars`, optional source, safe connection metadata, and correct actions; no secrets.
- [TEST-018] Generic connector fields remain provider-neutral and the single Kakao action adapter is confined to the connector composition seam.
- [TEST-019] Integration config is restart-required, serializes no secrets, and missing optional config does not block core startup.

### External operation/send

- [TEST-020] Pure validation/connection/refresh/rate failure creates no operation and makes no message POST.
- [TEST-021] Two nodes with one new key create one dispatching row and make at most one message POST.
- [TEST-022] Active dispatching duplicate returns 409 without a second POST.
- [TEST-023] Same key/different payload returns 422 without a second POST.
- [TEST-024] Terminal success/partial/failed/unknown replay returns `replayed=true` without a second POST.
- [TEST-025] Crash after fence but before POST becomes unknown and never reclaims/reposts.
- [TEST-026] Crash after POST but before terminal write becomes unknown and never reposts.
- [TEST-027] Terminal DB write failure returns/settles unknown and never reposts.
- [TEST-028] Timeout, reset, truncated/unparseable body, and ambiguous 5xx map to unknown.
- [TEST-029] Only fixture-proven no-side-effect provider rejection maps to failed.
- [TEST-030] Full and partial provider fixtures map counts/indexes/internal codes correctly.
- [TEST-031] confirmed=false, duplicate/0/6 recipients, empty/201-char text, and client link/template fields are rejected before fence.
- [TEST-032] OAuth/account/external-operation/log/metrics/AppError/browser persistence fixtures contain no raw UUID, nickname, token, or provider body; manual-send text is not persisted. Scheduled fan-out retains only the pre-existing authoritative `scheduled_messages.content`, while provider-specific target/payload copies remain ciphertext.
- [TEST-033] DB-backed hourly cap is not exceeded under concurrent nodes; replay does not consume a new slot.
- [TEST-034] No Kakao/external_share source imports or writes Discord outbox/outbound/delivery journal paths.

### Wire/UI/CI

- [TEST-035] Snapshot exact connector, start, disconnect, friends, send, replay, and AppError schemas/statuses.
- [TEST-036] Inline composer enforces max 5, Unicode-scalar character count, confirm gate, one key per intent, identical-payload key reuse, zero POST retries, and disabled double-submit.
- [TEST-037] Unknown/partial UI has no automatic retry, safely rechecks an identical payload with the same key, and requires duplicate-risk confirmation before a changed-payload/new-key send.
- [TEST-038] Inventory generator and git-diff gate pass in every route-changing PR.
- [TEST-039] Migration immutable checksum validation passes in every migration-changing PR.
- [TEST-040] Pilot aggregate derives counts without raw recipient/text fields or a new audit table.
- [TEST-041] Dashboard API tests accept valid OAuth/disconnect/friends/send payloads and reject malformed URLs, remote-unlink claims, friend pages, operation IDs, counts, statuses, and retry flags before UI/cache use.
- [TEST-042] Scheduled create/PATCH rejects agent/Kakao combinations, missing confirmation, invalid recipient sets, content outside 1..=200, and content/timing changes that retain a Kakao target without renewed confirmation before persistence.
- [TEST-043] Active scheduled provider targets are ciphertext-only in full rows, omitted from list-row memory, and represented by count-only API summaries.
- [TEST-044] One fire transaction creates exactly one Discord outbox and one external outbox; cancellation, a stale claim, or either outbox insert failure commits neither.
- [TEST-045] Reprocessing a fire slot or stale external outbox lease never creates another obligation or provider POST; the stable outbox-derived key replays the operation fence.
- [TEST-046] Terminal scheduled definitions and external outbox rows retain safe summaries while plan/payload ciphertext, nonce, and key version are cleared.
- [TEST-047] Delivery responses expose independent Discord and provider states without UUID, nickname, text, token, or raw provider body.

---

## 19. Traceability

| Requirement | Tasks | Verification |
|---|---|---|
| REQ-001 | TSK-B-005 | TEST-036 |
| REQ-002 | TSK-G0-001, TSK-G0-002, TSK-G0-003, TSK-G0-005, TSK-G0-006 | TEST-001, TEST-002, TEST-003; EVIDENCE-G0-001, EVIDENCE-G0-002, EVIDENCE-G0-003, EVIDENCE-G0-004, EVIDENCE-G0-005, EVIDENCE-G0-006, EVIDENCE-G0-007, EVIDENCE-G0-008, EVIDENCE-G0-009, EVIDENCE-G0-010 |
| REQ-003 | TSK-B-003, TSK-B-005, TSK-D-001 | TEST-024, TEST-034, TEST-036, TEST-042 |
| REQ-004 | TSK-B-003, TSK-B-006, TSK-D-003 | TEST-034, TEST-044 |
| REQ-005 | TSK-B-005, TSK-C-002 | TEST-035, TEST-036 |
| REQ-006 | TSK-B-003, TSK-C-003 | TEST-034 |
| REQ-007 | TSK-B-001, TSK-B-003 | TEST-021, TEST-025, TEST-026, TEST-027 |
| REQ-008 | TSK-B-001, TSK-B-006, TSK-D-003 | TEST-025, TEST-026, TEST-034, TEST-044 |
| REQ-009 | TSK-B-001, TSK-B-003, TSK-B-005 | TEST-022, TEST-024, TEST-025, TEST-026, TEST-027, TEST-037 |
| REQ-010 | TSK-B-001, TSK-B-004 | TEST-021, TEST-022, TEST-023, TEST-024 |
| REQ-011 | TSK-B-002, TSK-B-003 | TEST-024, TEST-028, TEST-029, TEST-030 |
| REQ-012 | TSK-A-002, TSK-A-003 | TEST-004, TEST-005, TEST-006, TEST-007 |
| REQ-013 | TSK-A-003, TSK-A-004 | TEST-008, TEST-009, TEST-010 |
| REQ-014 | TSK-A-003, TSK-A-004 | TEST-012, TEST-013, TEST-014 |
| REQ-015 | TSK-A-003, TSK-A-004 | TEST-011 |
| REQ-016 | TSK-A-004, TSK-B-004 | TEST-009, TEST-035 |
| REQ-017 | TSK-A-004, TSK-A-005, TSK-B-003 | TEST-016, TEST-017 |
| REQ-018 | TSK-A-005 | TEST-015, TEST-017, TEST-018 |
| REQ-019 | TSK-A-001 | TEST-019 |
| REQ-020 | TSK-A-004, TSK-B-004 | TEST-035 |
| REQ-021 | TSK-A-002, TSK-B-002, TSK-B-003, TSK-B-005, TSK-D-002, TSK-D-004 | TEST-007, TEST-017, TEST-032, TEST-043, TEST-046, TEST-047 |
| REQ-022 | TSK-B-003, TSK-B-004, TSK-B-005 | TEST-031, TEST-036 |
| REQ-023 | TSK-A-001, TSK-B-002, TSK-B-005 | TEST-031, TEST-035, TEST-036 |
| REQ-024 | TSK-B-001, TSK-B-003 | TEST-021, TEST-033 |
| REQ-025 | TSK-G0-001, TSK-G0-003, TSK-G0-004, TSK-G0-006 | TEST-001, TEST-002, TEST-014, TEST-028, TEST-029, TEST-030; EVIDENCE-G0-001, EVIDENCE-G0-002, EVIDENCE-G0-003, EVIDENCE-G0-004, EVIDENCE-G0-005, EVIDENCE-G0-006, EVIDENCE-G0-007, EVIDENCE-G0-008, EVIDENCE-G0-009 |
| REQ-026 | TSK-A-001, TSK-A-003, TSK-A-006, TSK-B-001, TSK-B-006 | TEST-038, TEST-039 |
| REQ-027 | TSK-A-004, TSK-A-005, TSK-B-004 | TEST-035 |
| REQ-028 | TSK-B-006 | TEST-021, TEST-025, TEST-026, TEST-027 |
| REQ-029 | TSK-A-001, TSK-A-005, TSK-B-003 | TEST-016, TEST-019, TEST-020 |
| REQ-030 | TSK-PILOT-001, TSK-PILOT-002, TSK-C-001, TSK-C-002 | TEST-003, TEST-040 |
| REQ-031 | TSK-B-005 | TEST-035, TEST-041 |
| REQ-032 | TSK-D-001, TSK-D-003 | TEST-042, TEST-044 |
| REQ-033 | TSK-D-004 | TEST-045 |
| REQ-034 | TSK-D-002, TSK-D-004 | TEST-043, TEST-046 |
| REQ-035 | TSK-D-003, TSK-D-004 | TEST-044, TEST-047 |

All IDs are written in full to support mechanical validation. No test or evidence row may be silently replaced by prose such as “CI docs”.

---

## 20. Verification Commands

Minimum implementation checks are:

```text
cargo fmt --all --check
cargo check --lib
cargo test --lib services::oauth_connection::tests
cargo test --lib services::external_share::tests
cargo test --lib services::kakao::tests
cargo test --lib server::routes::kakao::tests
cargo test --lib config::kakao_friend_share_config_tests
cd dashboard && npm run build
cd dashboard && npm test
python scripts/check_postgres_migration_checksums.py
python scripts/check_api_docs_coverage.py
python scripts/generate_inventory_docs.py --check
python scripts/audit_maintainability.py --check
python scripts/check_test_target_integrity.py --enforce
```

Additional focused suites MUST cover:

- vault unit tests
- OAuth mock server tests
- connector backend/dashboard tests
- operation Postgres concurrency tests
- provider response fixtures
- privacy/log capture tests
- migration immutable checksum test

Passing focused tests is not reported as full-project health unless the full suite was actually run.

---

## 21. Cohesion, Reuse, Extensibility, and ROI Acceptance

| Axis | Acceptance condition | Rejected shortcut |
|---|---|---|
| Cohesion | provider protocol, OAuth persistence, vault, operation state, UI routing each have one owner and one-way dependencies | god module or route-owned business SQL |
| Reuse | reuse only contracts with matching semantics: routes, AppError, connector surface, redaction, HTTP stack, fingerprint helper | idempotency TTL or Discord journal reuse for a non-replay-safe POST |
| Extensibility | provider DTOs stay isolated, connector DTO gains typed kind/actions, DB keys include provider/channel/account | premature channel trait, generic template DSL, ID branches throughout UI |
| ROI | official wire evidence precedes code; default-disabled Settings pilot and rollout gates precede product entrypoints; operation rows provide aggregate evidence | full public-share system, new analytics pipeline, or global UI before recurring demand |
| Safety | durable at-most-once fence, sticky unknown, DB refresh lease, fixed crypto contract | automatic resend, process-local correctness, ambiguous 5xx as failed |

This document describes an implemented local slice, not an activated integration. The merge gate and rollout gate are deliberately separate: local code can be reviewed while missing Kakao-console, live-account, landing-page, and demand evidence keeps the feature disabled.

---

## 22. Implementation Readiness

| Area | Status | Blocker |
|---|---|---|
| Product promise and v1 scope | Ready | none |
| AgentDesk local architecture evidence | Ready | none |
| At-most-once send contract | Locally implemented; rollout blocked | PostgreSQL crash/multi-node tests and live ambiguity evidence |
| OAuth/scope/token contract | Official wire verified; locally implemented | Kakao console and live E2E; PKCE intentionally excluded because no official contract was found |
| Friends/message provider DTO | Official wire verified; locally implemented | sanitized live-account fixtures |
| Connector/status architecture | Locally implemented with focused safety-helper and response-schema tests | rendered component interaction coverage before rollout |
| Vault and DB invariants | Locally implemented | PostgreSQL integration/race coverage; dependency versions are lockfile-pinned |
| ROI | Blocked | EVIDENCE-G0-010 and Gate 0 demand confirmation |
| Code | **Local verification complete** | default-disabled until all rollout gates pass |
| Production activation | **Blocked** | console eligibility, live E2E, approved HTTPS landing URL, demand, and rollout test suite |

---

## 23. Change from Previous Draft

| Previous draft | Current contract |
|---|---|
| “결과/카드 공유” | Settings text-only test send; fixed landing is not result sharing |
| `idempotency_keys` + 24h reclaim | durable non-reclaiming `external_share_operations` fence |
| crash 후 TTL 재POST 가능 | expired dispatch becomes sticky unknown; no re-POST |
| Discord delivery concepts 미조사 | `0105_delivery_journal` explicitly isolated |
| connector vec에 row만 추가 | async DB-aware canonical projection + backward-compatible kind/env list/connection/actions |
| `/api/kakao/status`와 connector status 병렬 | connector endpoint is the only UI status surface |
| Kakao ID-specific row actions | generic row + one typed action adapter |
| `ExternalShareChannel` with one impl | concrete service; extraction deferred until second channel |
| token columns NOT NULL + disabled wipe | account row delete on local disconnect; nullable expiries |
| XChaCha or ChaCha | XChaCha20-Poly1305, nonce/AAD/key contract fixed |
| process-local refresh/rate accepted | DB lease and DB-backed send cap |
| HTTP `409 or 400`, `429 or 400` | exact status/ErrorCode table |
| provider claims “high confidence” without direct links | blocking Evidence Ledger with direct official sources/fixtures |
| route inventory in P2 | inventory/checksum/taxonomy in every changing PR |
| layer-oriented PR split | one default-disabled connection + manual-share vertical slice with stable workstream IDs |
| undocumented PKCE assumption | no PKCE in v1 unless Kakao publishes a compatible contract |

---

## Change Log

| Date | Note |
|---|---|
| 2026-08-09 | Initial AgentDesk reuse survey and wire draft |
| 2026-08-09 | Detailed OAuth/friends/send/idempotency/connectors draft |
| 2026-08-09 | **Cohesion/ROI/safety rewrite**: added provider evidence and rollout gates; narrowed v1 to a Settings text-only pilot; replaced replay-safe idempotency reuse with a non-reclaiming external operation fence; isolated Discord `0105_delivery_journal`; made connector projection DB-aware; fixed crypto, refresh, disconnect, HTTP, privacy, cluster, test, and traceability contracts. |
| 2026-08-09 | **Implementation synchronization**: aligned the Spec with the default-disabled OAuth/vault/friends/send slice, oauth2-rs 5.0.0, Kakao's comma-delimited scopes and refresh-token omission, bounded provider responses, the inline composer, one cohesive PR, and separate merge versus rollout gates. |
| 2026-08-09 | **Ready-for-review hardening**: bound every new Kakao Dashboard response to a zod parser, inferred TypeScript types from the schemas, and added valid/invalid boundary tests before moving the PR out of Draft. |
| 2026-08-09 | **Scheduled fan-out follow-up**: permitted one explicit automatic path—confirmed push reservations—and specified encrypted provider targets, atomic Discord/external outbox handoff, stable outbox-derived idempotency, provider-local retry/status, and terminal ciphertext scrubbing without introducing a Kakao-to-Discord dependency. |
