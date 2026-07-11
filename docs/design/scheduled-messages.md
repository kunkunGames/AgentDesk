# Scheduled Messages (예약 메시지 풀) — DB & API 설계

## 목표

에이전트(또는 사람/시스템)가 **예약 메시지 풀**에 메시지를 저장해 두면, 지정한
날짜·시간에:

- **push 모드**: 에이전트 개입 없이 시스템이 곧바로 Discord 채널로 전송
- **agent 모드**: 지정된 에이전트가 예약 메시지를 읽고(턴 주입) 자신의 판단으로
  가공·요약해 대상 Discord 채널로 전송

두 경로 모두 최종 Discord 전송은 기존 `message_outbox` → `message_outbox_loop`
경로를 재사용한다. 이 설계는 새 전송 파이프라인을 만들지 않는다 — "언제/누가
보낼지"의 스케줄링 계층만 추가한다.

## 기존 컴포넌트와의 관계

| 기존 컴포넌트 | 역할 재사용 |
|---|---|
| `message_outbox` (0001, 0042, 0066) | 최종 Discord 전송 큐. push 모드 발화 시 여기로 enqueue. **outbox drain claim이 이미 `next_attempt_at <= NOW()`를 게이트하므로(`server/mod.rs` claim_pending_message_outbox_batch_pg) 지연 전송·재시도·claim을 outbox가 온전히 소유** — handoff 이후 스케줄러는 관여하지 않는다 |
| `RoutineAgentExecutor` (`services/routines/agent_executor.rs`) | agent 모드 발화 실행기. `start_agent_run`(턴 시작) + `poll_agent_runs`(완료 폴링)를 직접 호출 — "예정 시각에 에이전트 턴 시작 후 완료 감시" 문제를 routines가 이미 해결함 |
| `routines` / `routine_runs` (0035) | 스키마 패턴 차용: 정의 row + 실행 이력 row 분리, `next_due_at` partial index due-scan, lease 기반 중복 실행 방지, `schedule` 파서 재사용 |
| `worker_registry` + `message_outbox_loop` 패턴 | `scheduled_message_loop` 워커를 기존 등록 패턴으로 추가. adaptive backoff(500ms–5s) 폴링 패턴 동일 적용 |
| `agents.discord_channel_id` | `target_channel_id` 미지정 시 agent 모드의 기본 대상 채널 |

### 검토했으나 채택하지 않은 대안 (재사용 극대화 관점)

- **outbox 단독 설계 (0-테이블)**: push 1회성 예약은 `INSERT INTO message_outbox
  (..., next_attempt_at = scheduled_at)` 하나로 이미 동작한다. 그러나 outbox는
  발송 대기 큐이지 예약 풀이 아니다 — 수정/취소/목록 API, 반복, agent 모드,
  작성자·이력이 전부 없다. 요구사항에 agent 모드가 명시돼 있어 정의 테이블은
  불가피하다. 대신 이 native 지연 능력을 push 모드 handoff 단순화에 활용한다
  (아래 상태 기계 참조).
- **routine으로 표현**: agent 모드 예약은 개념상 1회성 routine이지만,
  `routines.script_ref NOT NULL`(스크립트 중심 실행 모델)에 메시지 원문
  보관·수정 UX를 우겨넣으면 두 도메인이 모두 오염된다. 실행기만 재사용하고
  데이터 모델은 분리한다.

recurring 자동화 전반은 `routines`의 영역이다. 이 테이블은 "이 내용을 이 시각에
이 채널로"라는 **메시지 중심** 예약에 특화하되, `routines.schedule`과 같은
표현(`@every` duration 또는 5-field cron)으로 선택적 반복을 지원한다.

## DB 설계

Postgres 전용 (messages 라우트와 동일하게 pg pool 필수). 마이그레이션:
`migrations/postgres/0082_scheduled_messages.sql` (`0079`~`0081`은 최신 upstream 계열이 선점).

### `scheduled_messages` — 예약 정의(풀)

```sql
CREATE TABLE IF NOT EXISTS scheduled_messages (
    id                 TEXT PRIMARY KEY,            -- 'smsg_' + uuid
    -- 내용
    content            TEXT NOT NULL,
    title              TEXT,                        -- 목록/로그 표시용 (선택)
    -- 대상
    target_channel_id  TEXT,                        -- Discord 채널 ID.
                                                    -- agent 모드에서 NULL이면 agents.discord_channel_id 사용.
                                                    -- push 모드에서는 NOT NULL (앱 레벨 검증)
    bot                TEXT NOT NULL DEFAULT 'announce',  -- message_outbox.bot과 동일 도메인
    -- 전달 방식
    delivery_kind      TEXT NOT NULL DEFAULT 'push',      -- 'push' | 'agent'
    agent_id           TEXT REFERENCES agents(id),        -- delivery_kind='agent'일 때 필수
    agent_instruction  TEXT,                        -- agent 모드에서 메시지와 함께 주입할 지시문 (선택)
    on_agent_failure   TEXT NOT NULL DEFAULT 'fail',      -- 'fail' | 'push_raw'
                                                    -- push_raw: 에이전트 전달이 최종 실패하면 원문을 push 모드로 강등 전송
    -- 스케줄
    scheduled_at       TIMESTAMPTZ NOT NULL,        -- 다음(또는 유일한) 발화 시각
    schedule           TEXT,                        -- 반복 규칙. NULL=1회성.
                                                    -- routines.schedule과 동일 문법 (@every 10m | 5-field cron)
    timezone           TEXT NOT NULL DEFAULT 'Asia/Seoul', -- cron 해석용
    expires_at         TIMESTAMPTZ,                 -- 반복 예약 종료 시각 (선택)
    -- 상태
    -- 'scheduled' | 'firing' | 'sent' | 'failed' | 'canceled' | 'expired'
    -- 반복 예약은 발화 후 다시 'scheduled'로 돌아간다 (터미널: sent/failed/canceled/expired)
    status             TEXT NOT NULL DEFAULT 'scheduled',
    in_flight_delivery_id TEXT,                     -- firing 중 delivery row ID (routines.in_flight_run_id 패턴)
    fire_count         INTEGER NOT NULL DEFAULT 0,
    last_fired_at      TIMESTAMPTZ,
    last_error         TEXT,
    -- 출처
    source             TEXT NOT NULL DEFAULT 'api', -- 'api' | 'agent' | 'discord' | 'system'
    created_by         TEXT,                        -- 생성 주체 (agent id 또는 사용자 식별자)
    -- 메타
    dedupe_key         TEXT,                        -- 생성 시점 중복 방지 (선택, 활성 상태에서 unique)
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT chk_smsg_delivery_kind CHECK (delivery_kind IN ('push', 'agent')),
    CONSTRAINT chk_smsg_status CHECK (status IN
        ('scheduled', 'firing', 'sent', 'failed', 'canceled', 'expired')),
    CONSTRAINT chk_smsg_agent_required CHECK
        (delivery_kind <> 'agent' OR agent_id IS NOT NULL)
);

-- due-scan 전용 partial index (routines idx_routines_due_scan 패턴)
CREATE INDEX IF NOT EXISTS idx_scheduled_messages_due_scan
    ON scheduled_messages(scheduled_at)
    WHERE status = 'scheduled';

CREATE INDEX IF NOT EXISTS idx_scheduled_messages_agent
    ON scheduled_messages(agent_id, status)
    WHERE agent_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_scheduled_messages_channel
    ON scheduled_messages(target_channel_id)
    WHERE target_channel_id IS NOT NULL;

-- 활성(비터미널) 예약에 한해 dedupe_key 유일 (message_outbox uq 패턴)
CREATE UNIQUE INDEX IF NOT EXISTS uq_scheduled_messages_active_dedupe
    ON scheduled_messages(dedupe_key)
    WHERE dedupe_key IS NOT NULL
      AND status IN ('scheduled', 'firing');
```

### `scheduled_message_deliveries` — 발화 이력 (routine_runs 패턴)

반복 예약은 발화 1회당 1 row. 재시도는 row 내부 카운터로 추적한다.

```sql
CREATE TABLE IF NOT EXISTS scheduled_message_deliveries (
    id                  TEXT PRIMARY KEY,           -- 'smdel_' + uuid
    scheduled_message_id TEXT NOT NULL REFERENCES scheduled_messages(id),
    fire_scheduled_at   TIMESTAMPTZ NOT NULL,       -- 이 발화가 예정됐던 시각 (dedupe 축)
    delivery_kind       TEXT NOT NULL,              -- 발화 시점 스냅샷
    -- 'running' | 'sent' | 'failed' | 'interrupted'
    status              TEXT NOT NULL DEFAULT 'running',
    -- 다중 노드 안전: claim + lease (routine_runs.lease_expires_at 패턴)
    claim_owner         TEXT,
    lease_expires_at    TIMESTAMPTZ,
    -- push 경로 추적
    outbox_id           BIGINT,                     -- 생성된 message_outbox row
    -- agent 경로 추적
    turn_id             TEXT,                       -- 주입된 에이전트 턴
    fallback_outbox_id  BIGINT,                     -- on_agent_failure='push_raw' 강등 시
    retry_count         INTEGER NOT NULL DEFAULT 0,
    next_attempt_at     TIMESTAMPTZ,
    error               TEXT,
    started_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at         TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT chk_smdel_status CHECK (status IN
        ('running', 'sent', 'failed', 'interrupted')),
    -- 동일 발화 시각 중복 발화 방지 (at-most-once per fire slot)
    CONSTRAINT uq_smdel_fire_slot UNIQUE (scheduled_message_id, fire_scheduled_at)
);

CREATE INDEX IF NOT EXISTS idx_smdel_parent
    ON scheduled_message_deliveries(scheduled_message_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_smdel_running_lease
    ON scheduled_message_deliveries(lease_expires_at)
    WHERE status = 'running';

CREATE INDEX IF NOT EXISTS idx_smdel_retry_scan
    ON scheduled_message_deliveries(next_attempt_at)
    WHERE status = 'running' AND next_attempt_at IS NOT NULL;
```

## 상태 기계

```
scheduled ──(due, claim 성공)──▶ firing ──(전송 확인)──▶ sent        (1회성)
    ▲                              │       └(schedule 있음)─▶ scheduled (다음 fire 시각 재계산)
    │                              ├──(재시도 소진)──▶ failed
    └──(PATCH로 시각/내용 수정)      └──(expires_at 경과)─▶ expired
scheduled ──(DELETE)──▶ canceled
firing    ──(DELETE)──▶ canceled   (진행 중 delivery는 interrupted 마킹, outbox 이미 enqueue됐으면 취소 불가 — 응답에 명시)
```

- **push 모드의 "sent" 판정 = outbox handoff 성공**: `message_outbox` INSERT가
  성공하면 delivery는 즉시 터미널(`sent`, 의미상 "handed off"). 이후
  재시도·최종 실패는 outbox가 자체 소유하며(retry_count/next_attempt_at),
  스케줄러는 outbox 상태를 다시 폴링하지 않는다 — 감시 책임을 이중으로 두지
  않는다. 최종 전송 결과는 `deliveries.outbox_id` 조인으로 조회 가능
  (GET deliveries 응답에 outbox 상태를 lazy join으로 포함).
- **agent 모드의 "sent" 판정**: `RoutineAgentExecutor::poll_agent_runs` 패턴으로
  주입한 턴이 정상 종료하면 `sent`. 판정을 더 엄격히 하려면 에이전트에게 완료 시
  `POST /api/scheduled-messages/:id/deliveries/:deliveryId/ack`
  호출을 지시문에 포함 (v2 옵션, 초기 구현은 턴 종료 = sent).
- 따라서 **완료 감시(3단계)와 `in_flight_delivery_id`는 agent 모드에서만 의미**
  가 있다. push 전용 인스턴스라면 스케줄러는 claim→handoff→터미널로 끝난다.

## 스케줄러 워커 — `scheduled_message_loop`

`worker_registry`에 기존 `message_outbox_loop`와 동일 패턴으로 등록
(adaptive backoff 500ms–5s — 예약 메시지는 분 단위 정밀도면 충분하므로
idle 시 5s 상한까지 늘어나는 기존 백오프 로직을 그대로 사용).

```text
매 tick:
1. due-claim (다중 노드 안전):
   UPDATE scheduled_messages
      SET status = 'firing', in_flight_delivery_id = $new_id, updated_at = NOW()
    WHERE id IN (
        SELECT id FROM scheduled_messages
         WHERE status = 'scheduled' AND scheduled_at <= NOW()
         ORDER BY scheduled_at
         LIMIT $batch
         FOR UPDATE SKIP LOCKED)
    RETURNING *;
   → 각 row에 대해 delivery row INSERT (uq_smdel_fire_slot이 이중 발화 차단,
     claim_owner = node id, lease_expires_at = NOW() + 2m)

2. 발화:
   - expires_at 경과 → delivery 없이 status='expired'
   - push:  message_outbox INSERT
            (target=channel_id, content, bot, source='scheduled_message',
             dedupe_key='scheduled_message:v1:{id}:{fire_scheduled_at epoch}')
            → INSERT 성공 즉시 delivery=sent (터미널, "handed off").
              이후 재시도/실패는 outbox_loop 소유 — 여기서 다시 감시하지 않는다.
   - agent: RoutineAgentExecutor::start_agent_run으로 대상 에이전트 턴 시작.
            프롬프트 = agent_instruction + 예약 메시지 원문 + 대상 채널 ID
            + "전송 후 결과 보고" 지시

3. 완료 감시 (agent 모드 running delivery만 해당):
   - poll_agent_runs 패턴으로 turn 종료 확인 → sent /
     (실패 && on_agent_failure='push_raw' → 원문을 outbox로 강등 enqueue,
      fallback_outbox_id 기록 후 sent) / failed
   - lease 만료 && 미완료 → interrupted 마킹 후 부모를 scheduled로 복원
     (fire_scheduled_at 동일 슬롯은 uq 제약으로 재발화 안 됨 → 다음 tick에
      retry_count 증가시켜 새 delivery 재시도, 최대 3회 exponential backoff)

4. 발화 완료 후 부모 갱신:
   - schedule NULL → status='sent'|'failed'
   - schedule 있음 → 다음 fire 시각 계산(routines schedule 파서 재사용,
     timezone 적용) 후 scheduled_at 갱신, status='scheduled',
     fire_count += 1, in_flight_delivery_id = NULL
```

부팅 복구: `status='firing'` && delivery lease 만료 row를 `scheduled`로 복원
(routines 부팅 복구와 동일 원칙 — 만료된 lease만 건드린다).

## API 설계

베이스: `/api/scheduled-messages`. 응답/요청 JSON은 camelCase
(`routes/messages.rs` 컨벤션). pg pool 없으면 503 (기존과 동일).
새 파일 `src/server/routes/scheduled_messages.rs`, DB 계층 `src/db/scheduled_messages.rs`.

### POST `/api/scheduled-messages` — 예약 생성

```jsonc
// Request
{
  "content": "내일 오전 스탠드업 안건: ...",         // 필수
  "title": "스탠드업 리마인더",                      // 선택
  "targetChannelId": "1492021444308238487",        // push면 필수, agent면 선택
  "bot": "announce",                               // 선택, 기본 announce
  "deliveryKind": "agent",                         // 'push' | 'agent', 기본 push
  "agentId": "coder",                              // agent 모드 필수
  "agentInstruction": "핵심만 3줄로 요약해서 보내줘", // 선택
  "onAgentFailure": "push_raw",                    // 선택, 기본 fail
  "scheduledAt": "2026-07-08T09:00:00+09:00",      // 필수, ISO 8601
  "schedule": "0 9 * * 1-5",                       // 선택 (반복), @every 24h 도 가능
  "timezone": "Asia/Seoul",                        // 선택, 기본 Asia/Seoul
  "expiresAt": "2026-08-01T00:00:00+09:00",        // 선택
  "source": "agent",                               // 선택
  "createdBy": "planner",                          // 선택
  "dedupeKey": "standup-reminder-w28"              // 선택
}
// 201 Response
{ "scheduledMessage": { "id": "smsg_...", "status": "scheduled", ... } }
```

검증 (400):
- `scheduledAt`이 과거(허용 오차 60s 초과) && `schedule` 없음 → 거부
  (`schedule` 있으면 다음 cron 시각으로 자동 보정 후 응답에 보정값 반환)
- `deliveryKind='agent'` && `agentId` 없음/미존재 → 거부
- `deliveryKind='push'` && `targetChannelId` 없음 → 거부
- `deliveryKind='agent'` && `targetChannelId` 없음 && 해당 agent의
  `discord_channel_id` 없음 → 거부
- 활성 `dedupeKey` 충돌 → 409 + 기존 row 반환 (idempotent create)

### GET `/api/scheduled-messages` — 목록

쿼리: `status`, `deliveryKind`, `agentId`, `targetChannelId`, `dueBefore`,
`dueAfter`, `limit`(기본 50, 최대 200), `before`(created_at 커서 — messages
라우트와 동일 페이지네이션).

```jsonc
{ "scheduledMessages": [ ... ], "nextCursor": "2026-07-07T14:00:00Z" }
```

### GET `/api/scheduled-messages/:id`

정의 + 최근 delivery 5건 포함.

```jsonc
{ "scheduledMessage": { ... }, "recentDeliveries": [ ... ] }
```

### PATCH `/api/scheduled-messages/:id` — 수정

`status IN ('scheduled')`일 때만 허용 (firing/터미널이면 409).
수정 가능 필드: `content`, `title`, `targetChannelId`, `bot`, `agentId`,
`agentInstruction`, `onAgentFailure`, `scheduledAt`, `schedule`, `timezone`,
`expiresAt`. 검증은 POST와 동일.

### DELETE `/api/scheduled-messages/:id` — 취소

- `scheduled` → `canceled` (200)
- `firing` → `canceled` + 진행 delivery `interrupted` 마킹. 단, outbox에 이미
  enqueue된 뒤면 전송 자체는 막지 못함 → `{"canceled": true, "note": "delivery already handed off to outbox"}`
- 터미널 상태 → 409

### POST `/api/scheduled-messages/:id/trigger-now` — 즉시 발화 (테스트/수동)

`scheduled` 상태에서만. `fire_scheduled_at = NOW()`로 delivery를 즉시 생성하고
발화 경로를 태운다. 반복 예약이면 원래 `scheduledAt`은 유지된다.

```jsonc
{ "delivery": { "id": "smdel_...", "status": "running" } }
```

### GET `/api/scheduled-messages/:id/deliveries` — 발화 이력

쿼리: `limit`(기본 20), `before` 커서.

### 에이전트가 예약을 넣는 경로

에이전트는 위와 동일한 HTTP API를 사용한다 (`source: "agent"`,
`createdBy: <agent id>`). 별도 내부 API를 만들지 않는다 — 기존 스킬/도구에서
`POST /api/scheduled-messages` 하나로 충분하다.

## 결정 사항 요약

1. **outbox 재사용 + 감시 책임 단일화**: Discord 전송·재시도·dedupe·지연은
   `message_outbox`가 이미 해결한 문제(claim 쿼리가 `next_attempt_at <= NOW()`
   게이트). 예약 계층은 발화 시점에 outbox로 handoff하고 즉시 손을 뗀다 —
   같은 전송을 두 워커가 감시하는 이중 책임을 만들지 않는다.
2. **정의/이력 분리 + lease claim**: `routines`/`routine_runs`에서 검증된 패턴을
   그대로 차용해 다중 노드 이중 발화를 DB 제약(`uq_smdel_fire_slot` +
   `FOR UPDATE SKIP LOCKED`)으로 차단. agent 모드 실행은
   `RoutineAgentExecutor`(start_agent_run/poll_agent_runs)를 직접 재사용 —
   신규 에이전트 실행 경로를 만들지 않는다.
3. **반복은 선택 기능**: 1회성 예약이 1급 시민. 반복 문법은 `routines.schedule`
   파서를 재사용해 새 문법을 도입하지 않는다.
4. **agent 모드 실패 강등 옵션**(`on_agent_failure='push_raw'`): "반드시 나가야
   하는 공지"와 "에이전트 가공이 의미인 메시지"를 예약 단위로 구분할 수 있게 함.
5. **Postgres 전용**: messages 라우트와 동일하게 pg pool 필수, sqlite 호환
   마이그레이션은 만들지 않는다.

## 구현 파일 맵 (예상)

| 파일 | 내용 |
|---|---|
| `migrations/postgres/0082_scheduled_messages.sql` | 위 스키마 (+ immutable-checksums.json 갱신) |
| `src/db/scheduled_messages.rs` | CRUD + due-claim + delivery 상태 전이 쿼리 |
| `src/server/routes/scheduled_messages.rs` | 위 7개 핸들러 |
| `src/server/routes/mod.rs`, `domains/ops.rs` | 라우트 등록 (protected ops 도메인) |
| `src/server/routes/docs/inventory/endpoints/part_09.rs` | API docs 인벤토리 항목 (coverage 가드 필수) |
| `src/services/scheduled_messages.rs` | `scheduled_message_loop` 워커 (fire/감시/복구) |
| `src/server/worker_registry.rs` | 워커 등록 항목 추가 (`ScheduledMessages`, leader-only) |
| `src/services/routines/store.rs` | `next_due_after`를 `pub(crate)`로 공개 (스케줄 문법 재사용) |

구현 노트 (설계와의 차이):
- agent 모드 실행은 `RoutineAgentExecutor::start_agent_run` 직접 호출 대신 그 아래
  프리미티브인 `start_reserved_headless_agent_turn_with_owner_channel`을 사용한다 —
  executor는 `RoutineStore`/`ClaimedRoutineRun`에 강결합돼 있어 가짜 routine row가
  필요해진다. 완료 증거 쿼리(session_transcripts/agent_quality_event)는 동일 모델.
- agent 턴은 대상 채널에서 직접 시작한다: 릴레이된 assistant 응답 자체가 전달된
  메시지이고, non-empty assistant transcript가 곧 전달 증거다.
- push_raw 강등 시 대상 채널이 없으면 `agent:<id>` outbox 타깃(에이전트 기본 채널)
  으로 보낸다.
