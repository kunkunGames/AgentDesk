# 에이전트별 Discord 기본 실행 노드

leader/worker는 AgentDesk **노드의 운영 역할**이다. Discord 에이전트는 기존 역할과
채널 연결을 유지하면서 새 세션을 시작할 기본 장비를 선택할 수 있다. 여러 worker를
구분하기 위해 `leader` 또는 `worker`라는 문자열 대신 안정적인 `instance_id`를 저장한다.

대시보드의 에이전트 상세 → **Discord 기본 실행 노드**에서 등록된 노드를 선택하고
저장한다. 목록은 hostname, leader/worker 역할, OS를 함께 보여준다. 등록 정보가
없어진 기본 노드도 자동으로 다른 장비로 바꾸지 않으며 명시적으로 해제할 수 있다.

중앙 PostgreSQL `agents.default_execution_node_id`가 기본값의 정본이다.
worker의 로컬 YAML reseed는 이 값을 쓰지 않는다. API는 full profile에서만 제공한다.

```http
GET /api/agents/{id}/execution-node
PUT /api/agents/{id}/execution-node
Content-Type: application/json

{"default_node_id":"windows-worker-1"}
```

`{"default_node_id":null}`은 기본값을 해제하고 기존 배정 정책을 사용한다. 필수
OS·도구·저장소 조건은 별도의 `execution-requirements` API에 유지한다. 기본 노드를
변경해도 필수 조건을 삭제하거나 기존 세션을 강제 종료하지 않는다.

배정 우선순위는 다음과 같다.

1. 기존 live session owner.
2. 현재 채널/스레드의 명시적 `/node` 선택.
3. 에이전트의 기본 실행 노드.
4. 선호 label과 활성화된 자동 용량 배정 정책.

직접 에이전트에 연결된 채널은 자신의 설정을 사용한다. 직접 연결이 없는 Discord
스레드는 실제 Discord metadata에서 확인한 부모 채널의 정책을 사용한다. 일반 채널의
category를 부모 에이전트로 취급하지 않으며, 기존 `threadInherit: false`는 유지한다.
부모 정책을 확인하지 못하면 정책이 없는 것으로 간주해 실행하지 않는다.
소유권·중복 방지·응답 대상은 항상 실제 스레드 ID를 사용한다.

기본 노드는 새 세션 배정에 적용된다. Mac이 소유한 기존 대화에서 기본값을 Windows로
바꿔도 다음 입력은 Mac에서 처리한다. 새 스레드/새 세션부터 Windows를 선택한다.
살아 있는 터미널, provider session 파일, 로그인 또는 작업 폴더를 OS 사이에 복제하는
기능은 아니다. worker에 해당 agent의 prompt·bot 설정·CLI 계정과 작업 경로를 준비해야 한다.

지정 노드가 오프라인이거나 준비되지 않았거나 필수 조건을 만족하지 못하면 사유를
표시하고 새 요청의 실행을 거절한다. 무기한 대기 예약을 만들거나 다른 OS로 자동
대체하지 않는다. 원래 지정 노드가 복구된 뒤 새 요청을 보낼 수 있다. 이미 저장한
요청의 실행 전 재시도에도 지정 노드를 보존하도록 outbox의 조건에 노드를 기록한다.
이 snapshot은 중앙의 필수 실행 조건과 별개다.

노드 선택 저장과 실행에는 `intake_routing.mode=enforce`가 필요하다. 이후 routing을
끄더라도 저장된 기본 노드나 필수 조건을 무시하고 다른 장비에서 실행하지 않는다.
전역 자동 용량 배정이 꺼져 있어도 명시적인 에이전트 기본 노드는 사용할 수 있다.

이 설정은 Discord intake의 기본 배정이다. 별도 dispatch 작업의
`required_capabilities.execution`이나 이미 배정된 dispatch의 owner를 바꾸지 않는다.
실제 설치·검증 범위는 [배포 보고서](../reports/heterogeneous-worker-rollout-2026-09-22.md)를 따른다.
