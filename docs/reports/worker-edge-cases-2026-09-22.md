# Worker 배치·대기열 추가 엣지케이스 검증

검증일: 2026-09-22. 대상은 Mac mini leader와 Windows worker이며, PostgreSQL 자동 테스트는 운영 DB와 분리된 테스트 서버를 사용한다.

## 실제 발견한 문제

직전 배포 `3befd2079`에서 Windows가 첫 요청을 실행하는 동안 후속 Discord 메시지를 보냈다. 첫 요청은 답변했지만 후속 요청은 대기열에 남았다. 실행·마무리 작업 수는 모두 0, 대기열은 1이었다. 중앙 `intake_outbox`의 해당 행은 `done`이며 시도 횟수는 1이었다. 이 DB 상태는 worker가 메시지를 인수했다는 증거이며, 실제 답변 완료를 보장하지 않는다.

재현은 전용 테스트 스레드 `1551857716555948033`에서 수행했다. 정체된 메시지는 `1551858427817361482`, outbox 행은 22다. 첫 요청 실행 중 임시 에이전트의 기본 노드를 Mac으로 바꿨지만, 기존 세션 소유권에 따라 두 요청 모두 Windows로 전달됐다. 배치 판단은 맞았고, worker 내부 대기열 재개가 실패했다.

원인은 대기열 재개 코드가 `cached_serenity_ctx`와 REST 토큰을 모두 요구한 데 있다. worker 프로필에는 Gateway가 없으므로 조건을 충족할 수 없었다. 저장된 대기열의 시작 시 복구도 Gateway 부팅 경로에만 연결돼 있었다.

## 수정 구조

- `queue_io/transport.rs`: Gateway 캐시를 선택 사항으로 두고 기존 REST 인증 정보를 사용한다.
- `queue_dispatch/kickoff.rs`: 채널 정책, mailbox, dispatch lease, 중앙 intake admission, queued marker 정리를 공통 경로에서 처리한다.
- `session_runtime/channel_routing.rs`: Gateway와 REST가 동일한 allowlist·스레드 부모·provider·DM 검증을 사용한다.
- `runtime_bootstrap/queued_recovery.rs`: 기존 대기열 → inflight → dispatch marker → placeholder 복구 순서를 공통 함수로 분리한다. worker도 이를 거쳐 새 intake를 시작한다. Discord 메시지 이력 재수집은 worker에 추가하지 않는다.

단일 머신의 기본 프로필은 `full`, cluster는 선택 사항이다. 별도 worker 바이너리나 새로운 배치 알고리즘은 추가하지 않는다. 기존 세션 소유권과 OS 등 필수 조건은 계속 중앙 admission에서 확인한다.

## 추가 테스트 범위

| 경계 | 기대 동작 | 검증 상태 |
| --- | --- | --- |
| 기본 노드의 CLI·인증·poller·용량·OS·backend·등록 소실 | 호환 가능한 대체 노드만 선택 | 새 PG 테스트 추가, 실행 중 |
| 12개 동시 입력과 2개 슬롯 | 정확히 2개 예약, 해제된 슬롯 재사용 | 새 PG 테스트 추가, 실행 중 |
| 동일 Discord 메시지 동시 재전송·완료 후 기본 노드 변경 | 한 번만 전달 | 새 PG 테스트 추가, 실행 중 |
| idle 세션, 오프라인 소유 노드, 중복 소유 세션 | 기존 owner 유지 또는 명시적 차단 | 새 PG 테스트 추가, 실행 중 |
| 명시적 노드 override와 필수 조건 충돌 | 조건 우회 및 조용한 대체 금지 | 새 PG 테스트 추가, 실행 중 |
| REST 자격 증명은 있고 Gateway는 없음 | 대기열 실행용 transport 생성 | 새 회귀 테스트 추가, 실행 중 |
| 스레드 이름·부모 allowlist·DM | 기존 라우팅 정책 유지 | 새 회귀 테스트 추가, 실행 중 |
| 대기 중 채널 정책 변경으로 접근 거절 | 원본 대기 메시지와 디스크 상태 보존 | 새 회귀 테스트 추가, 실행 중 |
| 잘못된 기본 노드·OS API 입력 4개 | 요청 거절, 기존 설정 불변 | 실제 API 통과 |
| 실행 중 후속 입력 | 앞선 작업 완료 후 자동 시작 | 기존 배포에서 실패 재현, 수정 검증 중 |
| 취소 후 대화 재사용 | 자식 프로세스 종료 후 다음 요청 처리 | 후속 실제 검증 예정 |
| worker 오프라인·복귀 | 기존 owner·필수 OS 경계와 대화 복구 | 후속 실제 검증 예정 |

## 증거와 배포 경계

현재 Mac mini와 Windows의 설치본은 `3befd2079`다. 이 문서의 대기열 수정은 아직 빌드·회귀 검증 중이며, 설치본에 반영됐다는 의미가 아니다.

기존 에이전트 23개 전체 행의 checksum은 `3a34d55f1b525aa0dc7673ad7885e81e`로 유지된다. 실제 테스트에는 별도로 만든 에이전트 2개와 전용 Discord 스레드 2개만 사용한다.

로컬 원시 증거: `target/heterogeneous-worker-validation/rest-worker-queue-reproduction.json`, `worker-edge-live-20260922.json`, `worker-edge-focused-tests.json` 및 개별 로그. 원시 증거 폴더는 Git 추적 대상이 아니다.

첫 시도에서는 provider 세션 ID가 아직 저장되기 전에 테스트 코드가 문자열로 가정해 실패했다. 테스트의 nullable 필드 처리를 고친 뒤 새로운 요청으로 재현했으며, 이 테스트 코드 오류와 실제 대기열 장애는 별도로 기록했다. 기본 Windows 테스트 빌드도 컴파일러 프로세스 오류로 중단돼, CI와 동일한 debug 정보 비활성 설정으로 다시 빌드하고 있다.
