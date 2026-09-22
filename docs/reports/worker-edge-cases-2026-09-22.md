# Worker 배치·대기열 추가 엣지케이스 검증

검증일: 2026-09-22. 대상은 Mac mini leader와 Windows worker이며, PostgreSQL 자동 테스트는 운영 DB와 분리된 테스트 서버를 사용했다.

대기열 수정 실행 코드 `45c63b12560ef317eb400c54d32cfe0ba41e4acd`를 두 머신에 배포했다.
관련 자동 검사 76개가 통과했고, Windows의 대기열 복구·연속 입력·취소·오프라인 차단·복귀와
Mac 기본 대화를 실제 Discord에서 확인했다. 이전 배포 기록은 [통합 배포 보고서](heterogeneous-worker-rollout-2026-09-22.md)에 있다.

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

## 자동 테스트

새 PG 테스트 5개, 새 REST 회귀 테스트 3개를 포함해 관련 검사 **74개와 기존 local 배정 PG 검사 2개, 총 76개**가 통과했다. 전체 저장소 테스트를 모두 실행했다는 뜻은 아니다.

| 경계 | 기대 동작 | 검증 상태 |
| --- | --- | --- |
| 기본 노드의 CLI·인증·poller·용량·OS·backend·등록 소실 | 호환 가능한 대체 노드만 선택 | 새 PG 검사 통과 |
| 12개 동시 입력과 2개 슬롯 | 정확히 2개 예약, 해제된 슬롯 재사용 | 새 PG 검사 통과 |
| 동일 Discord 메시지 동시 재전송·완료 후 기본 노드 변경 | 한 번만 전달 | 새 PG 검사 통과 |
| idle 세션, 오프라인 소유 노드, 중복 소유 세션 | 기존 owner 유지 또는 명시적 차단 | 새 PG 검사 통과 |
| 명시적 노드 override와 필수 조건 충돌 | 조건 우회 및 조용한 대체 금지 | 새 PG 검사 통과 |
| REST 자격 증명은 있고 Gateway는 없음 | 대기열 실행용 transport 생성 | 새 회귀 검사 통과 |
| 스레드 이름·부모 allowlist·DM | 기존 라우팅 정책 유지 | 새 회귀 검사 통과 |
| 대기 중 채널 정책 변경으로 접근 거절 | 원본 대기 메시지와 디스크 상태 보존 | 새 회귀 검사 통과 |
| 허용하지 않은 첨부 URL 4개 | 다운로드·outbox·local session 생성 전 차단 | 기존 PG 검사 계약 수정 후 통과 |
| cluster와 역할 설정 생략, routing 비활성 | 기본 `full` 및 기존 local 실행 유지 | 설정·기존 PG 검사 통과 |

나머지는 queue presleep 26개, intake admission 15개, queued placeholder 3개, queued card gate 11개,
runtime profile 2개, cluster 설정 3개, 완료 처리 4개, REST 전달 2개다. 새 8개와 합해 74개이며,
기존 local 배정 PG 2개는 별도로 실행했다. PostgreSQL은 운영 5432/worker tunnel 15433이 아닌
격리 테스트 서버 15432와 테스트별 DB를 사용했다.

PG lane membership, SQL 실행 지점 inventory, test target integrity, `cargo fmt --all -- --check`,
`git diff --check`가 통과했다. 유지보수성 hard gate 위반은 없고, 기존 debt 허용 기준을 늘리지 않았다.

## 실제 Discord·DB 검증

Windows 검증은 [연속 대화 스레드](https://discord.com/channels/1469509996621594686/1551857716555948033)와
[Windows 필수 조건 스레드](https://discord.com/channels/1469509996621594686/1551857718711689306)에서 수행했다.

| 상황 | 실제 결과 |
| --- | --- |
| 잘못된 기본 노드·OS API 입력 4개 | 400/422로 거절, 기존 정책 불변 |
| 이전 배포에서 멈춘 메시지를 남겨 둔 채 worker 갱신 | 원래 입력 `1551858427817361482`를 재전송하지 않고 답변 `1551879554581397525` 수신. 대화 맥락과 provider session 유지 |
| 새 실행 중 후속 입력, 실행 중 기본 노드를 Mac으로 변경 | 앞선 답변 `1551879852007882753` 이후 대기 답변 `1551880214424981555`. 같은 Windows DB/provider session 유지 |
| Mac 중앙 API로 실행 취소 후 새 요청 | 관측한 PowerShell 자식 PID 종료를 7.97초 안에 확인. 답변 `1551880692689010709`에서 기존 대화 맥락 유지 |
| 기존 Windows 세션의 worker 중지 | 안내 `1551882035239583786`, 새 outbox 및 다른 머신의 session 생성 없음 |
| Windows 필수 조건을 둔 새 대화, Windows 중지 | 안내 `1551882030994690160`, Mac으로 조건을 우회하지 않음 |
| worker 복귀 후 기존 대화와 필수 Windows 새 대화 | 각각 `1551882593375363144`, `1551882779300601867`로 답변. 기존 대화의 provider session과 맥락 유지 |
| 노드 미지정 agent의 새 Mac 대화와 후속 입력 | 답변 `1551883156016070737`, `1551883267848929343`. Mac session 111137과 provider session 유지, intake outbox 0개 |

Discord 답변은 미리 만든 placeholder를 수정할 수 있다. 따라서 메시지 ID의 순서만으로
완료 순서를 추정하지 않는다. Windows 연속 입력은 17:56:50 KST에 들어왔고, 첫 답변의 최종 수정은
17:57:50, 후속 답변의 최종 수정은 17:58:19였다. Windows에서는 각 입력의 원문 링크를 기준으로 답변 한 개를 확인했다.

[Mac 기본 대화 스레드](https://discord.com/channels/1469509996621594686/1551883111875481636)에서도
각 입력에 답변 한 개와 대화 연속성을 확인했다. 다만 **Mac 답변의 원문 링크 미표시**와
**idle 세션 force-kill API의 20초 응답 timeout**은 기존 보고서와 동일하게 관측됐다.
기본 대화·배치 호환성 통과를 Mac의 모든 표시·세션 제어 기능이 완전 정상이라는 뜻으로 확대하지 않는다.

## 배포·호환성 판단

[Native release CI 35702962415](https://github.com/kunkunGames/AgentDesk/actions/runs/35702962415)의
validate, dashboard, Windows x86_64·macOS ARM64·Linux x86_64 native build가 모두 통과했다.
publish는 skipped이며 공개 GitHub Release를 게시하지 않았다. Mac과 Windows archive의
source commit, profile, 파일 계약과 SHA-256을 확인한 뒤 실제 설치했다. 운영 schema는 127로 유지했다.
검증 종료 시 `git ls-remote`로 확인한 `kunkunGames/AgentDesk`의 `main`도 같은 `45c63b125`였다.
뒤따르는 테스트 계약·문서 보완은 실행 코드를 바꾸지 않으므로 배포 artifact는 이 commit을 유지한다.

| 장비 | 역할 / 프로필 | 실행 코드 |
| --- | --- | --- |
| Mac mini `single-node` | leader / full, 기본 Discord gateway와 실행 | `45c63b125` |
| Windows `windows-worker-1` | worker / worker, 실행 전용 | `45c63b125` |

기존 에이전트 23개 전체 행의 checksum은 `3a34d55f1b525aa0dc7673ad7885e81e`로 유지된다.
23개 모두 기본 노드는 `null`, 필수 실행 조건은 `{}`이며 전역 자동 용량 배정도 비활성이다.
따라서 이 배포로 기존 Discord 에이전트가 일괄 Windows로 이동하지 않는다.

**최종 감사: 2026-09-22 18:19 KST.** 두 노드 모두 `online`, `healthy`, `fully_recovered=true`,
`db=true`이고 active/finalizing/queue는 모두 0이었다. 검증 스레드의 전달 outbox 9행은 모두
Windows 대상·`done`·attempt 1이며 메시지별 중복 행이 없었다. 취소 요청도 인수 기록에는
포함되므로 9행을 모두 성공 답변으로 세지 않는다. 오프라인에서 거절한 두 입력과 Mac 기본
대화는 전달 outbox가 없다. 임시 agent 3개와 해당 스레드의 session을 제거하고 Discord
스레드는 기록을 남긴 채 archive했다. 늦게 재등록된 Mac idle 행도 제거 후 60초 동안
재등록되지 않았으며, 마지막 DB 조회에서 검증 session 수 0을 다시 확인했다.

현재 요구에는 공통 바이너리와 명시적 worker profile, 기존 intake router·세션 제어·대기열 복구를
재사용하는 구성이 비용 대비 효과가 높다. 역할별 binary, 새 스케줄러, 별도 메시지 복제 계층을 추가하지 않는다.
우선 실행 노드와 필수 OS 조건을 구분하며, 기존 owner는 계속 보존한다. 여러 worker는 instance ID와
readiness·capacity로 구분하므로 특정 Windows 한 대에 종속되지 않는다.

플랫폼별 native build 통과와 실제 장비 운영은 구분한다. Linux worker와 Mac Studio는 실기기 설치를
검증하지 않았고, Mac mini의 gateway/DB 고가용성 및 worker의 prompt·로그인·작업 폴더 자동 동기화는
제공하지 않는다. 패키지 크기·메모리·지연 시간이 모든 대안보다 최소임을 측정한 것도 아니다.

## 검증 과정과 원시 증거

- 첫 live 시도는 아직 저장되지 않은 provider ID를 테스트 코드가 문자열로 가정해 중단됐다. nullable 처리를 고친 뒤 새로운 요청으로 실제 대기열 장애를 재현했다.
- Windows 기본 debug 빌드는 컴파일러 프로세스 오류로 중단됐다. CI와 같은 debug 정보 비활성 설정으로 빌드했고, 테스트 계약 수정본도 별도 소스 snapshot에서 컴파일했다.
- 최초 관련 검사 중 하나는 portable attachment bundle 도입 전의 blanket 차단을 기대하고 실제 CDN의 가짜 주소에 의존했다. 현재 계약에 맞춰 허용하지 않는 URL 4개를 다운로드 전에 거절하는 검사로 수정했으며, 최초 실패 기록과 해당 모듈 재실행 결과를 모두 보존했다.
- 이전 검증 스레드의 종료된 세션 기록 4개를 발견했다. archive 상태, 실제 backend 부재, 정확한 스레드·세션 ID를 확인한 뒤 정상 삭제 API로 정리했다.
- Mac full profile의 기존 YAML roster 동기화가 DB에만 만든 임시 agent 2개를 제거하고 nullable session `agent_id`를 비웠다. 기존 23개는 불변임을 확인하고 임시 fixture만 다시 만들었다. 소유권 검사는 정확한 검증 channel과 instance ID를 사용한다.
- worker 재기동 직후 readiness의 provider 목록이 아직 비어 있는 순간을 검증 코드가 처리하지 못했다. 부팅 중 빈 목록을 대기 상태로 처리하도록 고치고, 이미 통과한 오프라인 입력을 다시 보내지 않은 채 복귀 검증을 이어서 통과했다.
- Windows cleanup 코드가 처음에는 session key를 요구하는 force-kill 경로에 숫자 DB ID를 보내 400을 받았다. 문서화된 key 경로로 수정한 뒤 정상 정리됐다. Mac에서는 tmux 종료 직후 늦은 등록으로 생긴 idle 행 111138을 실제 DB 감사가 발견했다. backend 부재를 확인한 후 해당 행만 정상 삭제 API로 정리하고 재등록 여부를 관찰했다.

로컬 원시 증거는 Git 추적 대상이 아닌 `target/heterogeneous-worker-validation/`에 있다.

- `rest-worker-queue-reproduction.json`, `worker-edge-live-20260922.json`: 최초 장애와 실제 입력·응답·취소·복귀 기록.
- `worker-edge-focused-tests-initial.json`, `worker-edge-focused-tests.json`, `worker-edge-final-test-summary.json`: 최초 실패와 최종 76개 검사 결과.
- `worker-edge-final-static.json`, `worker-edge-ci-result.json`: 정적 검사와 native release CI 결과.
- `worker-edge-native-artifacts.json`, `worker-edge-windows-deployment.json`, `mac-worker-queue-deploy.log`: 후보 archive와 두 머신 배포.
- `worker-edge-old-test-cleanup.json`: 이전 검증에서 남은 종료 세션 정리.
- `worker-edge-mac-default.json`, `worker-edge-mac-cleanup-lifecycle.json`, `worker-edge-mac-cleanup-confirmation.json`: Mac 기본 대화, 알려진 idle 제어 timeout, 실제 정리 확인.
- `worker-edge-final-audit.json`: 최종 배포 identity, 양쪽 health·DB, 원래 agent 23개 보존, 검증 outbox와 session 정리 확인.
