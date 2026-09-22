# Mac mini leader / Windows worker 적용 및 실기기 검증

기준일: 2026-09-22. 설계 기준은 [이기종 worker 도입 검토](../design/heterogeneous-worker-cluster-implementation-review.md),
운영 계약은 [worker 프로필](../operations/worker-runtime-profile.md),
[실행 용량](../operations/execution-capacity.md),
[에이전트별 기본 실행 노드](../operations/agent-execution-node.md),
[릴리스 패키징](../ci/release-packaging.md)이다.
이 문서는 소스 구현, 자동 검사, 실제 설치, provider 응답을 구분한다.

## 결정과 ROI

현재의 신뢰하는 소규모 장비 집합에서는 **하나의 AgentDesk 코드와 OS별 공통 artifact에
역할별 설정을 적용하는 구성**을 유지한다. 별도 worker 제품, coordinator, 메시지 큐,
DB 또는 agent 정의 사본을 새로운 운영 정본으로 추가하지 않는다. 기존 PostgreSQL의
outbox·owner·lease와 provider 실행 코드를 재사용한다.

이는 개발·유지보수·업데이트 비용에 대한 판단이다. 전체 대안의 비용/성능 벤치마크로
최대 ROI를 입증했다는 뜻은 아니다. worker 전용 바이너리는 다운로드 크기나 배포
시간이 실측 병목일 때 다시 검토한다. 압축률 개선만으로 실행 메모리가 줄지는 않는다.

`16757ee52` Windows package의 ZIP entry 크기를 실제 측정한 결과는 다음과 같다.

| 구성 | 압축 전 | ZIP 내부 압축 크기 |
| --- | ---: | ---: |
| 공통 `agentdesk.exe` | 134.4 MiB | 40.8 MiB |
| dashboard 정적 자산 | 114.9 MiB | 112.4 MiB |
| 기타 runtime 자산 | 0.9 MiB | 0.3 MiB |
| 전체 archive | — | 153.6 MiB |

대시보드 sprite PNG가 archive의 약 73%를 차지한다. 배포 전송량을 줄이는 다음 단계는
worker용 Rust 코드를 별도로 빌드하는 것보다 **동일 실행 파일을 유지하면서 dashboard
자산을 선택적으로 전달하는 패키징**이다. 기존 packager의 `--without-dashboard`는
내부 package 생성에 사용할 수 있지만, 현재 공개 release verifier의 공통 artifact 계약은
dashboard 포함을 요구한다. 공개 worker artifact가 이미 제공된다고 보지 않는다.
현재 Windows 설치에서는 dashboard 파일을 복사하지 않았다. 위 수치는 package 구성의
크기이며 backup·로그·작업 폴더 또는 provider 자식 프로세스의 메모리 측정은 아니다.

응집도는 cluster 배정·readiness·실행 요구 조건·첨부 전달·용량 모듈에서, 재사용은
공통 provider 실행·세션 제어 handler·패키징에서 확보한다. 플랫폼 차이는 process,
socket, 디렉터리 연결, 서비스 실행 adapter에 둔다.

## 실제 배치

| 항목 | Mac mini | Windows PC |
| --- | --- | --- |
| 주소 | `192.168.1.147:8791` | `192.168.1.222:8791` |
| instance ID | `single-node` — 기존 owner 식별자 보존 | `windows-worker-1` |
| role / profile | `leader` / `full` | `worker` / `worker` |
| 프로세스 관리 | 사용자 LaunchAgent | 사용자 로그온 Task Scheduler |
| 실행 slot | 2 | 2 |
| PostgreSQL | Mac 내부 loopback 5432 | loopback 15433 → SSH → Mac 5432 |
| DB pool max / foreground reserve | 32 / 6 | 6 / 2 |
| 자동 capacity 배정 | 비활성 — 에이전트별 명시적 선택부터 적용 | 비활성 |

Mac의 기존 instance ID를 보기 좋은 새 이름으로 변경하지 않았다. durable session
owner를 변경하는 동작과 장비 표시명을 바꾸는 동작은 다르다.

Windows worker에서 gateway, voice, dashboard, admin CRUD, leader background services는
시작하지 않는다. 공통 실행·poller·heartbeat·owner 출력·취소·복구는 유지한다.
실제 health에서 `worker`, `healthy`, `fully_recovered=true`와 모듈 제외를 확인했다.
Windows `/`, `/api/agents`, `/api/settings`는 인증된 요청도 404다.

Windows 작업은 로그인한 사용자 권한으로 실행된다. 재부팅 후 로그온 전/로그아웃
상태의 무인 Windows 서비스까지 구현한 구성은 아니다.

## 기존 Discord 에이전트와 세션

기존 에이전트의 역할, 프롬프트, Discord 채널 연결은 유지한다. leader/worker는
에이전트 종류가 아니라 **AgentDesk 노드의 역할**이다. 모델의 프롬프트 판단으로
실행 위치를 결정하지 않는다.

1. Mac leader가 Discord 입력을 수신한다.
2. 기존 live owner가 있으면 그 노드를 유지한다.
3. 새 세션은 명시적 노드 선택, 필수 실행 조건, 준비 상태와 선택된 배정 정책을 따른다.
4. Windows에 배정된 turn은 Windows의 provider CLI와 로컬 작업 경로에서 실행된다.
5. owner가 결과를 전달하고, Mac의 중앙 API는 owner의 출력·취소 API로 요청을 전달한다.

기존 운영 세션을 자동으로 다른 OS에 이사시키지 않는다. `ProcessBackend` 자식은
worker 프로세스 종료와 함께 끝나며 살아 있는 프로세스 재연결을 성공으로 가장하지
않는다. 후속 turn/resume은 provider와 세션 복구 계약을 따른다.

기존 23개 agent row는 Windows 초기화·재시작 및 Mac 재배포 전후의 동일 집계
checksum으로 보존을 확인했다. Discord 실험에는 전용 thread와 임시 검증 agent를
사용하며, 이 agent는 운영 정의 변경을 뜻하지 않는다.

운영자가 새 채널/스레드에서 `/node` 선택기로 worker를 지정할 수 있다. 기존 live
owner가 있으면 해당 owner가 우선하므로 이 선택은 살아 있는 Mac 세션을 Windows로
이동시키는 명령이 아니다. 새 소스는 에이전트 상세 화면에서 기본 실행 노드를 선택하고,
직접 연결되지 않은 Discord 스레드가 실제 부모 채널의 실행 정책을 상속하도록 구현했다.
직접 연결된 스레드 정책은 부모보다 우선하며 `threadInherit: false`도 유지한다.
이 기능은 migration 127과 새 leader 설치가 필요하다. 기존 schema 126 설치본에
UI와 부모 실행 정책 상속이 이미 반영됐다는 의미는 아니다.

기본 노드는 우선 장비로 취급한다. 소유자가 없는 새 세션은 우선 장비의 준비 상태·필수
조건·여유 슬롯을 확인하고, 불가능하면 다른 호환 노드를 선택한다. 우선 장비가 leader여도
동일하게 우선권을 적용한다. 기존 `/node` 선택과 필수 `nodes` 조건, live/stale owner와
outbox 중복 방지 경계를 재사용하며 별도 스케줄러를 추가하지 않았다. 우선값이 없는
기존 agent는 전역 자동 배정이 꺼진 상태에서 기존 local 동작을 유지한다.

Mac mini가 꺼지는 경우는 별개다. 현재 mini가 gateway와 PostgreSQL을 함께 실행하므로
실행 전용 Windows worker만으로 Discord 서비스를 이어받을 수 없다. 우선 구성은
상시 가동 mini와 복수 worker이며, gateway/DB 고가용성은 이번 실행 대체 범위에 포함하지 않는다.

Windows의 세션은 background native child process로 실행된다. Discord 에이전트의
역할을 다시 작성할 필요는 없지만, 각 worker에는 provider CLI/로그인과 실제 작업
경로가 필요하다. Git 저장소의 논리 ID를 노드별 로컬 경로에 매핑하며, Mac의 파일,
로그인 상태 또는 실행 중인 터미널을 다른 OS로 자동 복제하지 않는다.

중앙 agent roster와 worker의 실행 설정 배포도 구분한다. 현재 역할·공통 프롬프트는
`settings/content.rs`에서 **실행 노드의 파일**을 읽는다. DB 공유만으로 Mac의 prompt
수정이나 새 agent의 로컬 설정이 모든 worker에 자동 전달되지는 않는다. worker의
local YAML은 중앙 roster를 덮어쓰지 않도록 startup reseed와 config audit 양쪽에서
차단한다. 운영 적용 시 대상 agent의 prompt·bot 설정·작업 경로를 worker에 준비하고
검증해야 하며, 모든 기존 agent가 설정 변경 없이 즉시 이동 가능하다고 보지 않는다.

## 네트워크와 인증

Windows→Mac과 Mac→Windows API 통신 및 Mac에서 바라본 인증된 node probe를
실제로 확인했다. Windows 노드는 `online`, `session_api_routable=true`,
`reachability_verified=true`, `trust_validated=true`로 관측됐다.

운영자가 관리자 PowerShell에서 등록한 `AgentDeskWorker-TCP-8791` 규칙은
`192.168.1.147`에서 Windows의 설치된 `agentdesk.exe`로 오는 TCP 8791을 허용한다.
공유기 포트 포워딩이나 인터넷 공개를 설정하는 명령이 아니다. Windows의 별도 자동
앱 허용 규칙도 존재하므로 이 명시적 규칙 하나만으로 전체 방화벽의 유일한 허용
범위를 단정하지 않는다.

SSH 호스트 키 고정과 공개키 인증을 사용하고 PostgreSQL은 LAN에 추가 공개하지
않았다. worker는 선택한 trusted-worker 모델에 따라 중앙 DB와 Discord REST
credential을 사용한다. DB 비접속 worker나 노드별 credential 격리를 제공한다는
주장은 하지 않는다. 비밀값은 release artifact나 이 문서에 포함하지 않는다.

## 마이그레이션과 Mac 배포

운영 DB 전체 백업, 기존 binary·설정 백업을 보관한 뒤 격리 PostgreSQL에 복원하여
migration을 두 번 실행했다. 120/121번에 적용됐던 과거 Kakao migration은 SQL
checksum과 description이 정확히 일치하는 경우만 122/123번 이력으로 이관했다.
checksum 검사를 비활성화하지 않았다.

운영 schema는 `0126_execution_capacity.sql`까지 적용됐다. Kakao 관련 데이터는
격리 검증에서 bindings 2, events 34, targets 36, requests 38, operations 42개로
보존됐다. 이전 schema를 전제로 한 binary만 되돌리는 rollback은 사용하지 않는다.

Mac 배포는 기존 deploy script의 drain, migration, package layout을 재사용했다.
기존 운영자가 지정한 내부 디스크 launchd stdout/stderr 경로를 원자적 plist 갱신 시
보존하도록 수정했고, restart health 요청에는 실제 listener 포트를 포함한 Origin을
사용한다. helper 자동 검사와 실제 launchd plist/health 호출이 통과했다.

Mac의 Claude CLI는 설치·설정돼 있지 않다. 이 조건을 health provider 목록으로
확인한 경우에만 계정 탐색 API의 `503 / not_installed`를 정상적인 미설치 진단으로
판정한다. 다른 503, 다른 endpoint, provider 목록 검증 실패는 계속 실패다.
Mac 운영 API smoke가 통과했고 full profile은 healthy 상태다.

Windows에서 실제 Mac LAN dashboard를 headless Chromium의 desktop 1440×900 및
mobile 390×844 viewport로 열어 로그인, agent API, 인증 WebSocket 연결, 로그아웃과
새로고침 후 재인증을 확인했다. 브라우저 storage에 서버 토큰이 남지 않는 것도 검사했다.

외장 SSD `990PRO`의 디렉터리 읽기와 원래 소스 저장소 Git 조회에는 5~7초 timeout이
재현됐다. 단순 경로 stat 성공은 내부 파일 읽기 정상의 증거가 아니다. 하드웨어 고장,
권한/TCC 또는 다른 원인 중 무엇인지 아직 확정하지 않았다. Mac 배포는 내부 디스크의
검증 checkout·Python 환경·npm cache와 현재 운영 prompt를 사용해 완료했다. 외장
SSD를 사용하는 원래 repo readiness는 false이며 별도 운영 확인이 남는다.

## Windows 실기기에서 발견하고 수정한 사항

### 일반 사용자 디렉터리 초기화

Windows symlink 권한 오류 1314일 때만 디렉터리 junction을 사용한다. 관리자 권한이나
Developer Mode 없이 worker를 기동했고, 괄호·따옴표·한글 경로 및 연결 제거 후 원본
보존 검증이 통과했다. 공유 prompt 정본을 복사하는 방식으로 대체하지 않았다.

### 강제 종료와 포트 상속

작업 스케줄러 중지 뒤 OpenCode 하위 프로세스가 남아 이전 API listener를 유지하는
문제를 실제 PID 계보와 포트 해제로 확인했다. dcserver 시작 전에 kill-on-close Job
Object를 설정하고 Windows listener를 생성 시점부터 비상속으로 만든다.

모듈 자동 검사, 실제 Task Scheduler 손자 프로세스 종료 검사, **운영 AgentDesk의
연속 두 차례 stop/start**가 통과했다. 두 번째 검증에는 cmd → node → OpenCode
하위 프로세스가 포함됐으며, 모두 종료·8791 해제·새 PID healthy 복구를 확인했다.

### provider wrapper의 플랫폼 경계

실제 Discord 입력은 Mac에서 Windows로 전달됐고 `intake_outbox`와 `sessions`
owner가 Windows로 기록됐다. 첫 응답은 정상 모델 답변이 아니라 프로세스 종료 안내였다.
Windows `ProcessBackend`가 호출하는 공통 wrapper CLI가 Unix 조건으로 제외된 것이
원인이며, 설치된 binary에서 `unrecognized subcommand`로 재현했다.

공통 Claude/Codex/Qwen pipe wrapper와 진단을 모든 플랫폼에서 사용하고 FIFO만
Unix에 제한한다. Release matrix에 실제 native binary의 pipe 진입점 검사를 추가했다.
기존 Mac binary는 세 진입점 모두 통과했다. Windows 수정본의 실제 모델 응답도
아래 Discord 검증에서 확인했다.

Windows npm `codex.cmd`는 12,000자 입력에서 `The command line is too long`으로
실패했고 같은 설치의 native `codex.exe`는 동일한 help 검사에 성공했다. 공통 탐색기에
동일 npm 설치의 native 실행 파일 해석을 추가했다. x64/ARM64, nested/hoisted/legacy
설치와 잘못된/custom launcher 보존을 검사했다. macOS/Linux와 명시적 operator
registry/env 경로의 선택 의미는 유지한다.

Windows 기본 사용자 Codex 계정의 별도 read-only/ephemeral 요청은 실제 정답 응답을
받았다. 이 검사와 Discord→cluster→provider 통합 검증은 구분한다.

`16757ee52`를 실제 Windows worker에 설치한 뒤 Discord에서 요청한 검증 문자열을
받았다. 재시작 직후 readiness/probe가 준비되기 전 입력은 실행을 거절했고, leader가
새 worker의 준비 상태를 확인한 뒤 전달한 입력은 Windows Codex가 처리했다.

### 호스트 식별자와 여러 봇의 실행 문맥

Windows의 `hostname -s` 실패를 성공한 빈 출력으로 받아들이던 공통 함수 때문에
세션 키에 호스트 부분이 빠지고 중앙 출력 API가 409를 반환했다. Unix에서만 `-s`를
사용하고 종료 코드·빈 출력·형식을 검증하도록 수정했다. Windows의 실제
`COMPUTERNAME`과 일치하는지 포함해 세 검사가 통과했다.

같은 provider의 여러 봇이 하나의 worker queue를 읽을 때, 작업을 먼저 가져온 봇의
SharedData/HTTP/token을 사용하던 문제도 확인했다. 실제 검증에서 세션 namespace가
바뀌고 다른 봇의 완료 표시 편집이 `Unknown Message`로 실패했다. 기존 채널별 runtime
resolver를 재사용하여 **accept 전에 채널 소유 봇을 결정**하고, 해당 봇의 실행 상태와
REST credential을 함께 사용한다. 게이트웨이가 없는 worker에서도 명시적 채널과 실제
Discord thread의 부모 채널을 확인하며, 후보가 중복되거나 확인할 수 없으면 거절한다.
작업을 가져온 runtime과 실행 runtime 양쪽의 재시작 차단·drain을 유지한다.

실제 resolver 소스를 포함하는 격리 harness의 열한 검사가 통과했다. 이 harness의 Discord
HTTP/registry 의존성은 test double이므로 실제 다중 봇 연속 대화·첨부·취소 검증을
대체하지 않는다. 부분 기동 중 아직 등록되지 않은 봇의 작업을 다른 제한된 runtime이
대신 받지 못하게 하는 조건도 포함한다. `b38aa916f` Windows 설치 후 실제 Discord
검증에서 지정된 Codex 봇의 답변, 후속 세션 유지, 첨부 처리와 중앙 출력 조회가 통과했다.

사용자의 CLI 목적지가 Mac leader인 경우에도 startup doctor는 Windows 자신의
loopback API를 검사하도록 공통 HTTP client의 명시적 base URL 요청을 재사용했다.
수동 doctor의 원격 credential 전송 opt-in은 유지한다. Windows에서는 Unix tmux
session-discovery loop를 시작하지 않고 native process registry를 사용한다.

### 실제 Discord 검증과 실행 취소

전용 [검증 스레드](https://discord.com/channels/1469509996621594686/1551757855168925736)에서
`b38aa916f` 설치본을 검사했다. 일반 답변은 지정된 Codex 봇이 보냈고 DB session 111128의
owner는 Windows였다. 후속 대화에서 동일 DB session과 provider session ID를 유지했다.
실제 Discord 첨부 파일에만 넣은 무작위 문자열을 Windows에서 읽어 정확히 답했고,
중앙 Mac API의 session output도 `process`, `alive`, `available`로 조회됐다.

동일 설치본의 실행 취소는 실패했다. Mac API가 취소 성공과 in-flight 해제를 반환했지만
실행 중인 PowerShell `Start-Sleep`은 남았다. 로그의 `cancel_token_missing_runtime_target`
및 `no_verified_process_backend_target`과 코드를 대조한 결과, Codex/Qwen의 재사용 wrapper에
새 turn의 취소 토큰을 연결하지 않던 것이 원인이었다. 공통 process 입력 함수에서
각 turn의 PID를 입력 전 등록하고 이미 취소된 입력을 거절하도록 수정했다. Claude도
같은 경로를 재사용하며 Windows `taskkill`은 실제 성공 종료 코드를 확인한다.
소스 수정만으로 실제 취소 성공을 주장하지 않으며 새 설치본에서 자식 종료를 재검증한다.

### 에이전트별 기본 노드 선택

`fc579aa65`부터 `agents.default_execution_node_id`, full-profile API, 에이전트 상세 화면의
장비 선택과 Discord 부모 정책 상속을 추가했다. 우선순위는 기존 live owner → 채널 `/node`
→ 에이전트 기본 노드 → label/활성 자동 배정이다. 선택은 구체적인 instance ID이며,
오프라인 또는 실행 조건 불충족 시 다른 OS로 자동 대체하지 않는다.

대시보드 컴포넌트 4개 검사, TypeScript 검사와 production build가 통과했다. Playwright의
desktop/mobile에서 선택만으로 쓰기 요청이 발생하지 않는 것, 저장, 상세 재개방 후 값 유지,
기본값 해제, 오프라인 안내를 확인했다. 인접 에이전트 상세/모바일 목록 검사도 통과했다.
이 브라우저 검증은 API fixture를 사용한다. UI 저장의 실제 운영 DB 반영 및 새 세션 배정은
migration 127과 leader/worker 배포 후 별도로 확인해야 한다.

## 릴리스와 검증 경계

- [PR #2118](https://github.com/kunkunGames/AgentDesk/pull/2118)의 native release workflow와
  [PR #2119](https://github.com/kunkunGames/AgentDesk/pull/2119)의 CI/복구 선행 수정은 main에 반영됐다.
- 2026-09-22 확인 당시 원격 main은 `f2a71b102738ded6631bac11043ebba9903e587c`이며
  구현 브랜치가 이 main을 포함한다. 구현 브랜치는 `feat/heterogeneous-worker-release`다.
  전체 cluster 기능이 main에 병합됐다는 뜻은 아니다.
- feature CI의 native Linux/Windows/macOS matrix `35664190385`는 모두 통과했다.
  이후 코드 수정본의 빌드와 실기기 응답은 별도로 검증한다.
- 공개 version tag나 GitHub Release를 게시하지 않았다. GitHub의 artifact 생성·검증·
  게시 workflow와 LAN 장비의 실제 설치는 별도 단계다. LAN으로 자동 배포하는
  상시 self-hosted runner가 설치됐다고 주장하지 않는다.
- PG capacity/attachment/requirements, Windows process 출력·취소, dashboard 인증과
  desktop/mobile fixture, packaging, migration rehearsal 검증이 통과했다.
  모든 repository test의 전체 통과나 모든 provider의 실제 계정 quota를 보증하지 않는다.
- Linux worker의 실제 설치, Mac Studio 추가 설치, 재부팅 후 무인 Windows 서비스,
  장시간 다중 노드 부하 검증은 수행하지 않았다.

## 완료 조건

Windows 수정본으로 실행 중인 자식의 실제 취소를 확인하고, 에이전트별 기본 노드 API와
새 세션의 선택 노드 실행을 확인한 뒤 임시 검증 agent를 정리한다. 전역 자동 배정은
비활성으로 유지하며 에이전트별 명시적 기본값부터 운영자가 선택한다. 기존 owner 보존과
필수 OS/provider 조건을 유지하며, SSD 장애에 따른 Mac 로컬 저장소 제한을 정상 동작으로
표시하지 않는다.

로컬 증거는 `target/heterogeneous-worker-validation/` 아래 build·migration·native
verification 로그와 `windows-production-restart-verification.json`에 남긴다. 이 경로의
운영 진단 파일을 release asset에 포함하지 않는다.
