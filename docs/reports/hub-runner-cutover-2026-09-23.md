# Hub/Runner 전체 장비 전환 검증

검증일: 2026-09-23 KST. 이 기록은 호환 별칭을 유지했던
[이전 용어 전환](hub-runner-terminology-2026-09-23.md) 이후의 **breaking cutover**다.
설치·확장·이동 절차는 [Hub/Runner 운영 문서](../operations/hub-runner-topology-and-migration.md)를 따른다.

## 배포 결과

배포한 실행 코드는 `eedf46d369273d2074da6d4c4bf7535c9ff75d3a`다.
[네이티브 빌드 35765254082](https://github.com/kunkunGames/AgentDesk/actions/runs/35765254082)의
Windows x64, macOS ARM64, Linux x64 패키지가 모두 성공했다. 공개 Release는 발행하지 않았다.
후속 커밋은 테스트 fixture·CI inventory·주석·문서를 정리하며 배포 코드의 동작을 바꾸지 않는다.

| 항목 | Mac mini | Windows PC |
| --- | --- | --- |
| 고정 장비 ID | `mac-mini` | `windows-pc` |
| 역할 / 기능 모드 | `hub` / `full` | `runner` / `runner` |
| 노드 protocol | schema 2 | schema 2 |
| 실행 backend | tmux, process | process |
| 실행 슬롯 | 2 | 2 |
| health / DB / 복구 | healthy / true / fully recovered | healthy / true / fully recovered |
| Codex 준비 상태 | eligible | eligible |
| Hub의 전달 검사 | 로컬 장비 | 신뢰 주소·인증·대상 장비 확인 성공 |
| 시작 출력 제한 | 운영 override 300초 | 운영 override 300초 |

코드 기본 제한은 60초다. 두 운영 장비의 기존 300초 override를 유지했다.
마지막 보존 검사는 2026-09-22 19:33:36 UTC에 수행했다.

| 보존 대상 | 전환 전 | 전환 후 | 비교 방법 |
| --- | --- | --- | --- |
| 에이전트 | 23 | 23 | 전체 행의 정렬된 JSON checksum 일치 |
| 세션 | 73 | 73 | ID, 소유 장비, provider 세션, session key, agent ID checksum 일치 |
| DB migration | 127 | 129 | 성공한 `_sqlx_migrations`의 최댓값 |

세션 checksum에서 승인된 장비 ID 매핑만 정규화했다. 원래 provider 세션 ID를 바꾸거나
다른 장비로 복사하지 않았다. 에이전트 checksum은 `3a34d55f1b525aa0dc7673ad7885e81e`,
정규화한 세션 식별자 checksum은 `911dc4d6263a66ee2878b466b3672caf`다.
검증용 에이전트와 세션을 제거한 뒤 같은 수와 checksum을 다시 확인했다.

Migration 0128과 0129를 적용하고 모든 노드가 중지된 상태에서
`single-node → mac-mini`, `windows-worker-1 → windows-pc`를 한 transaction으로 변경했다.
이전 `worker_nodes`, `worker_mcp_endpoints` 테이블은 존재하지 않는다.
YAML의 장비 ID, gateway 우선 장비와 상대 노드 신뢰 설정 키도 변경했다.
그 밖의 설정값이 의미상 동일한지 YAML 파싱 결과로 검증했다.

Windows의 `AgentDeskRelease`, `AgentDeskRelease-DatabaseTunnel` 작업이 실행 중이며
이전 `AgentDeskWorker` 작업 두 개는 XML 백업 후 제거했다.
기존 `AgentDeskAPI-TCP-8791` 방화벽 규칙은 실제 실행 파일과 Mac 주소만 허용한다.
Mac은 기존 `com.agentdesk.release` launchd 서비스를 사용한다.

## 백업과 서명

- Windows: `C:\Users\12336\.adk\backups\hub-runner-cutover-20260923-eedf46d36`.
  설정·runtime·실행 파일·예약 작업 XML을 보존했다. `config/credential` junction은
  링크 자체를 보존했고 연결된 credential 저장소를 복제하지 않았다.
- Mac: `/Users/kunkun/.adk/validation/hub-runner-cutover-20260923/backups/eedf46d36`.
  설정·runtime·launchd plist·실행 파일을 보존했다. runtime FIFO 40개는 파이프 형태로 보존했다.
- PostgreSQL custom dump: 169,293,342 bytes,
  SHA-256 `43d65bfa9d72c62d07f3cf1f26ab020777384f8a8dd93d6eb85e9915d5d7c670`.
  `pg_restore --list`로 agents/sessions 항목을 확인했다. 별도 DB에 전체 복원하는 검사는 하지 않았다.

Mac 서명은 공식 배포 스크립트의 기존 식별자 `com.itismyfield.agentdesk`를 유지한다.
첫 설치 보조 스크립트가 식별자를 생략해 launchd 프로세스의 LAN 접근이 실패했다.
같은 바이너리는 SSH에서 접속에 성공했고 launchd에서는 `No route to host (os error 65)`를
반환했다. 기존 식별자로 서명하고 정상 종료·재기동한 뒤 Hub의 전달 검사가 성공했다.
방화벽이나 macOS 개인정보 보호 설정을 완화하지 않았다.

최종 설치 파일 SHA-256:

- Windows: `49ddf000a78bc27a2936471d247446e6c02031223b8878c0f24f249149c789e0`.
- Mac: `cd455d4ae1f19627a45f2942d06935e570e15f24b968e9d36aa494b5b7a2d9e3`.
  Mac 파일 해시는 로컬 서명 이후 값이다.

## 실제 Discord 왕복과 화면

사용자가 허용한 테스트를 별도 에이전트와 스레드에서 수행했다. 각 에이전트에
우선 장비 저장 → 기본값 복원 → 우선 장비 재저장을 확인하고 명령 한 개만 보냈다.

| 장비 | 관측 결과 | 응답 메시지 ID |
| --- | --- | --- |
| Windows | 실제 출력 `Win32NT`, 소유자 `windows-pc`, process backend | `1552037766295060601` |
| Mac | 실제 출력 `Darwin`, 소유자 `mac-mini`, Hub 로컬 실행 | `1552037961363890199` |

각 요청에서 예상 Codex bot의 검증 문자열을 포함한 답변이 정확히 한 개였고,
`headless_turn` 중복 outbox는 없었다. Windows intake는 `done`, attempt 1, retry 0,
전달자 `mac-mini`였다. Mac의 Hub 로컬 경로는 원격 intake 행을 만들지 않는다.

Windows의 정상 force-kill API가 `tmux_killed=true`를 반환했으며 실제 native wrapper가
종료됐다. Mac 테스트 wrapper도 같은 API 요청으로 종료된 서버 로그와 이후
`alive=false`를 확인했다. 다만 Mac은 20초와 55초 클라이언트 제한 안에 응답을 받지
못했고, 이후 19:31:55 UTC의 tmux 종료 성공 로그로 확인했다. **Mac 종료 API의
응답 지연은 해결됐다고 주장하지 않는다.** 테스트 세션만 정리했고 원래 세션을 죽이지 않았다.
검증 스레드는 기록을 남긴 채 archive했다.

실제 Hub `/ops` 화면을 인증 후 Chromium에서 검사했다. API 응답을 모킹하지 않았다.

| 화면 | 노드·역할·전달 확인 | 후속 자동 갱신 | JS 오류 | 실패 HTTP 응답 | 가로 넘침 |
| --- | --- | --- | --- | --- | --- |
| 1280×960 desktop | 통과 | 통과 | 0 | 0 | 없음 |
| 390×844 mobile | 통과 | 통과 | 0 | 0 | 없음 |

서버가 제공하는 HTML과 진입점 JS/CSS 14개는 배포한 Mac 패키지 내용과 byte 단위로
일치했다. 로그인 토큰, 브라우저 저장 상태, 인증 화면 trace는 증거 파일에 저장하지 않았다.

## 테스트와 발견한 실패

- 로컬 Windows Rust 집중 검사: 22개 filter, 77 passed, 0 failed.
- [CI 35770525018](https://github.com/kunkunGames/AgentDesk/actions/runs/35770525018),
  소스 `3fd43d2d23e704ff153dd48c1d04f3922c62e24f`:
  library sweep 8,608 passed / 7 ignored, PG sweep 889 passed / 1 ignored.
  별도의 replay·targeted 검사는 이 합계에 중복 합산하지 않았다.
- 같은 CI에서 Windows 검사, Linux 컴파일, 린트, 고위험 복구, relay-authority mutation 검사 통과.
- 대시보드: 61개 파일 / 390 tests 통과, TypeScript·Vite 빌드 통과.
- 별도 mocked Playwright desktop/mobile 4개 검사 통과. 실제 화면 검사는 위 표로 구분했다.
- 보호된 기존 migration 132개 checksum, Rust formatting, diff whitespace 검사 통과.
- Python 회귀 49개는 WSL 17개 + WSL 31개 + Windows의 compiler 전용 1개로 검증했다.
  단일 환경에서 49개가 한 번에 성공한 결과로 표현하지 않는다.

처음 검출된 CI 문제는 누락된 writer-gate 경로 pin, Windows 전용 테스트 inventory,
분리된 helper를 따라가지 못한 소스 검사, runtime으로 옮겨진 queue route의 오래된
테스트 router, intake owner 취소 경로의 소스 정규식이었다. 테스트의 실제 계약을 유지하며
fixture와 inventory를 수정했다. 마지막 Script checks 실패는 새로 분리한
`merged_placeholders.rs`의 주석 비율 32.5%였다. 변경 이력 주석을 줄여 13.3%로 만들었고
동작 코드는 유지했다. 각 실패와 최종 재실행은 PR checks 및 아래 로그로 구분한다.

로컬 Rust 빌드는 처음 세 번 LLVM out-of-memory로 중단됐다. WSL의 사용하지 않는
clean page cache를 회수한 뒤 같은 빌드가 15분 10초에 성공했다. 다른 사용자 작업을
종료하거나 시스템 메모리 설정을 바꾸지 않았다.

## 증거 위치와 검증 범위

작업 증거의 로컬 루트는 `D:\AgentDesk\target\hub-runner-cutover\continuation`이다.

| 파일 | 내용 |
| --- | --- |
| `before-audit.json`, `after-audit.json` | 역할·빌드·schema·health·에이전트/세션 checksum |
| `discord-cutover-proof.json`, `discord-cutover-cleanup.log` | 실제 왕복·장비 선택·테스트 자료 정리 |
| `live-dashboard.json`, `live-dashboard-{desktop,mobile}.png` | 실제 서버 화면·오류·갱신 검사 |
| `node-terminology-served-dashboard.json` | 배포 패키지와 제공 자산 비교 |
| `focused-tests.json`, `test-runner-final.log` | 로컬 Rust 77개 결과 |
| `ci-library-latest.log`, `ci-postgres-latest.log`, `ci-script-latest.log` | CI 개별 job 원문과 최초 실패 |
| `rust-build-reclaimed.log` | 성공한 로컬 Rust 빌드 |
| `windows-trust-config.json` | 신뢰 설정의 장비 키 교체·다른 값 보존 |

Mac 추가 증거는 `/Users/kunkun/.adk/validation/hub-runner-cutover-20260923`의
`signing-correction.json`, `trust-config-receipt.json`, `backups/eedf46d36`에 있다.

실제 하드웨어 왕복은 Windows와 Mac mini의 Codex만 검증했다. Linux는 네이티브 빌드,
Mac Studio 추가와 Hub/DB 이동은 코드·문서 계약까지 검증했으며 해당 장비로 실제
이동하지 않았다. 모든 provider의 로그인, quota, 실제 호출 성공까지 보증하지 않는다.

기존 운영 로그에는 `scheduled external delivery claim requires consumer capability v1`
경고가 있었다. 최초 관측은 2026-09-22 02:51:58 UTC로 이 전환보다 앞선다. 운영 DB의
별도 consumer guard가 거절하는 상태이며 Hub/Runner 전환의 성공 항목에 포함하지 않는다.
해당 guard를 제거하거나 capability를 허위로 선언하지 않았다.
