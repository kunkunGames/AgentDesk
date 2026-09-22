# Hub/Runner 설정과 운영 용어 통일

기준일: 2026-09-23 KST. 용어와 호환 계약은
[공통 안내](../operations/node-terminology.md)를 따른다.

## 변경

| 구분 | 정식 설정 값 | 화면 표시 | 이전 입력 |
| --- | --- | --- | --- |
| 중앙 운영 역할 | `role: hub` | 허브 / Hub | `role: leader` |
| 참여 장비 역할 | `role: runner` | 실행 노드 / Runner | `role: worker` |
| 전체 기능 모드 | `runtime_profile: full` | 전체 기능 / Full features | 동일 |
| 실행 전용 모드 | `runtime_profile: runner` | 실행 전용 / Execution only | `runtime_profile: worker` |

`ClusterRole`로 파싱·검증·직렬화를 한곳에 모았다. 이전 설정은 계속 읽으며
직렬화 결과는 새 값이다. 잘못된 역할을 자동 선출 대상으로 해석하지 않는다.
공유 설정과 에이전트 목록의 동기화는 단독 운영 또는 명시적인 허브만 수행한다.
실행 전용 기능 모드는 활성 클러스터·명시적인 실행 노드·활성 intake routing을 요구한다.

운영 화면과 CLI는 현재 역할을 기준으로 이름을 표시한다. 역할과 기능 모드를
분리하므로 전체 기능 모드의 실행 노드를 허브로 잘못 표시하지 않는다.
에이전트 설정은 **우선 실행 장비 / 기본 정책 사용**으로 통일했다.
`cluster.enabled=false`일 때 **이 컴퓨터 · 단독 운영**을 표시하고 원격 제어를 숨긴다.

Windows 방화벽 설치 매개변수는 `-HubAddress`이며 `-LeaderAddress`도 별칭으로 지원한다.
기존 예약 작업·방화벽 규칙·노드 ID는 유지한다. 기존 세션의 소유권이나 배정 정책을
새 이름에 맞춰 일괄 변경하지 않는다.

## 혼합 버전 호환

공유 레지스트리의 역할 값과 schema-1 실행 준비 정보는 구버전 노드가 읽을 수 있게
기존 wire 값을 유지한다. 새 코드의 타입과 설정 값은 Hub/Runner를 사용하며 이 변환은
레지스트리 입출력 경계에 모은다. 기존 health의 `modules.leader_services` 키도 유지한다.
설정 API의 직렬화와 `/api/health.runtime_profile`은 새 값을 사용한다.

이름 변경에 DB migration을 추가하지 않았다. 배포 후보는 기존 운영 코드
`9af8c7863` 위에 용어 변경만 적용한 `7bfc24c64566b276fc563a0db01411532629687f`다.
로컬 `main`에 별도로 병합된 campaign retention migration 128은 이 후보에 포함하지 않았다.
후보 빌드는 [Release 실행](https://github.com/kunkunGames/AgentDesk/actions/runs/35745913270)에서
macOS ARM64, Windows x64, Linux x64가 모두 통과했다. 각 OS에서 허브와 실행 노드가
같은 실행 파일을 사용한다. 공개 Release 발행은 수행하지 않았다.

## 검증 기록

- 대시보드 단위 테스트: 61개 파일, 390개 테스트 통과.
- 대시보드 TypeScript·Vite 빌드 통과, npm audit 취약점 0개.
- Playwright: 데스크톱·모바일 4개 통과. 한국어·영어, 신·구 역할 이름,
  장비 선택 저장·재진입·기본값 복원, 실행 노드의 두 기능 모드, 만료 상태 제어 차단,
  단독 운영 표시와 제어 숨김을 확인했다.
- Windows 방화벽 스크립트 파서와 이전 매개변수 별칭 확인 통과.
- 설정 변환 16개 사례에서 주석·줄바꿈·기존 ID·다른 설정 보존 확인.
- 변경 문서의 상대 링크 130개 확인.
- 첫 후보의 CLI 참조 타입 오류는 배포 빌드에서 검출해 수정했다. 해당 실패 후보는 배포하지 않았다.
- 배포 후보와 동일한 Rust 소스의 집중 회귀 검사 50개 검증 완료. 설정 별칭·정규화,
  단독 운영 기본값, 혼합 버전 실행 준비 정보, 허브 설정 소유권, 실행 전용 API 인증과
  관리 API 차단, 중앙 작업의 시작 조건, 장비 배정·필수 조건·용량·lease·중복 실행을 검사했다.

다중 노드 검사 중 1개는 기능 assertion 전에 임시 DB의 127개 마이그레이션을 적용하는
과정에서 15초 제한을 두 번 초과했다. 검사 대상은 운영 5432가 아닌 SSH로 연결한
전용 15432 서버였고 잠금 경합은 없었다. 해당 임시 서버의 `agentdesk_test` 역할에만
`synchronous_commit=off`를 잠시 적용해 같은 후보·같은 15초 제한·같은 assertion으로
실행했고 통과했다. `fsync`와 운영 DB 설정은 변경하지 않았으며, 검사 후 역할 설정을
원복하고 `synchronous_commit=on`, 별도 역할 override 없음까지 확인했다.
이 결과를 기본 테스트 환경의 무조건적인 안정성 증거로 확대하지 않는다.
최초 실패와 재검증 기록은 별도로 보존했다.

배포 전 두 장비는 `healthy`, `fully_recovered=true`, `db=true`였고,
에이전트 23개와 스키마 127을 확인했다. 운영 설정의 startup timeout은 모두 300초였다.

## 실기기 배포

두 장비 모두 `7bfc24c64566b276fc563a0db01411532629687f`의 네이티브 패키지를 검증한 뒤
배포했다. Mac은 기존 표준 배포 절차의 작업 종료 대기·DB 검사·재시작·API smoke를
거쳤고, Windows는 처리 중 작업과 대기열이 비어 있는 상태에서 예약 작업을 재시작했다.

| 장비 | 완료 시각(KST) | 실제 설정 | Health | DB |
| --- | --- | --- | --- | --- |
| Mac mini | 2026-09-23 00:53:30 | `hub` / `full` | `healthy`, `fully_recovered=true` | 연결 정상 |
| Windows PC | 2026-09-23 00:55:52 | `runner` / `runner` | `healthy`, `fully_recovered=true` | 연결 정상 |

00:56:15 KST 배포 후 감사에서 두 장비가 같은 후보로 온라인이며, Windows의 Codex 실행
가능 상태와 허브에서 실행 노드로의 연결 검증이 완료됐음을 확인했다.
기존 에이전트 23개의 전체 행 checksum은 배포 전후
`3a34d55f1b525aa0dc7673ad7885e81e`로 같았고, DB 스키마 127과 각 장비의 300초
startup timeout도 유지됐다. 코드의 timeout 기본값은 기존 60초다.

Mac만 먼저 업데이트된 중간 상태에서도 새 허브와 구버전 Windows 노드가 모두
`healthy`이고 Windows의 실행 준비·연결 검증이 정상임을 확인했다.
이는 설정 별칭 단위 테스트와 별개인 실제 혼합 버전 운영 확인이다.

운영 허브가 HTTP로 제공한 대시보드 HTML과 직접 참조하는 JS/CSS 14개는 후보 패키지의
파일과 바이트 단위로 일치했다. Windows는 실행 전용 기능 모드로 운영한다.
Mac의 API smoke에서 Claude 계정 API는 기존 Claude 런타임 미설치로 선택 항목 처리됐다.
기존 에이전트 프롬프트 3개의 `/api/send` 안내 경고는 배포를 막지 않았으며,
이번 용어 변경의 기능 검사 결과와 구분한다.

배포된 실행 파일 SHA-256:

- Mac: `912a674166e28dd1b93fc3af57d5204c4a284ddf1e3f1ca254b68fa128ccf250`.
  표준 배포 과정에서 로컬 ad-hoc 서명을 적용한 파일이다.
- Windows: `3b13029e96c9b8e5ca37e5a7b0a1357984f1c3718e45ad2318db5a2913338b9e`.

설정 백업은 Mac의 내부 검증 디렉터리
`/Users/kunkun/.adk/validation/heterogeneous-worker-20260922/backups/node-terminology-before-7bfc24c64/`와
Windows의 `%USERPROFILE%\.adk\release\backups\node-terminology-before-7bfc24c64\`에 있다.
Windows 바이너리 백업은 같은 `backups` 아래 `node-terminology-binary-before-7bfc24c64/`다.

## Discord 왕복 검증

전용 스레드 `hub-runner-terminology-validation-20260923`에서 임시 에이전트의 우선 실행
장비를 Windows로 지정하고, PowerShell로 고유 문자열과 OS 플랫폼을 출력하도록 한 번
요청했다. [실제 Discord 응답](https://discord.com/channels/1469509996621594686/1551985516407164948/1551985598586163201)에서
고유 문자열과 `Win32NT`를 확인했다.

- 입력 메시지: `1551985526729474099`; 응답 메시지: `1551985598586163201`.
- 세션 소유자: `windows-worker-1`; 출력 backend: `process`.
- 중앙 intake 기록: 허브 `single-node`가 Windows로 전달, `status=done`,
  `attempt_no=1`, `retry_count=0`, `last_error IS NULL`.
- 응답은 한 개였고 올바른 Discord 원문 링크를 포함했다.
  `headless_turn` 경로의 중복 발송 기록은 없었다.
- 우선 장비 저장·기본값 복원·재지정과 미등록 장비 거부도 실제 API에서 확인했다.
- 임시 세션과 에이전트를 제거하고 테스트 스레드를 보관 처리했다.
  01:02:16 KST 최종 감사에서도 기존 에이전트 23개의 checksum, 두 장비의 health,
  스키마 127, 각 300초 설정이 유지됐고 활성 작업·마감 중 작업·대기열은 모두 0이었다.

### 확인된 별도 제한

왕복 실행은 통과했지만, 정리 단계의 `force-kill` API는 이미 유휴 상태인 Windows
ProcessBackend wrapper를 종료하지 못했다. 세션 DB 상태는 `disconnected`로 바뀌었으나
출력 API의 `alive=true`와 실제 wrapper PID가 유지됐다. 기존 종료 경로의 tmux 존재
검사가 native process registry를 보지 않는 코드 경계를 확인했다. 관련 파일은 운영
기준 커밋 `9af8c7863`에서 이번 후보까지 변경되지 않았다.

검증용 wrapper에 활성 provider 자식이 없음을 확인한 후 실행 파일 경로·부모 PID·전용
스레드 ID가 일치하는 프로세스 한 개만 OS에서 종료했다. `alive=false`를 확인한 다음
정상 API로 테스트 세션과 에이전트를 제거했다. 이 수동 테스트 정리를 종료 API의
기능 통과로 계산하지 않으며, 해당 기존 종료 API 문제는 이번 용어 변경에서 수정하지 않았다.

Linux는 네이티브 패키지 빌드까지 검증했다. 실제 Linux 장비의 Discord 왕복 실행이나
세 가지 OS의 모든 장애 조합을 검증했다는 의미는 아니다. 기존 단독 운영은 기본 설정과
브라우저 회귀 검사로 확인했으며, 이 배포의 실기기 구성은 Mac 허브와 Windows 실행 노드다.

## 증거 위치

로컬 검증 자료는 Git 추적 대상이 아닌 `target/heterogeneous-worker-validation/`의
`node-terminology-*` 파일에 저장한다. 빌드 로그, 정확한 후보의 Rust 회귀 결과,
설정·에이전트 checksum·노드 health의 배포 전후 비교를 별도로 보존한다.
브라우저 스크린샷은 `target/node-terminology-playwright/`에 있다.

핵심 결과 파일은 `node-terminology-focused-tests.json`, `node-terminology-release-ci.json`,
`node-terminology-mac-deployment.json`, `node-terminology-windows-deployment.json`,
`node-terminology-mixed-versions.json`, `node-terminology-served-dashboard.json`,
`node-terminology-after-audit.json`, `node-terminology-discord-proof.json`이다.
