# StreamJson 초기 출력 대기 시간 설정 및 배포

## 변경 계약

설정 키는 `runtime.stream_json_startup_output_timeout_secs`다. 프로그램 기본값은
60초를 유지하고, Mac mini leader와 Windows worker의 운영 설정에서 300초를
명시한다. 코드 commit은 `9af8c78638f726ee82693116eddaaf9d5f3d6c13`이다.

```yaml
runtime:
  stream_json_startup_output_timeout_secs: 300
```

- 설정 생략 또는 `0`: 60초.
- 양수 설정: 해당 초 수를 사용하며 최대 86,400초로 제한한다.
- 호출자가 별도로 `Duration::ZERO`를 전달하는 기존 경로: 90초 유지.
- Grok/AGY 공통 StreamJson runner가 실행을 시작할 때 live config snapshot을 읽는다.
- 첫 비어 있지 않은 stdout 줄을 기다리는 시간에 적용한다. 첫 출력 이후의 전체 실행 시간 제한은 추가하지 않는다.

Discord의 StreamJson 요청은 0이 아닌 호출 timeout을 전달하므로 운영 설정의
300초가 적용되는 경로다. 설정 파일 watcher가 활성화돼 있으면 변경한 값은
reload 이후 새 실행부터 반영된다. 이미 실행 중인 요청의 시작 시점에 계산한
timeout은 바꾸지 않는다. 세부 계약은 [StreamJson 문서](../stream-json-error-contract.md)를 참고한다.

## 검증

`cargo test --lib services::stream_json_cli -- --test-threads=1` 결과는
**34 passed, 0 failed**다. 기본값, `0`, 명시적 300초, 최대값 제한,
기존 90초 경로와 YAML 역직렬화를 포함한다. Grok/AGY codec, session,
policy 및 stderr 처리 검사도 같은 실행에서 통과했다.

`cargo fmt --all -- --check`, test target integrity, test lane coverage,
SQL execution surface inventory의 baseline 비교와 `git diff --check`를 확인했다.
설정 파일 수정 도구는 block/flow mapping, 빈 설정, 기존 값과 주석, CRLF를 포함한
6개 입력 및 두 머신의 실제 설정에 대한 변경 전 검사를 통과했다.

Native build는 [Release CI 35717509806](https://github.com/kunkunGames/AgentDesk/actions/runs/35717509806)에서
`fix/stream-json-startup-timeout-config`의 위 commit으로 실행했다. Windows x86_64,
macOS ARM64, Linux x86_64의 build와 native wrapper 검증이 모두 통과했다.
`publish=false`로 지정해 publish job은 skipped이며, Mac·Windows 배포 archive의
commit, `release-fast` build profile 및 파일 hash를 확인했다.

## 운영 반영 상태

**배포 완료 및 최종 감사: 2026-09-22 20:29 KST.**

| 장비 | 역할 / 프로필 | 운영 설정 | 실행 commit |
| --- | --- | --- | --- |
| Mac mini `single-node` | leader / full | 300초 | `9af8c7863` |
| Windows `windows-worker-1` | worker / worker | 300초 | `9af8c7863` |

운영 설정 파일은 Mac의 `/Users/kunkun/.adk/release/config/agentdesk.yaml`과
Windows의 `C:\Users\12336\.adk\release\config\agentdesk.yaml`이다.
양쪽 모두 수정 전후 YAML을 비교해 요청한 runtime 항목만 바뀐 것을 확인했다.
Mac은 표준 release 배포 절차의 migration 검사, turn drain, launchd 재기동과
API smoke를 통과했다. Windows는 대기 작업이 없는 상태에서 원본 실행 파일과
metadata를 백업하고 예약 작업을 재기동했다. 배포 패키지의 변경된 예제 YAML도 반영했다.

최종 API 관측에서 두 노드는 `online`, `healthy`, `fully_recovered=true`,
`db=true`이고 active/finalizing/queue 및 pending/retrying dispatch가 모두 0이었다.
Mac이 관측한 Windows의 Codex 실행 readiness와 forwarding reachability도 정상이다.
DB schema는 127, 기존 에이전트는 23개이고 전체 행 checksum은
`3a34d55f1b525aa0dc7673ad7885e81e`로 배포 전과 같다. 기본 실행 노드를
명시한 기존 에이전트 수도 계속 0이다.

이번 배포 검증 범위는 관련 Rust 검사, 실제 운영 설정, native 패키지, API·DB와
노드 실행 상태다. Mac 배포 smoke는 `api` 범위로 실행했고 Discord에 새 테스트
메시지를 보내거나 실제 provider의 무출력 상태를 300초 동안 유지하는 실험은
수행하지 않았다. 이전 leader/worker Discord 검증은
[별도 보고서](worker-edge-cases-2026-09-22.md)에 기록돼 있다.

## 설정 백업과 배포 중 조정

원본 설정 백업 디렉터리는 다음과 같다.

- Mac: `/Users/kunkun/.adk/validation/heterogeneous-worker-20260922/backups/startup-timeout-config-before-9af8c7863/`
- Windows: `C:\Users\12336\.adk\release\backups\startup-timeout-config-before-9af8c7863\`

Mac의 기존 `release/backups`가 외장 `990PRO`로 연결된 symlink여서 첫 준비 시도의
디렉터리 생성이 지연됐다. 이 시도에서는 서비스와 운영 설정을 변경하기 전에
해당 배포 helper만 중지했다. 설정 백업 위치를 내부 validation 디렉터리로
명시한 뒤 배포를 완료했다. 기존 symlink와 외장 디스크 구성은 변경하지 않았다.

검증 원본은 Git 추적 대상이 아닌 `target/heterogeneous-worker-validation/`의
`startup-timeout-tests.json`, `startup-timeout-static-checks.json`,
`startup-timeout-config-render-checks.json`, `startup-timeout-ci-result.json`,
`startup-timeout-native-artifacts.json`, `startup-timeout-mac-deployment.json`,
`startup-timeout-windows-deployment.json`, `startup-timeout-before-audit.json`,
`startup-timeout-after-audit.json` 및 대응 로그에 보관한다.
