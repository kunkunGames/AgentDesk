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
`fix/stream-json-startup-timeout-config`의 위 commit으로 실행한다. `publish=false`로
지정했으며 배포 archive의 commit, build profile 및 파일 hash를 확인한다.

## 운영 반영 상태

2026-09-22 20:00 KST 기준 native artifact를 기다리는 중이며, 아직 운영 설정과
실행 파일은 교체하지 않았다. 이 절은 실제 배포 및 최종 상태 확인 후 갱신한다.

배포 전에는 두 노드 모두 `healthy`, `fully_recovered=true`, `db=true`이고
active/finalizing/queue 및 pending/retrying dispatch가 모두 0이었다.
기존 에이전트 23개 전체 행의 checksum은 `3a34d55f1b525aa0dc7673ad7885e81e`,
DB schema는 127이며, 두 운영 설정 모두 새 timeout 키는 없었다.

검증 원본은 Git 추적 대상이 아닌 `target/heterogeneous-worker-validation/`의
`startup-timeout-tests.json`, `startup-timeout-config-render-checks.json`,
`startup-timeout-before-audit.json` 및 대응 로그에 보관한다.
