# 검증 재현 명령과 운영 경계

아래 명령은 source checkout에서 실행한다. 실제 실행한 개발 검증과 아직
검증하지 않은 배포/운영 절차를 구분한다. 운영 서비스의 start/pause/cancel/rollback
명령은 대상 호스트와 지원 범위를 확인해 적용한다. Mac 기존 Routine의 실제 절차는
[Mac 운영 기록](MAC-MINI-OPERATIONS-2026-09-28.md)을 따른다. managed v1 전체 인수와 구분한다.

아래 기존 76개 Rust/158개 정책 결과와 `BUILD-IDENTITY.json`은 W1 migration 0131
추가 전 snapshot의 기록이다. 이후 작업본의 테스트 결과나 배포 binary로 대체하지 않는다.
OpenCode 전용 운영을 가정하지 않으며, 실제 운영 provider/profile의 인수는
[배포 계획](DEPLOYMENT-PLAN.md)을 따른다.

## 운영 감사 재현

최신 판정과 소스/바이너리 대응은 [OPERATING-READINESS.md](OPERATING-READINESS.md)와
`target/24x7-native/resume-20260928-acceptance-v9/OPERATING-READINESS-RESULT.json`을 따른다. 아래 Rust 전체 필터는
현재 작업본에서 그대로 사용할 수 있지만, 과거 source capture의 `--verify`는 해당
snapshot에만 적용한다. 파일이 바뀌면 새 capture를 만들고 이전 결과를 덮어쓰지 않는다.
v9의 1,680개 회귀와 이전 snapshot의 정확한 필터는 각 receipt의
`rust.build_and_test.command`에 있다. 과거 수를 최신 실행 수에 합산하지 않는다.
기존 provider/Routine 경로에 `services::discord::turn_bridge::` 전체와
`services::discord::inflight::save_store::identity_gate::runtime_stamp::`를 포함했다.
아래 기능별 명령과 실행 수는 각 제목에 기록한 이전 snapshot의 예시다.

프로세스 수거 회귀는 정상적인 init/subreaper가 있는 환경에서 실행한다. 재사용 빌드
container의 PID 1은 `sleep`이라 종료된 orphan이 zombie로 남았다. 프로세스 그룹의
`Gone` 검사를 완화하지 않고, lib test executable을 `/usr/bin/tini -s -- <test-binary>`로
실행해 실제 수거를 보장했다. Qwen wrapper 검사는 실제 `tmux`도 요구한다. 두 도구의
설치는 검증 container에 한정하며 운영 Mac 서비스의 실행 구조를 바꾸지 않는다.

소스·바이너리·증거의 일관성 감사는 다음과 같이 실행한다. 이 명령은 과거 테스트를
다시 실행하지 않으며, fixture를 실제 모델/E1/E4/E5/24시간 인수로 승격하지 않는다.

```bash
python3 -m unittest tests.test_activation_24x7_gate
python3 scripts/activation_24x7_gate.py --audit-only \
  --receipt target/24x7-native/resume-20260928-acceptance-v9/OPERATING-READINESS-RESULT.json \
  --output target/24x7-native/new-audit/result.json
```

`--output` 대상이 이미 있으면 실패한다. `--audit-only`를 빼면 현재 운영 요건 미충족에
대해 exit 2이며 증거 자체가 틀리면 exit 1이다. 이 오프라인 명령은 서비스 설정을 바꾸지 않는다.
기존 `--measurement` 입력은 지원하지 않는다. JSON의 시간·모델 호출 플래그만으로
실제 운영 인수를 인증할 수 없다.

보호된 Campaign의 실행·파일 전달·자동 제한 재작업은 다음 canary로 검사한다.
실제 모델의 구현 결과는 fixture로 주입하며, 공급·검사·완료·재시작은 실제 Hub가 수행한다.
보호된 registry가 없는 일반 Campaign은 의도적으로 실행하지 않는다. 과거
`probe_campaign_supply.py`의 미연결 반례는 이전 binary에 대한 역사적 증거다.

```bash
python3 scripts/probe_campaign_execution.py \
  --binary /target/debug/agentdesk \
  --artifact-dir target/24x7-native/new-campaign-execution
```

설정·권한·입력 manifest·지원 범위는 [CAMPAIGN-EXECUTION.md](CAMPAIGN-EXECUTION.md)를 따른다.

실제 checker의 운영 반례는 WSL checkout에서 다음과 같이 별도 실행한다. 새 artifact
경로를 사용하며 기존 결과를 삭제하지 않는다. 모든 경우 구현 완료 입력은 fixture다.

```bash
python3 scripts/probe_managed_checks_operations.py --scenario starvation \
  --artifact-dir target/24x7-native/new-starvation
python3 scripts/probe_managed_checks_operations.py --scenario rejected \
  --artifact-dir target/24x7-native/new-rejected
python3 scripts/probe_managed_checks_operations.py --scenario crash \
  --artifact-dir target/24x7-native/new-crash
python3 scripts/probe_managed_checks_operations.py --scenario failure \
  --artifact-dir target/24x7-native/new-failure
```

`--binary`로 정확히 검증할 container 내부 실행 파일을 지정할 수 있다. host가 새 DB만
소유하고 제거하며, crash 시험은 Hub만 강제 종료해 checker의 parent-death 경계를 검사한다.
추론 모델/채널/routine은 구성하지 않는다. 결과는 실제 모델 또는 OS 서비스 재부팅 인수가 아니다.

## 이전 W2 보호 검사와 실제 완료

계약·설정·API는 [MANAGED-CHECKS.md](MANAGED-CHECKS.md)를 따른다. 입력은
`target/24x7-native/MANAGED-CHECKS-VERIFIED-SOURCE.json`의 1,965개 파일이다. 해당 snapshot의
검증은 **Rust 369개 / 정책 161개 / 실제 checker HTTP 9개 조건 / 저장 API HTTP 6개 조건**이다.
처음 351개 회귀에서 발견한 스트림 테스트 clock 경쟁은 첫 출력 수신에 가상 시간을 맞추어
해결했고, 최종 회귀와 24회 반복 실행에서 확인했다. 과거 실행 수와 합산하지 않는다.

아래 명령은 WSL의 해당 source checkout에서 실행한다. test fixture는 전용 root Linux
container와 loopback PostgreSQL 44620이며 실제 provider/model/메시징 설정이 없다.
native 테스트에 필요한 Python3/Landlock과 HTTP fixture용 `libpq5`가 설치돼 있어야 한다.

```bash
docker exec --env CARGO_INCREMENTAL=1 \
  --env POSTGRES_TEST_DATABASE_URL_BASE=postgres://postgres:postgres@127.0.0.1:44620 \
  --env AGENTDESK_REQUIRE_PG=1 \
  --env AGENTDESK_TEST_OPENCODE_BIN=/target/24x7-tools/opencode-1.14.41 \
  adk24x7-native-build-20260926 \
  python3 scripts/build_token.py cargo test --locked --lib -- \
  services::opencode:: services::platform::managed_process:: \
  services::provider_auth_profile:: services::cluster::node_registry:: \
  services::cluster::execution_capacity:: services::cluster::execution_requirements:: \
  services::cluster::capability_routing:: services::cluster::intake_router_hook:: \
  db::intake_outbox:: services::provider::provider_conformance_invariant_tests:: \
  services::discord::router::message_handler::provider_dispatch:: \
  services::stream_json_cli:: services::discord::settings:: \
  server::test_phase_runs:: server::resource_locks:: db::campaigns:: \
  server::campaign_execution:: dispatch:: services::auto_queue:: db::auto_queue:: \
  services::auto_queue::route::phase_gate_catalog:: engine::transition:: phase_gate:: \
  engine::ops::kanban_ops::tests \
  reconcile::dispatch_delivery_reconcile_tests::review_candidates \
  --include-ignored --test-threads=1
docker exec --env CARGO_INCREMENTAL=1 adk24x7-native-build-20260926 \
  python3 scripts/build_token.py cargo build --locked --bin agentdesk
python3 scripts/probe_managed_checks.py \
  --artifact-dir target/24x7-native/managed-checks-http-new
python3 scripts/probe_test_phase_lifecycle.py \
  --artifact-dir target/24x7-native/managed-checks-storage-http-new
python3 target/24x7-native/capture-managed-checks-source.py --verify
```

probe의 `--artifact-dir`는 아직 없는 경로여야 한다. 실제 checker HTTP는 구현 dispatch만
fixture로 주입한다. 후보 동결·검사 실행·영수증·FSM 완료·현재 후보의 사람 승인·재시작은
실제 서비스가 수행한다. 최신 결과는 `managed-checks-http-v1/`에 있고 9개 조건,
기동 2회, 98.583초다. 기존 저장 API probe는 `managed-checks-storage-http/`에 있으며
6개 조건, 기동 2회, 5.236초다. 두 probe 모두 생성한 DB와 Hub를 정리했다.

검증한 실행 파일은 `bin/agentdesk-linux-x86_64-managed-checks`이고 SHA-256은
`0d995bfe00739249b84152948a0d05a5ef8b0549051444730ba64832a8fa21da`다. artifact 경로는
`target/24x7-native` 기준이며 `MANAGED-CHECKS-RESULT.json`이 source와 로그를 연결한다.
이는 설치/운영 배포 또는 W3 A/B/C의 완료 증거가 아니다.

## 검사 저장 경계와 실제 HTTP 검증

아래는 보호 검사 adapter 연결 전 `PHASE-RUN-LIFECYCLE-SOURCE.json`에 기록한 1,897개 입력의 검증이다.
집중 회귀 19개를 포함한 **303 passed / 0 failed / 0 ignored**이며, 이전 281개 snapshot과
합산하지 않는다. 실제 check driver나 FSM 성공 확정의 완료 증거는 아니다.

기존 Linux container에서 집중 회귀를 재현하는 명령이다. 다음 명령은 WSL Ubuntu의
source checkout에서 실행한다. 전체 회귀는 아래 고정 노드 ID 회귀 필터에
`server::test_phase_runs:: server::resource_locks:: db::campaigns::`
및 `services::auto_queue::route::phase_gate_catalog::`를 추가한다.

```bash
docker exec --env CARGO_INCREMENTAL=1 \
  --env POSTGRES_TEST_DATABASE_URL_BASE=postgres://postgres:postgres@127.0.0.1:44620 \
  --env AGENTDESK_REQUIRE_PG=1 adk24x7-native-build-20260926 \
  python3 scripts/build_token.py cargo test --locked --lib -- \
  server::test_phase_runs:: server::resource_locks:: db::campaigns:: --test-threads=1
docker exec --env CARGO_INCREMENTAL=1 adk24x7-native-build-20260926 \
  python3 scripts/build_token.py cargo build --locked --bin agentdesk
python3 scripts/probe_test_phase_lifecycle.py \
  --artifact-dir target/24x7-native/phase-run-http-new
```

`--artifact-dir`는 아직 존재하지 않는 경로를 지정한다. probe는 알려진 loopback
44620 fixture container를 확인한 뒤 새 DB만 생성/삭제한다. 새 DB 초기화는 기존
Hub 전용 계약을 따르며 채널·실행기·정책·routine 없는 Hub를 임시 HOME/root/설정으로
기동한다. host 환경의 credential은 서비스에 전달하지 않고 임시 인증값은 출력하지 않는다.
서비스 시작부터 검사까지 210초 제한을 두고 소유한 프로세스를 종료한 뒤 임시 설정을 제거한다.
다른 서비스의 root/DB를 가리키거나 운영 설정을 복사하지 않는다.

현재 결과는 `phase-run-lifecycle-http-canary-hub/`: 6개 조건 통과, 기동 2회,
5.245초다. 인증, 중복 시작, stale owner, immutable receipt, 재시작 후 보존을 검증한다.
evidence는 합성값이며 실제 프로젝트 검사 결과가 아니다. 최초 Runner의 빈 DB 초기화
실패는 `phase-run-lifecycle-http-canary/`에 보존했고 두 실행 모두 임시 DB 정리를 확인했다.

이전 저장 경계 snapshot의 Linux 실행 파일은 `bin/agentdesk-linux-x86_64-phase-run-lifecycle`에 보존했다.
build 7분 12초, `--version`/`--help` exit 0, SHA-256
`97d1fafd0a5a88ec95dfc9d7080f058757b16cba6afc0a0447b3c57aed75a293`이다.
`PHASE-RUN-LIFECYCLE-RESULT.json`이 현재 source와 이 binary/검증의 관계를 기록한다.

## 이전 W0/W1 실행 파일

281개 회귀를 통과한 Rust/migration snapshot에서 다음 application build를 실행했다.
이전 binary와 같은 버전 문자열을 사용하므로 SHA-256과 source fingerprint로 구분한다.

```powershell
wsl -d Ubuntu --exec docker exec --env CARGO_INCREMENTAL=1 `
  adk24x7-native-build-20260926 `
  python3 scripts/build_token.py cargo build --locked --bin agentdesk
python target/24x7-native/record-current-application-build.py
```

빌드 **14분 23초, exit 0**. 기록 script는 고정된 1,892개 입력을 대조하고 `--version`/
`--help`를 실행한 뒤 별도 이름으로 binary를 보존한다. 기존 동일 이름의 artifact가 있으면
덮어쓰지 않고 현재 생성 파일과 hash가 같은지 확인한다. Cargo dependency의 추가 입력은
빌드 후 관측한 hash로 구분한다. 결과는 `CURRENT-APPLICATION-BUILD-RESULT.json`이다.
application 서비스 설치나 시작은 수행하지 않는다.

## 실행 파일과 Rust 및 실제 PostgreSQL

다음은 초기 snapshot의 Linux dev binary build와 CLI smoke 기록이다. `cargo check`와 구분한다.
release bundle을 만들거나 기존 서비스에 설치한 명령은 아니다.

```powershell
wsl -d Ubuntu --exec docker exec adk24x7-native-build-20260926 `
  python3 scripts/build_token.py cargo build --locked --bin agentdesk
wsl -d Ubuntu --exec docker exec adk24x7-native-build-20260926 /target/debug/agentdesk --version
wsl -d Ubuntu --exec docker exec adk24x7-native-build-20260926 /target/debug/agentdesk --help
```

빌드와 두 CLI 호출 모두 exit 0. version은 `agentdesk 0.1.3`이다. 정확한 dirty
source와 실행 파일의 식별은 버전 문자열 대신 `BUILD-IDENTITY.json`의 digest를 사용한다.

검증한 Linux container는 Rust 1.94.1과 native dependencies를 사용한다.
container의 `/workspace`는 source read-only bind, `/target`은 별도 volume이다.
다음 environment는 **격리 fixture DB 전용**이며 production DB에 적용하지 않는다.
DB URL base 뒤에 `/postgres`를 붙이지 않는다. 테스트가 DB 이름을 붙인다.

```powershell
wsl -d Ubuntu --exec docker exec `
  --env POSTGRES_TEST_DATABASE_URL_BASE=postgres://postgres:postgres@127.0.0.1:44620 `
  --env AGENTDESK_REQUIRE_PG=1 `
  adk24x7-native-build-20260926 `
  python3 scripts/build_token.py cargo test --locked --lib -- `
  services::opencode:: db::campaigns:: server::resource_locks:: server::test_phase_runs:: `
  --test-threads=1
```

이 명령의 결과는 `50 passed; 0 failed; 0 ignored`였다.
container 생성 시 `CARGO_PROFILE_DEV_DEBUG=0`, `CARGO_PROFILE_TEST_DEBUG=0`,
`CARGO_BUILD_JOBS=1`, `CARGO_TARGET_DIR=/target`를 지정했다. Windows에서 global
Node/Rust 버전이 일치한다고 가정하지 않는다.

설정 기본값을 읽고 보존하는 소비자 회귀도 같은 test binary에서 별도로 실행했다.
다음 파일명은 이번 빌드 artifact의 실제 이름이며, 다른 빌드에서는 Cargo가 출력한
test binary 경로를 사용한다. 26개 통과했고 앞의 50개와 중복되지 않는다.

```powershell
wsl -d Ubuntu --exec docker exec `
  --env POSTGRES_TEST_DATABASE_URL_BASE=postgres://postgres:postgres@127.0.0.1:44620 `
  --env AGENTDESK_REQUIRE_PG=1 `
  adk24x7-native-build-20260926 /target/debug/deps/agentdesk-95d02490b2b7d0e3 `
  services::discord::settings:: services::provider_exec:: --test-threads=1
```

이 실행에서 `services::provider_exec::` 아래 일치하는 테스트는 0개였다.
26개는 모두 `services::discord::settings::` 회귀다.

## 여러 provider의 공통 경계 회귀

W1 DB 수명 계약과 registry 기반 Grok 경로 변경은 다음 별도 명령으로 검증한다.
기존 binary의 76개 결과와 합산해 서로 독립적인 테스트 수처럼 보고하지 않는다.

```powershell
wsl -d Ubuntu --exec docker exec `
  --env POSTGRES_TEST_DATABASE_URL_BASE=postgres://postgres:postgres@127.0.0.1:44620 `
  --env AGENTDESK_REQUIRE_PG=1 `
  adk24x7-native-build-20260926 `
  python3 scripts/build_token.py cargo test --locked --lib -- `
  services::opencode:: services::cluster::execution_capacity:: `
  services::cluster::execution_requirements:: `
  services::provider::provider_conformance_invariant_tests:: `
  services::discord::router::message_handler::provider_dispatch:: `
  services::stream_json_cli:: services::discord::settings:: --test-threads=1
```

`multi-provider-regression.log`은 첫 실행(110 pass, 4 fail),
`multi-provider-regression-corrected.log`은 SQL 별칭 충돌 수정 후 재실행 기록이다.
실패한 첫 기록을 성공 기록으로 덮어쓰지 않는다. DB의 provider key를
OpenCode/Codex/Grok/Qwen으로 바꾸는 시험은 공통 원장 계약 시험이며, 그 CLI로
실제 모델을 실행하는 인수 시험이 아니다. native adapter 연결과 실제 모델 검증은
별도로 남는다. migration checksum과 기존 migration의 불변성도 검사한다.

```powershell
python scripts/check_postgres_migration_checksums.py --base-ref HEAD
```

## 실제 native adapter와 PG 예산의 연결 시험

일반 테스트 환경에서 native CLI 설치를 강요하지 않도록 해당 시험 하나는 opt-in이다.
아래 명령은 `--include-ignored`를 명시하여 반드시 실행한다. skipped/ignored 상태를
native 연결 통과로 집계하지 않는다. 독립 native probe와 달리 production Rust launcher,
native HTTP/SSE adapter, 기존 execution guard와 실제 PostgreSQL을 함께 사용한다.

시험 binary는 격리 build container의 `/target/24x7-tools/opencode-1.14.41`이다.
`opencode-linux-x64@1.14.41` npm archive의 SHA-512 integrity를 확인하고 정확히
`package/bin/opencode`만 추출했다. `opencode-linux-package-receipt.json`을 보존한다.
실행본 SHA-256은 `d9d87ba7c6a01bbc2df785cf34848fc100fb505d6fb4fef60958d987f1e6e882`다.

```powershell
wsl -d Ubuntu --exec docker exec `
  --env POSTGRES_TEST_DATABASE_URL_BASE=postgres://postgres:postgres@127.0.0.1:44620 `
  --env AGENTDESK_REQUIRE_PG=1 `
  --env AGENTDESK_TEST_OPENCODE_BIN=/target/24x7-tools/opencode-1.14.41 `
  adk24x7-native-build-20260926 `
  python3 scripts/build_token.py cargo test --locked --lib -- `
  services::opencode:: services::cluster::execution_capacity:: `
  services::cluster::execution_requirements:: services::cluster::capability_routing:: `
  services::cluster::intake_router_hook:: db::intake_outbox:: `
  services::provider::provider_conformance_invariant_tests:: `
  services::discord::router::message_handler::provider_dispatch:: `
  services::stream_json_cli:: services::discord::settings:: `
  --include-ignored --test-threads=1 --nocapture
```

`read_edit`, `exhausted`, `quiet`의 실제 native 결과는 `MANAGED_NATIVE_EVIDENCE`
JSON 행으로 남는다. 정상 종료의 DB 반환, 도구 한도 초과의 unknown 보존,
조용한 모델의 deadline 취소를 검사한다. 모델은 loopback fixture다. 이 시험은
실제 모델 성능, Windows의 process containment, 재시작 후 unknown 해제나 24시간 인수가 아니다.

Linux 파일 격리 변경 후에는 위 필터에 `services::platform::managed_process::`와
`services::provider_auth_profile::`을 더한다. build container에만 `CARGO_INCREMENTAL=1`을
전달했으며 저장소의 기본 incremental 설정은 바꾸지 않았다. fixture가 자신의 임시
작업 폴더만 UID/GID 65534, 0700으로 준비하고, 합성 profile JSON만 0600으로 생성한다.
실제 사용자 workspace의 소유권을 바꾸는 설치 명령이 아니다. native `aliases` case는
symlink와 `/proc/self/environ`을 요청하여 실제 도구 오류와 외부 파일 불변을 검사한다.

새 source receipt는 `FILESYSTEM-CONTAINMENT-SOURCE.json`, log는
`filesystem-containment-regression.log`이다. 실행 후
`python target/24x7-native/capture-filesystem-source.py --verify`로 현재 입력의 일치를
검사한다. 별도 kernel harness는 부팅 의존성을 좁히기 위한 개발 artifact이며 production
실행 경로가 아니다. 서비스 배포 판정에는 실제 adapter/PG 시험과 일치하는 전체 binary가
추가로 필요하다.

이 후속 실행은 **265 passed / 0 failed / 0 ignored**, build 17분 05초, 시험
256.83초로 끝났다. `FILESYSTEM-CONTAINMENT-RESULT.json`에 결과와 hash를 기록했다.
네 native case는 정상 편집, 한도 초과 unknown, 3.349초 quiet 취소, alias 차단을 관찰했다.
alias case의 native terminal은 작업 성공/acceptance와 별개다. 테스트 종료 뒤 owned
native/build 자식이 없음을 확인했다. 실제 운영 서비스는 재시작하지 않았다.

`managed-integration-regression.log`는 새 provider 인자 전달 누락으로 실패한 최초
컴파일 기록이다. 확정된 E0061 뒤 해당 owned rustc만 중단했고 exit 101을 기록했다.
`managed-integration-regression-corrected.log`는 outbox 재배정 경로까지 수정한 재실행이다.
고정 소스는 각각 `MANAGED-INTEGRATION-SOURCE.json`과
`MANAGED-INTEGRATION-SOURCE-corrected.json`으로 구분한다.

## 고정 노드 ID와 종료 관측 회귀

위 Linux 파일 격리 회귀 필터에 `services::cluster::node_registry::`를 추가하여
ID precedence, hostname/PID fallback 거절, 실행 중 ID 변경 거절도 실행한다.
`services::cluster::execution_capacity::`에는 실제 `setsid` 자식 writer 시험이 포함된다.
이 시험은 disposable container의 자신이 만든 프로세스·임시 폴더만 UID 65532/65531로
실행하며, 부모가 끝난 뒤에도 자식이 쓰고 있음을 확인한다. 관측 중 독립 프로세스가
살아 있는지도 확인한다. `--include-ignored`와 Linux root fixture 조건이 필요하다.

```powershell
wsl -d Ubuntu --exec docker exec `
  --env CARGO_INCREMENTAL=1 `
  --env POSTGRES_TEST_DATABASE_URL_BASE=postgres://postgres:postgres@127.0.0.1:44620 `
  --env AGENTDESK_REQUIRE_PG=1 `
  --env AGENTDESK_TEST_OPENCODE_BIN=/target/24x7-tools/opencode-1.14.41 `
  adk24x7-native-build-20260926 `
  python3 scripts/build_token.py cargo test --locked --lib -- `
  services::opencode:: services::platform::managed_process:: `
  services::provider_auth_profile:: services::cluster::node_registry:: `
  services::cluster::execution_capacity:: services::cluster::execution_requirements:: `
  services::cluster::capability_routing:: services::cluster::intake_router_hook:: `
  db::intake_outbox:: services::provider::provider_conformance_invariant_tests:: `
  services::discord::router::message_handler::provider_dispatch:: `
  services::stream_json_cli:: services::discord::settings:: `
  --include-ignored --test-threads=1 --nocapture
```

입력은 `RUNTIME-OBSERVATION-SOURCE.json`, 실행 log는
`runtime-observation-regression.log`다. 시험 후
`python target/24x7-native/capture-runtime-source.py --verify`로 고정 입력과 대조한다.
native `exhausted`/`quiet` case의 종료 관측이 `not_observed`여도 원래 `unknown` row와
도구 예약이 남아 있어야 한다. CLI exit, `/proc`의 빈 목록, 다른 boot/PID namespace를
자동 lease 반환이나 backend 추론 종료 증거로 사용하지 않는다.

이 snapshot의 최종 결과는 **279 passed / 0 failed / 0 ignored**, build 16분 39초,
시험 276.84초다. `RUNTIME-OBSERVATION-RESULT.json`에 source/binary/log hash와
native 네 case의 결과를 묶었다. 종료 시 살아 있는 시험 자식은 0개였고, production
PID 37188의 경로·시작 시각도 그대로였다. container PID 1이 수거하지 않은 비실행
zombie는 lifecycle receipt에 별도 기록했다. 실제 runtime 교체·모델·24시간 시험은 아니다.

## 일반/managed OpenCode 버전 계약의 후속 회귀

같은 전체 필터로 두 permission fixture 회귀를 추가 실행한다. 일반 실행은 다른 버전
문자열에서 같은 API 규칙을 보존해야 하며, managed 실행은 검증하지 않은 버전을
config/session 조회 전에 거절해야 한다. 기존 ignored-session-permission 회귀도 유지하여
version pin 분리가 권한 무시 fallback이 되지 않게 한다.

입력은 `PERMISSION-COMPATIBILITY-SOURCE.json`, log는
`permission-compatibility-regression.log`로 앞의 279개 snapshot과 구분한다.
시험 후 `python target/24x7-native/capture-permission-source.py --verify`로 소스를
대조한다. 실제 native CLI는 1.14.41이며 fixture의 다른 version 문자열을 실제 release
인수 결과로 보고하지 않는다.

최종 결과는 **281 passed / 0 failed / 0 ignored**, build 8분 01초, 시험 254.18초다.
결과는 `PERMISSION-COMPATIBILITY-RESULT.json`에 기록했다. Node 정책, 서비스 실행 파일,
실제 모델 및 운영 24시간까지 새로 시험했다는 뜻은 아니다.

## Node 정책

`.nvmrc`의 22.22.0을 확인하고 받은 해당 Windows binary를 사용했다.

```powershell
$nodeExe = Join-Path $PWD 'target/24x7-native/toolchain/node-v22.22.0-win-x64/node.exe'
& $nodeExe --version
$policyTests = @(Get-ChildItem -LiteralPath policies/__tests__ -Filter '*.test.js' -File |
  ForEach-Object FullName)
& $nodeExe --test --test-reporter=spec @policyTests
```

158개 통과. PowerShell에서 디렉터리나 literal glob 하나를 Node test 파일 목록으로
전달하지 않는다.

## 실제 native probe

```powershell
$opencodeExe = Join-Path $env:APPDATA 'npm/node_modules/opencode-ai/node_modules/opencode-windows-x64/bin/opencode.exe'
python scripts/probe_opencode_native.py --binary $opencodeExe --expected-version 1.14.41 `
  --output target/24x7-native/opencode-native-probe.json
python scripts/probe_opencode_steps.py --binary $opencodeExe --expected-version 1.14.41 `
  --output target/24x7-native/opencode-steps-probe.json
$LASTEXITCODE
```

첫 probe는 native 권한의 허용/거절을 검사한다. 두 번째 probe는 native steps가
반복 모델을 강제로 제한하는지 검사하며, 현재 승인 binary에서는 **실패(exit 2)**가
기록된다. 실패를 숨기거나 expected-success 결과로 바꾸지 않는다. 두 probe 모두
자체 임시 서버·HOME·workspace만 생성하고 정리한다. 실제 provider credential과
production config를 읽지 않는다.

Linux의 linked-path 반례는 아래 명령으로 재현했다. script와 JSON은 로컬 검증 artifact다.
모든 대상은 생성한 합성 파일이며 기존 운영 파일을 사용하지 않는다. 프로세스 exit 0은
관찰 완료를 뜻하며 격리 합격을 뜻하지 않는다. 현재 결과는 symlink/hardlink 양쪽에서
workspace 밖 파일이 읽히고 수정된 **격리 실패 반례**다.

```powershell
wsl -d Ubuntu --exec docker exec adk24x7-native-build-20260926 `
  python3 target/24x7-native/probe-linked-paths.py
```

## 형식·저장소 필수 검사

```powershell
cargo fmt --all -- --check
git diff --check
python scripts/audit_maintainability.py --check --format json
python scripts/check_test_target_integrity.py --enforce
python scripts/check_pg_test_lane_membership.py --baseline-ref HEAD
```

테스트 이름을 추가한 경우에만 기존 inventory 생성기를 사용하고 diff를 검토한다.
새 테스트를 inventory에서 제외해 통과시키지 않는다.
PG 시험을 추가할 때는 baseline/allowlist를 늘리지 않고 아래 생성기를 필요한 순서대로
실행한다. 생성 옵션은 각각 별도 호출이다. 기존 manifest에 있는 파일에 시험만 추가했다면
뒤의 filter/path 출력은 변경되지 않을 수 있다.

```powershell
python scripts/check_test_target_integrity.py --write-lib-inventory-manifest
python scripts/check_pg_test_lane_membership.py --write-snapshots --manifest-only
python scripts/check_pg_test_lane_membership.py --write-non-pg-filter
python scripts/check_pg_test_lane_membership.py --write-pg-db-paths
python scripts/check_pg_test_lane_membership.py --baseline-ref HEAD
```

후속 admission 호환성 회귀는 같은 232개 범위에 thread/headless 정책 binding 및
legacy remote workspace 회귀를 추가한 것이다. `ADMISSION-COMPATIBILITY-SOURCE.json`과
`admission-compatibility-regression.log`로 이전 snapshot과 구분한다. 이 재사용 빌드 container의
후속 컴파일에는 `docker exec --env CARGO_INCREMENTAL=1`을 사용했다. 저장소의 Cargo 기본값,
CI 설정과 sccache 설정은 바꾸지 않았다.
첫 실행에서 outbox 경쟁 시험의 잠금 관찰 쿼리가 source 선택에 간섭하는 실패를 검출했다.
`pg_blocking_pids`로 바꾼 뒤 같은 범위를 재검증했고 **234 passed / 0 failed / 0 ignored**를
확인했다. `-corrected` source/log와 `ADMISSION-COMPATIBILITY-RESULT.json`으로 구분한다.

## Initializer 재실행

```powershell
$scaffoldRoot = Join-Path $PWD 'target/24x7-native/starter-scaffold'
python scripts/operator-init-portable.py --root $scaffoldRoot --dry-run
python scripts/operator-init-portable.py --root $scaffoldRoot
python scripts/operator-init-portable.py --root $scaffoldRoot
```

처음 10개 항목 생성, 재실행 생성 0개와 기존 파일 4개의 hash 불변을 확인했다.
`--force`는 사용하지 않았다. starter config는 24/7용으로 설정되지 않았고, 실행하지 않았다.

## 복구 및 원복의 현재 한계

Mac의 실제 운영 서비스는 migration 133과 호환되는 native binary로 교체했다.
배포 SHA, 원본 백업, canary와 검증 범위는
[Mac 운영 기록](MAC-MINI-OPERATIONS-2026-09-28.md)을 따른다. 초기 prototype의
미배포 상태를 현재 운영 상태로 해석하지 않는다. 원복이 필요하면 해당 배포에 함께
보존한 migration 133 호환 binary만 사용한다. migration 130 binary를 현재 DB에
덮어쓰거나 DB 보호 trigger를 제거하지 않는다.
시험 리소스를 정리할 때는 `agentdesk.purpose=24x7-native-verification` label과
정확한 container/volume 이름을 확인하고, 진행 중인 build/test가 없는지 먼저 확인한다.
전체 Docker prune이나 다른 AgentDesk/OpenCode PID 종료를 사용하지 않는다.

실제 운영 복구에서는 stream EOF/abort ACK/lease TTL을 writer 정지로 해석하면 안 된다.
unknown 실행의 동일 자원 재진입을 막고 실제 정지 또는 격리를 증명하는 W1 구현과
그 관리 명령이 아직 필요하다. managed prototype을 무인 자동 재시도에 활성화하지 않는다.
Mac의 기존 Routine은 별도 제한 프로필로 운영하며 재시도 0을 유지한다.
실패 뒤 GitHub 변경이 관측되면 run을 성공으로 바꾸지 말고 해당 Routine을 manual pause한 뒤
실행 identity·자식 프로세스·원격 SHA를 대조한다. source 수정은 집중 회귀, native 검증,
idle 배포, 실제 종료 검증을 거쳐 재개한다.
