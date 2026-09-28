# AgentDesk 24/7 v5 구현 및 검증 보고

## 2026-09-28 후속 검증

현재 판정은 [운영 감사](OPERATING-READINESS.md), 실제 Mac mini 이관/실행은
[Mac 운영 기록](MAC-MINI-OPERATIONS-2026-09-28.md)을 따른다.
`resume-20260928-final`의 970개 회귀는 최종 Qwen/종료 판정 수정 전 snapshot이며,
최종 수정본은 `resume-20260928-acceptance-v9`에 따로 기록한다. 이전 문단의 미배포/모델
미호출 상태와 373개 이하 테스트 수는 각 당시 기록이며 현재 상태를 뜻하지 않는다.
전체 lib inventory는 실행 수가 아니다. 기존 기록과 후속 실행 수를 합산하지 않는다.
최종 관련 회귀는 **1,680 passed / 0 failed / 0 ignored**다. 최종 binary의 실제 Hub/PG
여정 6개도 통과했다. source/binary hash와 정확한 범위는 운영 감사의 최신 표를 따른다.

v9은 Qwen wrapper와 복구 세션의 소유권, 부모의 후속 실행을 막는 process tree 종료,
확인된 lease 시간 안에서만 허용하는 DB 갱신 재시도를 포함한다. 집중 24개와 권한/handoff
47개는 넓은 회귀에 포함되며 합산하지 않는다. native Mac 배포와 실제 모델 실행 증거는
별도 Mac 운영 기록에 연결한다.

## 이전 snapshot별 검증 기록

기준 명세: `AgentDesk_24x7_Reuse_Implementation_Deployment_Spec_v5_2026-09-25.md`.
기준 커밋: `9d34870a7e5ced95f9ad5b99259adb2f50726c20`.
구현 브랜치: `kunkunGames/agentdesk-24x7-native-v5`.
최신 검증일: 2026-09-27 KST. 이전 snapshot의 기록은 아래에 보존한다.

**판정: W0/W1의 일부와 W2 첫 adapter를 검증했지만 문서 전체 구현과
24/7 운영 활성화는 충족하지 않았다.** 최신 운영 감사는 [OPERATING-READINESS.md](OPERATING-READINESS.md)를 따른다. 검증된 W2 범위는 Linux의 작은 파일 과제다. 보호된
checker 실행, receipt, 기존 FSM 완료, Rust/JS disposition, 사람 승인이 포함된다. 검증된
후속 W3 코드는 보호된 registry와 기존 tick/Auto-Queue/FSM에 연결했다. 선행 산출물은
완료 receipt와 frozen artifact를 검증하고 실제 파일로 전달한다. 후속 통합 증거는
[Campaign 실행 계약](CAMPAIGN-EXECUTION.md)과 운영 감사에 기록한다. 실제 모델 E1 인수는 별도다.
범용 빌드 검사와 전체 프로젝트 suite는 포함되지 않는다. Native 기능을 재사용하는 방향은
유지한다. 새 scheduler, queue, Campaign 원장, LLM client, DB table은 추가하지 않았다.
실제 모델, OS 설치, 재부팅, 측정된 24시간은 이 보고서의 통과 범위가 아니다.

**추가 범위 조건:** 2026-09-26 사용자 지시로 Codex, Grok, Qwen CLI 등도 24/7
본선 실행자가 될 수 있다. OpenCode는 첫 native 검증 대상이며 제품 전체의 고정
실행기가 아니다. [배포 계획](DEPLOYMENT-PLAN.md)의 provider 공통 경계와 인수표를 따른다.

**후속 구현 전 재검증:** 2026-09-27 KST. `operations-current-regression.log`는 **373 passed /
0 failed / 0 ignored**, 398.64초다. Node 정책 161개와 실제 Hub의 운영 반례 네 가지도
통과했다. 소스/binary/HTTP의 정확한 대응은 `OPERATING-READINESS-RESULT.json`과
[운영 감사](OPERATING-READINESS.md)를 따른다. DB helper 시험의 digest 저장과
실제 A→B 파일 소비를 구분한다. 구현 완료는 fixture이고 native 모델 backend도 모의
서버다. OS 설치, 재부팅, 실제 모델 및 측정된 24시간 통과가 아니다. 아래 과거 snapshot의
숫자/해시는 기록으로 보존하며 현재 결과와 합산하지 않는다.

**이전 managed-checks 실행 파일:** Linux dev application build가 13분 17초에 통과했고
`--version`/`--help`는 각각 exit 0이었다. 그 binary SHA-256은
`0d995bfe00739249b84152948a0d05a5ef8b0549051444730ba64832a8fa21da`이다.
보존 파일은 `target/24x7-native/bin/agentdesk-linux-x86_64-managed-checks`다. 현재 기록의
해시는 아니다.
`MANAGED-CHECKS-RESULT.json`이 소스·바이너리·시험 로그의 hash와 범위를 연결한다.
새 DB·임시 root·빈 정책/실행기 설정으로 서비스를 두 번 기동해 HTTP를 검사한 뒤 종료했다.
운영 서비스 설치나 정책/dashboard/config까지 포함한 배포 bundle 인수는 아니다.

### 검사 실행 수명 후속 결과

`PHASE-RUN-LIFECYCLE-SOURCE.json`의 fingerprint는
`5f904f7dff5df17b26e32c92a71ae8550d39dfe01dc7a4f85b07b210849cbb1f`다.
기존 `test_phase_runs`와 `resource_locks`를 재사용하고 새 table/queue는 추가하지 않았다.

- 같은 idempotency key의 시작을 직렬화하며 실행 row와 resource lease를 함께 commit한다.
  동시 시작 12건 중 실행권은 하나만 생긴다. 기존 실행/완료의 재전송은 새 실행을 만들지 않는다.
- 실행 중 완료는 현재 owner·유효 lease·같은 candidate/범위/계약을 확인한다.
  늦은 owner, 만료·재배정된 lease, 다른 candidate와 owner token 재사용을 거절한다.
- terminal receipt는 불변이다. 동일 결과의 재전송은 기존 시간/증거를 유지하고,
  다른 결과 덮어쓰기와 일반 upsert를 통한 소유권 생성·변경을 거절한다.
- 완료 증거와 resource 반환도 한 transaction이다. insert 실패와 lock 삭제 실패를
  실제 PostgreSQL에서 주입하여 row와 lock의 원자적 rollback을 검사했다.
- legacy unowned 보고는 유지한다. 이 보고와 `latest-passing-evidence` 조회를
  인증된 실제 check 또는 전체 mandatory acceptance의 통과 증거로 승격하지 않는다.

세부 계약은 [검사 실행 수명](../test-phase-run-lifecycle.md), 집중 시험 로그는
`phase-run-lifecycle-regression.log`다. 첫 집중 실행은 19개/20.57초 통과했고,
이후 같은 test binary로 W0/W1 native 회귀와 Phase Gate catalog를 포함한 **303개**를
재실행했다. `phase-run-lifecycle-combined-regression.log`에 339.53초/실패 0/무시 0을
기록했다. 19개는 303개에 포함되므로 합산하지 않는다. 이전 source의 281개와도 합산하지 않는다.
최종 source·binary·probe·로그 hash와 결과는 `PHASE-RUN-LIFECYCLE-RESULT.json`에 묶었다.

실행 파일 probe `scripts/probe_test_phase_lifecycle.py`는 **6개 조건 / 서비스 기동 2회**를
5.245초에 통과했다: 인증 없는 요청 거절, 중복 시작, 잘못된 owner/raw upsert 거절,
불변 완료 receipt, 이전 owner와 과거 반환 요청의 successor 보호, 재시작 후 결과 보존.
회수된 미완료 실행은 `running`과 빈 증거를 보존하고 성공으로 바뀌지 않았다.
`phase-run-lifecycle-http-canary-hub/result.json`과 sanitized service log를 보존했다.
이 HTTP 시험의 evidence는 합성값이며 실제 프로젝트 검사 실행 결과가 아니다.

최초 Runner 기동은 빈 DB의 shared configuration을 초기화할 수 없다는 기존 Hub 전용
계약으로 실패했다. `phase-run-lifecycle-http-canary/`에 실패와 정리 증거를 남겼다.
fixture를 채널·실행기·정책 없는 Hub로 바꾼 후 통과했으며, 두 시험의 임시 DB는 삭제했다.
임시 HOME/config/인증값도 제거했다. 운영 DB·서비스의 설치/교체/중지 명령은 실행하지 않았다.

test inventory 9,703개, PG 등록 880개/129개 파일을 기록한다. 이는 실행 수가 아니다.
PG/non-PG 분리·CI 경로·test-target integrity·maintainability 검사와 format/diff 검사를
수행했다. baseline/allowlist를 확대하지 않았다. 이 문단은 보호 검사 adapter를 연결하기
전 snapshot이다. 당시에는 registry, 실제 checker, 전체 acceptance coverage와 FSM 성공 CAS가
남아 있었다. 연결 후 확인된 범위는 [W2 첫 adapter](#w2-첫-adapter)를 따른다.

**검증본과 현재 작업본:** 초기 Linux binary, Rust 76개, Node 정책 158개 및
`BUILD-IDENTITY.json`은 W1 DB 수명 확장 **이전 snapshot**의 증거다. 로그와 binary의
hash는 다시 대조해 일치함을 확인했지만, migration 0131 및 이후 소스까지 검증했다는
뜻은 아니다. 현재 작업본의 별도 검증 기록은 아래와 같다.

| 추가 검증 | 확인 결과 | 증거 |
|---|---|---|
| Rust lib 재빌드 및 관련 114개 회귀, 첫 실행 | build 통과(17m 32s), **110 passed / 4 failed / 0 ignored**. 실패는 migration 0131의 SQL 별칭 `old`와 PL/pgSQL `OLD` 변수 충돌(`42702`) | `multi-provider-regression.log` |
| migration 근본 수정 | 별칭을 `occupied_lease`로 바꾸고 새 migration checksum 갱신. 기존 migration은 수정하지 않음 | migration 0131, immutable checksum manifest |
| 수정 SQL 실제 PG probe | **6개 조건 통과**: 기존 lease admission, unknown TTL 보존, provider 간 동일 workspace 충돌, 독립 provider 진척, unknown 점유 계산, NULL managed 한도 거절. 격리 schema transaction을 rollback | `managed-migration-probe.json` |
| 수정 후 Rust 회귀 | **114 passed / 0 failed / 0 ignored**, 실행 33.28초. 아래 native 연결 변경 이전 snapshot | `multi-provider-regression-corrected.log` |
| Node 정책 재실행, pinned 22.22.0 | **158 passed / 0 failed / 0 skipped** | `multi-provider-policy-tests.log` |
| 구조 및 필수 검사 | maintainability check 통과, 새 hard gate 0. format/diff/checksum/test inventory 통과; migration 134개 보호, inventory 9,681개 | `multi-provider-maintainability.json`, `multi-provider-test-integrity.log` |

Grok 직접 경로, registry 일관성, `force_fresh` 명시적 회귀는 수정 후 실행에서
통과했다. 이 결과는 해당 범위의 회귀 증거이며 전체 저장소 회귀나 배포 승인은 아니다.

### Native 경로 연결 후 새 검증

별도 source fingerprint `3e1208ea70598e71c74a6c864605c4d6eb60166cac67d1d473a8315c8e93ea65`
(`MANAGED-INTEGRATION-SOURCE-corrected.json`, 1,882개 입력)를 고정하고 회귀를 실행했다.
이 snapshot은 앞의 114개 통과 snapshot과 다르며 두 수를 합산하지 않는다.

- `managed-integration-regression.log`: 최초 컴파일에서 outbox 재배정 경로의 provider
  인자 전달 누락(E0061)을 검출했다. 해당 호출을 수정했고 실패 기록을 보존했다.
- `managed-integration-regression-corrected.log`: **232 passed / 0 failed / 0 ignored**.
  Rust 1.94.1 Linux lib-test build 16분 45초, 시험 281.48초. 기존 intake/outbox 배정·재배정,
  공통 execution lease 및 native OpenCode 통합을 포함한다. 실제 CLI/PG가 필요한 opt-in
  시험도 `--include-ignored`로 실행했다.
- 시험 후 1,882개 입력이 고정 snapshot과 일치함을 재확인했다. 그 뒤
  `src/db/intake_outbox.rs`의 메서드 체인 줄바꿈 하나만 rustfmt에 맞췄고,
  공백을 제외한 바이트의 동일성을 확인했다. 이전/이후 hash와 log hash는
  `MANAGED-INTEGRATION-RESULT.json`에 기록했다. 이 결과는 최신 설치 binary의 identity가 아니다.
- Linux OpenCode 1.14.41의 독립 native probe: **통과**. 읽기 3회 승인/4번째 거절,
  읽기 후 편집 2회 승인, 승인 전 파일 불변을 관찰했다. Rust DB 연결 증거와는 별도다.
- Linux binary SHA-256:
  `d9d87ba7c6a01bbc2df785cf34848fc100fb505d6fb4fef60958d987f1e6e882`.
  npm 배포 archive의 SHA-512 integrity와 실행본 버전도 확인했다.
- 구조 검사 통과, 새 hard gate 0; migration 134개 checksum 보호 통과;
  test inventory 9,686개로 갱신. 기존 voice PCM 테스트의 중복 식별자 경고는 그대로 기록한다.
- 새 PG 시험 파일/모듈을 기존 CI 경로와 non-PG 제외 목록에 등록했다. PG lane 검사
  통과(872 tests, 128 files, 새 debt 0), test-target integrity 통과.
  baseline/allowlist는 확대하지 않았다. `managed-integration-pg-lane.log`,
  `managed-integration-test-integrity-final.log`에 결과를 보존했다.

| 실제 native + Rust + PG 조건 | 관찰 결과 |
|---|---|
| read → edit, 도구 한도 2 | DB 예약 2회, 실제 파일 편집 확인, `Done`, lease 정상 반환. loopback 모델 요청 3회 |
| read 4회 요청, 도구 한도 3 | DB 예약 3회, 초과 요청 거절, `Done` 없음, `unknown` row 보존. loopback 모델 요청 4회 |
| 모델이 응답하지 않음, 시간 한도 3초 | 약 **3.401초**에 취소 반환, 도구 예약 0회, `Done` 없음, `unknown` row 보존. loopback 모델 요청 1회 |

시간은 이 실행의 단일 관찰값이며 p95/최악 지연 보장이 아니다. 실제 native CLI를
사용했지만 모델은 fixture다. 잔존/이탈 subprocess, 일반 shell/MCP, Windows containment,
unknown의 재시작 후 실제 종료 대조·해제, 실제 모델이나 24시간 운영을 입증하지 않는다.

### Admission 호환성 후속 검증

canonical agent policy와 legacy remote workspace 보강 후, 같은 범위에 새 회귀 두 개를
추가했다. 최초 실행은 build 20분 22초, **233 passed / 1 failed / 0 ignored**
(282.52초)였다. 새 두 회귀와 native 연결 시험은 통과했지만 기존 outbox capability-loss
경쟁 시험이 실패했다. `admission-compatibility-regression.log`를 보존한다.

실패한 시험은 sweep의 source lock을 관찰하려고 `FOR UPDATE NOWAIT`를 수행했다.
관찰 쿼리가 먼저 source를 잠그면 실제 코드의 `SKIP LOCKED`가 그 행을 건너뛰고
뒤의 family를 재시도할 수 있었다. 실패 결과의 `source_id=2`가 이 경로를 보여준다.
시험을 원본 행의 잠금 없이 `pg_blocking_pids`로 capability 갱신 transaction에 대한
실제 대기를 관찰하도록 수정했다. timeout을 늘리거나 assertions를 제거하지 않았다.
저장소의 기존 PG 경쟁 시험에서도 쓰는 관찰 방식이다. 제품 sweep 로직은 변경하지 않았다.

수정 전 입력은 `ADMISSION-COMPATIBILITY-SOURCE.json`, 수정 후는
`ADMISSION-COMPATIBILITY-SOURCE-corrected.json`이다. 수정 후 fingerprint는 별도 결과에
기록하며, `admission-compatibility-regression-corrected.log`를 재검증 근거로 사용한다.
이 결과를 앞의 232개나 첫 233개와 합산하지 않는다.

**해당 snapshot 후속 회귀: 234 passed / 0 failed / 0 ignored**, Rust 1.94.1 Linux lib-test
build 7분 47초, 시험 267.98초. 수정 후 source fingerprint:
`dbfeeef42ffa50630f70845b17d5f6e98a4a207a852abc0f75f3b168568f3d9f`.
시험 후 1,883개 입력의 hash를 대조했다. 결과·로그 hash·native 관찰값은
`ADMISSION-COMPATIBILITY-RESULT.json`에 보존한다.

- canonical agent의 thread/headless 정책, 빈 정책 우선권, 잘못된 정책 거절,
  DB row가 없는 기존 role의 채널 조회를 실제 PostgreSQL로 확인했다.
- legacy remote/미생성 workspace를 공통 계층이 잘못 거절하지 않고,
  검증되지 않은 remote managed 실행은 거절하는 회귀를 통과했다.
- native read/edit 한도 2: 예약 2회, 파일 변경, Done, 정상 lease 반환.
  한도 3에 대한 4번째 read 요청: 예약 3회, Done 없음, unknown 보존.
  무응답 모델의 3초 deadline: **3.322초** 취소 반환, unknown 보존. 시간은 단일 관찰값이다.
- test inventory 9,688개, test-target integrity 통과. PG/non-PG CI 등록 검사 통과
  (873개 PG 시험, 128개 파일, 새 debt 0). 이 등록 수를 실행/통과 수로 세지 않는다.
  최초 non-PG 목록 누락은 생성기로 수정했고 baseline/allowlist는 바꾸지 않았다.

최신 서비스 실행 파일을 빌드·설치했다는 뜻은 아니다. 이번 결과는 Linux lib-test와
실제 native CLI + loopback fixture이며, 운영 배포·실제 모델·OS 격리·24시간 인수는 미완료다.

### Native 파일 경계 반례

동일 Linux OpenCode 1.14.41, `--pure`, read/edit만 허용한 profile과 매번 `once` 승인하는
session policy로 경계를 별도 검사했다. 모든 파일·HOME·config는 disposable fixture다.
AgentDesk DB 연결이나 OS 격리는 사용하지 않았다. 아래는 native 권한 설정만의 관찰값이다.

| 요청 경로 | read/edit 결과 | workspace 밖 합성 파일 |
|---|---|---|
| workspace 안 일반 파일 | 두 도구 완료 | 불변 |
| 밖 파일의 직접 경로 | 두 도구 오류, permission grant 0 | 읽지 못함, 불변 |
| 안 symlink → 밖 파일 | 두 도구 완료, grant 2 | 읽힘, 수정됨 |
| 안 hardlink ↔ 밖 파일 | 두 도구 완료, grant 2 | 읽힘, 수정됨 |

증거: `target/24x7-native/opencode-linked-paths-probe.json`.
재현 script: 같은 디렉터리의 `probe-linked-paths.py`.
당시 managed native grant는 read/edit 종류와 예산을 검사하며 이 파일 alias 경계를 추가로
차단하지 않았다. 따라서 W0의 보호 config/credential/검사 파일 격리는 **미완료이며 실제
반례가 있다**. 이 결과를 tool budget 실패나 실제 운영 파일 손상으로 해석하지 않는다.
수정 방향은 task가 보호 파일을 읽거나 쓸 수 없는 OS 소유권·filesystem/mount 경계를
확정하고 같은 반례를 다시 실행하는 것이다. 단순 경로 문자열 검사만으로 TOCTOU와
hardlink 격리를 입증하지 않는다.

### OS 파일 경계 후속 작업

공통 Linux Landlock/전용 UID 경계를 기존 managed launcher에 연결했다. 기존 unmanaged
spawn 경로는 이 경계를 사용하지 않는다. 보호 profile JSON을 서비스가 읽고 검증하여
전달하며, native 프로세스에는 별도의 임시 HOME을 준다. 기존 auth catalog의 credential
전용 env 계약을 유지하고 실제 native 시험도 해당 resolver를 통과하도록 바꿨다.

- 같은 production 모듈을 복사하여 컴파일한 작은 harness에서 kernel 시험 **3개 통과**:
  정상 workspace read/edit, 외부 symlink/자기 environ/상속 fd/다른 executable 차단,
  기존 hardlink admission 거절, 보호 config의 alias/권한 검사.
  `filesystem-boundary-harness-corrected.log`. 최초 2 pass/1 fail은 REFER 차단의
  정상 errno `EXDEV`를 시험이 예상하지 못한 문제였고 별도 원본 log를 보존했다.
- 첫 native bootstrap은 SIGTRAP으로 종료됐다. `strace`에서 Bun의 자기 maps/cgroup 및
  공개 kernel 정보 읽기 거절을 확인했다. child의 해당 proc 파일만 fork 후 등록하고
  environ/mem/PID 디렉터리 전체는 열지 않았다. 수정 후 native 1.14.41 health 통과,
  기동 2.296초(단일 관찰): `filesystem-boundary-bootstrap-proc.log`.
- 위 harness/bootstrap은 AgentDesk 전체 adapter 회귀를 대신하지 않는다.
  `FILESYSTEM-CONTAINMENT-SOURCE.json`에 source 1,888개를 고정하고
  `filesystem-containment-regression.log`로 후속 시험을 실행했다.
- Landlock ABI 3의 network/IPC 및 일부 metadata syscall은 이 구현의 보장 밖이다.
  OS 파일 경계 일부가 통과해도 W0 전체, W1 crash 복구, W2/W3, 설치 또는 24시간 운영을
  통과 처리하지 않는다. [공식 지원 범위](https://docs.kernel.org/userspace-api/landlock.html).

**확정 후속 결과: 265 passed / 0 failed / 0 ignored**, Rust 1.94.1 Linux lib-test build
17분 05초, 시험 256.83초. 기존 234개 범위에 auth profile 28개와 실제 kernel 경계 3개를
더한 실행이며 기존 횟수와 합산하지 않는다. 203개 기존 build warning은 남아 있다.
source fingerprint는
`bd5b12de7fa4a57b2092c9b9f98d9be57d4c27aadeb64d4e88149a1a47f6cebe`이며,
시험 후 1,888개 입력의 일치를 확인했다. test binary SHA-256은
`f668ab1bec19b032e7dffb852758cb359d963cf4720493044455e63ab6aa82b2`다.
이는 AgentDesk 서비스 실행 파일의 hash가 아니다.

| 실제 production launcher + profile resolver + native CLI + PG | 관찰 결과 |
|---|---|
| 정상 read → edit | 도구 2회 예약, 실제 편집, native `Done`, lease 반환, 1.700초 |
| read 4회 요청 / 한도 3 | 예약 3회, 초과 거절, `Done` 없음, unknown 유지, 1.743초 |
| 무응답 / deadline 3초 | 3.349초 취소 반환, 예약 0회, `Done` 없음, unknown 유지 |
| 밖 파일 symlink read/edit + 자기 environ read | 도구 3개 모두 오류, 예약 2회, 밖 파일 불변, 합성 내용/credential marker 미노출, 1.723초 |

`aliases`의 native `Done`은 오류 도구까지 종료된 provider turn의 terminal이다.
요청한 외부 파일 작업의 성공이나 카드 acceptance 통과가 아니다. 이 launcher
snapshot 시점에는 W2의 보호 검사와 성공 CAS가 아직 없었다. 위 시간은 각각 한 번의 관찰이며 p95나 물리적 종료
최악 지연을 입증하지 않는다. native fixture는 warm server 기동 후 실행 예산을 연결한다.
전체 제품의 cold-start deadline/재시작 인수는 별도로 남는다.

format/diff, 유지보수성(새 hard-gate finding 0), migration checksum 134개,
test-target integrity(9,691개), PG lane 등록(873 tests/128 files) 검사를 통과했다.
PG baseline/allowlist는 그대로다. 결과·source·binary·log hash와 각 native 관찰값은
`FILESYSTEM-CONTAINMENT-RESULT.json`에 보존한다. Linux test/native 자식이 남아 있지
않음을 확인했고 Windows 운영 PID 37188의 시작 시각과 실행 경로도 그대로 확인했다.

### 고정 노드 ID와 재시작 뒤 종료 관측

기존 `node_registry`와 heartbeat에 종료 불명 실행의 관측을 연결했다. managed admission과
capability 광고는 명시한 `cluster.instance_id` 또는 `AGENTDESK_INSTANCE_ID`가 실제 등록된
ID와 일치할 때만 허용한다. hostname/PID fallback과 실행 중 ID 변경은 managed owner로
사용하지 않는다. 일반 노드 ID의 기존 fallback 자체는 유지한다.

prompt를 보내기 전에 실제 warm runtime의 boot ID, PID namespace, PID, 시작 시각, UID를
nonce가 있는 기존 lease에 영속적으로 묶는다. 새 migration의 제약도 binding 없는 managed
`running` 전이를 거절한다. watchdog의 배타적 종료는 spawn 시각의 process identity를
기존 kill helper에 전달하여 PID가 재사용된 다른 프로세스를 대상으로 삼지 않게 한다.

heartbeat는 만료/unknown 실행을 최대 128개씩 관측하고, nonce와 runtime binding이
그대로인 row에만 기록한다. 별도 scheduler, queue, table을 추가하지 않았다.

| 관측값 | 의미와 허용 범위 |
|---|---|
| `alive` | 원래 runtime 또는 같은 전용 UID의 살아 있는 프로세스를 관찰함 |
| `not_observed` | 원래 runtime 종료와 해당 UID의 빈 관측 목록. 원자적 정지 증명이 아님 |
| `unobservable` | 부팅/PID namespace가 바뀌었거나 identity/프로세스 목록을 확인하지 못함 |
| `inference_status=unknown` | 위 로컬 관측으로 원격 모델 요청의 종료를 판정하지 않음 |

어느 관측값도 자동 lease 해제, 예산 환불, `quiesced` 승격을 허용하지 않는다. `/proc` 조회는
fork와 경쟁할 수 있으며, 다른 boot/namespace에서는 이전 실행의 생사를 단정할 수 없다.
따라서 W1의 실제 containment/backend 정산, 재시작 후 충돌 해제와 C 진척은 여전히 남는다.

이 변경의 입력 1,892개와 fingerprint
`4a21076bf58f8142e8db6948036cb53043f1935bc396951ca7857bb20abd7db8`를
`RUNTIME-OBSERVATION-SOURCE.json`에 고정했다. `runtime-observation-regression.log`의
결과는 **279 passed / 0 failed / 0 ignored**, build 16분 39초, 시험 276.84초다.
앞의 265개 범위에 node registry 13개와 실제 이탈 writer 시험 1개를 포함한 실행이다.
이전 통과 수와 합산하지 않는다. 기존 build warning 203개는 그대로 남는다.

- 실제 Python 자식이 `setsid`로 원래 process group을 벗어나 파일을 쓰게 했다.
  부모를 종료·수거하고 원래 group의 소멸을 확인해도 자식은 계속 썼으며, 관측은
  `alive`였다. 독립 UID의 정상 프로세스는 관측과 owned cleanup 동안 계속 살아 있었다.
- 저장된 boot ID를 다른 값으로 바꾼 경우 `unobservable`을 확인했다. 이는 실제 호스트
  재부팅이나 container 교체 인수가 아니다. boot/PID namespace가 다르면 자동 복구하지 않는다.
- native 한도 초과는 예약 3회, `Done` 없음, unknown 유지였다. 무응답의 3초 deadline은
  **3.276초**에 취소 반환했다. 종료 관측 후에도 두 case의 `local_writer_status`는
  `not_observed`, `inference_status`는 `unknown`이며 원래 row와 도구 예약이 유지됐다.
- 정상 read/edit는 예약 2회·실제 파일 변경·native terminal·lease 반환을 확인했다.
  alias case도 기존 외부 파일/credential marker 미노출과 도구 거절을 유지했다.
  모델은 loopback fixture이며 위 시간은 단일 관찰값이다.
- format/diff, 유지보수성 hard-gate finding 0, migration 134개 checksum,
  test inventory 9,694개와 PG lane 873 tests/128 files 검사를 통과했다.
  등록 수를 시험 실행 수로 계산하지 않으며 baseline/allowlist는 확대하지 않았다.

시험 후 1,892개 source 입력의 일치를 확인했다. test binary SHA-256은
`8a4673fbe6a6f14f175e0f623997478dd03b8deea7e0130e943fd39666dfd4b8`다.
`RUNTIME-OBSERVATION-RESULT.json`에 native 결과와 source/binary/log hash를 기록했다.
검증 container의 살아 있는 시험 자식은 0개였다. 해당 개발 container의 PID 1이 수거하지
않은 비실행 zombie 60개는 별도로 기록했으며 실행 중 writer로 집계하지 않았다.
Windows 운영 PID 37188의 경로·시작 시각은 그대로다. 이 runtime observation snapshot은
당시 서비스 실행 파일의 재빌드·설치, 실제 모델, 자동 unknown 해제, 24시간 인수를
끝내지 않았다. 이후 `0b1e6c…`는 W2 성공 CAS를 확인했지만, 그 binary의 W3 DB helper
시험은 실제 A→B 파일 전달을 입증하지 않았다. 후속 구현은 최신 운영 감사를 따른다.

### 일반 OpenCode 실행의 버전 계약 분리

앞의 snapshot에서는 명시적 allowlist가 있는 모든 OpenCode 실행에 1.14.41을 요구했다.
후속 수정은 이 정확한 버전 요구를 managed admission이 있는 실행에만 적용한다. 일반
실행은 healthy runtime, 실효 native permission schema, 요청한 session permission의
실제 응답 binding을 계속 검사한다. profile의 deny/ask와 규칙 순서도 보존한다.

HTTP fixture에 다른 버전 문자열과 같은 권한 계약을 제공하여 일반 실행의 준비가
성공하는지 검사하고, 같은 응답에서 managed 실행은 config/session 요청 전에 거절되는지
검사한다. 이 fixture가 다른 실제 OpenCode release의 native 동작을 인증하지는 않는다.
실제 CLI 통합은 여전히 hash가 고정된 1.14.41을 사용한다.

source 1,892개, fingerprint
`bbd989dde48e2bbb3bb049b41e6c9022d866696fd8f5ab9fcd7716823fa6b181`를
`PERMISSION-COMPATIBILITY-SOURCE.json`에 고정했다.
`permission-compatibility-regression.log`가 해당 snapshot의 새 회귀 기록이다.
잔존 writer 시험도 boot ID와 PID namespace 불일치를 각각 검사하도록 보강했다.

**확정 결과: 281 passed / 0 failed / 0 ignored**, Linux lib-test build 8분 01초,
시험 254.18초. 기존 279개 범위에 위 permission 회귀 2개를 더했으며 통과 수를 합산하지
않는다. native 한도 초과/무응답은 기존 unknown과 예약을 유지했고, 3초 무응답 deadline의
취소 반환은 3.438초였다. 정상 read/edit와 alias 차단도 통과했다. 시간은 단일 관찰값이다.
test binary SHA-256은
`797c714b246a7ecc9a6599b7f8bed6e1dfe641bc619c84703025b1a2ce292d28`다.

format/diff, 유지보수성 hard gate 0, migration 134개, inventory 9,696개,
PG lane 873 tests/128 files 검사를 통과했다. baseline/allowlist는 그대로이며
`PERMISSION-COMPATIBILITY-RESULT.json`에 고정 source와 binary/log/native 증거를 묶었다.
검증 container에 살아 있는 시험 자식은 없고 비실행 zombie는 69개로 별도 기록했다.
운영 PID 37188의 경로·시작 시각은 그대로다. 전체 저장소 회귀, 다른 실제 CLI 버전,
배포 및 24시간 인수 결과로 해석하지 않는다.

## 후속 검증: 현재 소스의 application binary

위 281개 회귀와 같은 1,892개 Rust/migration/toolchain 입력을 빌드 후 다시 대조했다.
Linux dev application build는 **14분 23초, exit 0**이며 compiler warning은 351개다.
`--version`은 `agentdesk 0.1.3`, `--help`는 Usage 출력과 exit 0을 확인했다.
version 문자열은 초기 binary와 같으므로 snapshot 식별에는 SHA-256을 사용한다.

- 파일: `bin/agentdesk-linux-x86_64-permission-compatibility`, 302,346,560 bytes, ELF.
- SHA-256: `72503bd1e30c6619d42dc37c34f77dd8eeba8fde9c5646ed0a40975e6ea2c9b9`.
- 결과: `CURRENT-APPLICATION-BUILD-RESULT.json`, log: `current-application-build.log`.
- 초기 `bin/agentdesk-linux-x86_64`와 `BUILD-IDENTITY.json`은 덮어쓰지 않았다.

컨테이너의 생성 파일과 복사한 artifact의 hash가 일치한다. Cargo dependency 기록도
보존했다. 앞의 1,892개 capture에 없던 `defaults.json`과 `assets/runner-entry.html`은
**빌드 후 관측한 추가 입력 hash**로 따로 표시했다. 이 capture를 배포 정책·dashboard·
운영 config 전체의 사전 고정 bundle로 해석하지 않는다. 실제 배포의 W7 인수는 남아 있다.

`current-application-lifecycle.json`에서 검증 컨테이너의 살아 있는 자식은 0개,
비실행 zombie는 69개이며 Windows 운영 PID 37188의 경로·시작 시각은 같음을 확인했다.
application 서비스는 시작하거나 설치하지 않았다. 실제 모델·A/B/C·24시간 시험도 아니다.

## 현재 코드 재검토

- 공통 예산/격리는 기존 `node_execution_leases`를 확장한다. 새로운 테이블이나
  scheduler를 추가하지 않았다. 같은 workspace의 잔존 writer는 provider가 달라도 충돌한다.
- `ManagedAttempt`의 prompt/tool admission 및 정상 terminal 관측을 OpenCode의 기존
  HTTP/SSE 경로에 연결했다. native `permission.asked`에 DB 예약을 먼저 commit한 뒤
  `once`를 보낸다. read/edit 외의 도구는 이 첫 template에서 지원하지 않는다.
- 기존 profile의 human `ask`는 headless template에서 `deny`로 좁힌다. native 규칙
  순서/경로 매칭을 보존하며, 기존 `allow`만 예산이 있는 `ask`로 바꾼다.
- silent provider에서도 기존 lease owner가 deadline 취소를 발행한다. DB와 worker의
  wall clock을 빼지 않고 DB의 잔여 시간과 worker monotonic clock을 사용한다.
- managed/native launcher는 별도 warm key와 `--pure`, 지정 profile 환경을 쓴다.
  service DB/control-plane 환경은 상속하지 않는다. 이 환경 분리는 OS/filesystem/network
  containment 전체를 증명하지 않는다.
- registry의 adapter별 protocol과 node의 `managed_providers`가 admission 기준이다.
  OpenCode 첫 제한 프로토콜만 선언하며 Codex/Grok/Qwen의 native 제한 지원을 추정하지 않는다.
- 오류/timeout 때 best-effort abort와 배타적 warm server 종료를 요청하지만, 이 요청을
  quiescence 증거로 쓰지 않는다. unknown의 실제 종료 대조 및 해제는 아직 미완료다.
- 새 `acquire`는 기존 capacity 비활성 경로에서도 DB policy/retained lease 조회를 수행한다.
  기준 HEAD는 해당 경로에서 바로 반환했다. DB 장애 때 기존 실행의 가용성에 미치는
  영향을 해소·검증해야 하며, 관련 회귀 통과가 이 동작 변화를 없애는 것은 아니다.
- 앞의 279개 snapshot까지 `permissions::prepare`의 OpenCode 1.14.41 요구가 일반
  실행에도 적용됐다. 후속 수정은 정확한 버전 요구를 managed 실행에만 적용하며,
  일반 실행도 healthy runtime, 실효 permission schema, session 적용 확인을 요구한다.
  잘못된 권한을 prompt 안내로 대체하는 fallback은 없다. 해당 변경은 별도
  `PERMISSION-COMPATIBILITY-SOURCE.json`과 후속 회귀 log로 검증한다.
- 후속 수정은 executor의 기존 `ProviderExecutionContext.agent_id`를 정책 조회에
  사용한다. canonical row가 있을 때 빈 정책과 잘못된 정책도 그 row를 기준으로 판단한다.
  DB row가 없는 기존 role/caller는 채널 lookup을 유지한다. thread/headless가 다른
  채널 정책을 상속하거나 부모의 한도를 잃는 반례를 실제 PG 회귀로 추가했다.
- 일반 remote workspace를 로컬 `canonicalize` 실패로 거절하던 동작을 분리했다.
  기존 remote/미생성 경로의 검증은 해당 adapter가 맡는다. managed 실행은 확인된 로컬
  자원이 필요하며 remote managed 실행은 미지원으로 거절한다. 이 두 후속 수정은
  `ADMISSION-COMPATIBILITY-SOURCE.json`과 별도 회귀 로그로 구분한다.
- Grok은 기존에도 OpenCode facade를 거쳐 native Grok CLI로 전달됐다. 잘못된 모델
  실행을 재현한 것은 아니다. 이번 변경은 registry를 따르는 직접 경로로 맞추어
  중복 provider 선택을 제거하고, 중간 facade에서 상수 false였던 `force_fresh`를 보존한다.
- native OpenCode 독립 probe에서 권한 요청을 3회 `once` 승인하고 4번째를 거절했을 때
  read 완료가 정확히 3회였다. 이는 native gate 계약 증거이며 아직 AgentDesk DB budget과의
  실제 연결 증거가 아니다 (`opencode-tool-budget-contract.json`).
- custom session title과 `formatter=false`, `lsp=false`를 사용하는 별도 fixture에서
  model 요청 2회를 관찰했다. 실제 모델, 모든 도구, OS/network 격리를 입증하지 않는다
  (`opencode-managed-aux-probe.json`).

"기존 기능 사이드이펙트 없음" 또는 "ROI/응집도/재활용/확장성 최대"는 현재 판정이
아니다. `0b1e6c…`의 373개 lib 시험과 HTTP probe는 W2 성공 CAS의 증거이며,
W3 helper의 합성 digest 시험은 실제 파일 전달 증거가 아니었다. 후속 W3 구현과
최종 실행 증거는 [운영 감사](OPERATING-READINESS.md)를 따른다. 실제 모델, OS 설치,
재부팅, 24시간 표본은 확인하지 않았다.

## 재사용·변경 지도

| 작업 | 실제 소유자/호출자와 변경 | 후조건 및 남은 조건 |
|---|---|---|
| W0 권한 | `opencode::run_session` → native permission; 기존 launcher → 공통 `FilesystemBoundary` | native allowlist에 Linux Landlock/전용 UID/보호 config snapshot을 연결. 실제 링크 우회 차단과 정상 read/edit 통과. network/IPC, 다른 OS/일반 도구는 미완료 |
| W0 runtime identity | `acquire_warm_server` → `managed_runtime::prepare` → `runtime_identity::warm_server_key` | env/unset/home, 보호 config 내용, UID/GID, binary metadata를 warm identity에 반영. 실행 중 권한 철회·기존 writer drain·bundle admission은 미완료 |
| W1 결과 관측 | 기존 `consume_sse_inner`/message recovery/CancelToken | matching terminal 없는 EOF는 원문을 보존하고 오류 반환. 복구 JSON/abort ACK로 Done을 만들지 않음. DB unknown fence는 검증, 실제 정지 대조·해제는 미완료 |
| W1 공유 프로세스/복구 관측 | 기존 exclusive-retirement, birth-checked kill helper, node heartbeat | 실제 이탈 writer의 관측과 독립 프로세스 생존 확인. 고정 node ID와 durable runtime binding 추가. 로컬 관측으로 unknown을 해제하지 않으며 backend 정산·재부팅 인수는 미완료 |
| W1 실행 한도 | 기존 lease와 OpenCode 1.14.41 HTTP/SSE 연결 | native steps 반례 후 Rust/PG admission 연결. read/edit의 3회 한도와 3초 deadline fixture 통과. 일반 도구/실제 모델/물리적 종료는 추가 인수 필요 |
| W2 검사→완료 | 기존 `test_phase_runs`, Phase Gate, Kanban/FSM, review 정책, `checks` driver | 첫 Linux 파일 과제 adapter의 보호 실행, receipt, 성공 CAS, Rust/JS disposition, 사람 승인을 검증. 범용 빌드·전체 suite·다른 OS·운영 활성화는 미완료 |
| W3 목표→기존 큐 | `server/campaign_execution` → 기존 tick/Auto-Queue/FSM, `handoff::publish`, 공통 계약/dispatch binding | 보호 plan, 전역 task key, 유한 batch, 검증된 선행 파일, 제한 재작업, pause/cancel 경계를 구현. 직접 done helper는 제거. 실제 모델 E1, 운영 GC/복원은 별도 인수 |
| W4~W6 | 기존 plan/Routines/sidecar/수정 카드 | 선택 기능. 구현·활성화하지 않음. observer가 W1~W3 결함을 보완한다고 가정하지 않음 |
| W7 구축 | 기존 Cargo/build token, portable initializer | 격리 빌드·시험, initializer 재실행, 임시 Hub의 실제 HTTP와 재시작 확인. 설치 서비스, 전체 canary bundle, E5, 재부팅 및 24시간 시험은 미완료 |

`permissions.rs`와 `runtime_identity.rs`는 실제 adapter caller가 있는 작은 책임
단위로 분리했다. `completion_tests.rs`는 transport 단절이 성공으로 승격되는
반례를 검사한다. 두 Python probe는 실제 native 효과를 관찰하는 개발용 검사이며
제품 실행 경로가 아니다.

## 실행 결과

원본 로그와 JSON은 checkout의 `target/24x7-native/`에 있다. 이 디렉터리는
배포 bundle이 아니며, git에 포함되지 않는 로컬 검증 artifact다.

| 검증 | 결과 | 증거 |
|---|---|---|
| Linux 실행 파일 | **빌드 통과**, dev profile, ELF x86_64 확인. 생성 바이너리 `--version`/`--help` exit 0 | `linux-binary-build.log`, `linux-cli-help.txt`, `BUILD-IDENTITY.json` |
| Linux Rust, pinned 1.94.1, debug=0, jobs=1 | **50 passed, 0 failed, 0 ignored**. OpenCode 새 회귀 13개 포함 | `linux-native-tests.log` |
| 설정 소비자 회귀 | 같은 test binary로 별도 **26 passed, 0 failed, 0 ignored**. 위 50개와 중복 없음 | `linux-consumer-tests.log` |
| 실제 PostgreSQL 17 | 위 50개 중 Campaign 3개, resource lock 2개 실행. `AGENTDESK_REQUIRE_PG=1` | 같은 로그 |
| Node 정책, pinned 22.22.0 | **158 passed, 0 failed, 0 skipped** | `policies-tests.log` |
| 설치된 OpenCode 1.14.41 native permission | read 성공, shell 거절, profile deny 유지, shell 양성 대조, 잘못된 action HTTP 400 | `opencode-native-probe.json` |
| 동일 binary의 native steps | **FAIL**. steps=3, read 완료 8개, main-model 요청 9개, 별도 auxiliary 요청 1개 | `opencode-steps-probe.json` |
| portable initializer | 처음 10개 항목 생성, 재실행 생성 0개, 기존 파일 4개의 hash 동일 | `starter-scaffold-probe.json` |
| Windows Cargo check | 이전 중간 snapshot에서 통과. 최종 소스 Windows binary 검증으로 해석 금지 | `cargo-check-no-debug.log` |
| Windows Rust test build | 한 번은 rustc allocation OOM, debug=0 재시도는 host commit limit 접근으로 소유한 build만 중단. Windows 테스트 실행 없음 | `opencode-tests.log`, `opencode-tests-no-debug.log` |

Linux build container는 20 GiB 메모리 한도와 별도 Cargo target volume을 사용했다.
Linux 검증은 Windows 실행 파일이나 macOS/oMLX 검증을 대신하지 않는다.
binary는 `bin/agentdesk-linux-x86_64`에 보존했다. format, diff, maintainability,
test-target integrity 검사도 통과했고 정확한 hash와 결과는 `BUILD-IDENTITY.json`에
기록했다. Rust lib test 빌드에는 기존 경고 203개, dev binary 빌드에는 351개가 있었으며
전체 저장소가 warning-free라고 보고하지 않는다.

## V01~V36 대응

`부분`은 해당 행 전체의 통과를 의미하지 않는다. `미실행` 항목을 기존 순수 함수
회귀나 source review만으로 통과 처리하지 않는다. 기존 T/AR 대응은 v5 명세를 유지한다.

| ID | 현재 증거/상태 | 남은 인수 |
|---|---|---|
| V01 | 부분: runtime overlay/config snapshot identity, spawn 전 보조 기능 설정 검사와 실제 native fixture | 실제 local model, 모든 보조 추론·network 차단, reload/drain |
| V02 | 부분: native-only 링크 실패 반례 뒤 Linux UID/Landlock을 연결하여 symlink/자기 environ 차단, 기존 hardlink admission 거절 | 다른 OS/일반 도구, runtime의 전체 credential 경로, network/IPC, subagent/MCP |
| V03 | 부분: EOF success JSON, message recovery, abort ACK/500, 수동 cancel에서 Done 0 | task evidence 진위, attempt binding, 전체 coverage |
| V04 | 부분: suite 누락·0회·skip·부분 실행 거절 회귀와 HTTP human approval | 실제 프로젝트 전체 mandatory suite |
| V05 | 부분: 허용 밖 추가 파일은 unverifiable로 남고, 역슬래시 파일명은 후보에서 거절 | 보호 binary 변조, symlink/hardlink 전용 시험 |
| V06 | 부분: 취소·deadline은 통과 출력 뒤에도 성공 receipt를 남기지 않음. 동시 결과 변경, 과거 승인, 공개 위조를 거절하고 완료 감사는 1회 | mutant 반례 전체 |
| V07 | 부분: 실제 checker가 PG 시험과 HTTP tick에서 FSM을 1회 완료. 검사 통과만으로 human 카드를 완료하지 않음 | 유실 리뷰와 의도적 대기의 전체 구분 |
| V08 | 부분: 보호 plan에서 기존 queue로 공급하고 선행 artifact의 실제 파일·hash manifest를 후속 baseline에 연결 | fixture 통합 결과는 최신 운영 감사 참조. 실제 모델 E1 인수는 미충족 |
| V09 | 미실행, W4 미활성 | 제한된 decomposition |
| V10 | 부분: 실제 검사 중 Hub 강제 종료→자식 종료→lease 만료→같은 후보 1회 재검사와 완료 감사 확인 | 공급/증거 저장/wake-up의 나머지 crash 경계, single-call API 중복 방지 |
| V11 | 부분: 실패 assertion의 receipt와 보호 계약으로 기존 rework dispatch 생성, max_candidates에서 중단 | 실제 모델의 수정 성공, 일반 plan/decomposition 계보 |
| V12 | 부분: 미완료 또는 영구 거절 카드 16개 뒤 독립 checker 진척을 실제 HTTP로 검증 | 동일 agent/queue의 A 대기·B 의존·C 독립, live writer 충돌. checker 시험으로 전체 여정을 대체하지 않음 |
| V13 | 부분: 동시 공급·revision CAS·전역 task key, 중단된 비공개 입력 준비 복구 | 모든 batch/claim crash 지점, OS 재부팅 |
| V14 | 부분: 보호 graph/assignment/policy 불변, pause/cancel 뒤 신규 dispatch 및 실행 승격 거절 | 이미 실행 중인 provider의 실제 정지/격리 |
| V15 | 미실행, advisor 미활성 | snapshot 고정·나이·writer 경쟁 |
| V16 | 미실행, advisor 미활성 | request 중복 및 notification loop |
| V17 | 부분: profile deny/key 회귀 | 실제 advisor fallback·read-only 격리 |
| V18 | 미실행, advisor 미활성 | Routine 또는 sidecar 한 경로의 owner/cleanup |
| V19 | 미실행, advisor 미활성 | anchor와 owner 수명 구분 |
| V20 | 부분: nonterminal JSON은 Done 아님 | advisor 인증·schema·ACL·상관관계 |
| V21 | 미실행, advisor 미활성 | 수정 카드 원자적 연결 |
| V22 | 미실행, advisor 미활성 | 관련 변경 stale 판단 |
| V23 | 미실행, advisor 미활성 | live writer와 적용 경계 |
| V24 | 미실행, advisor 미활성 | hold/권한 확대/검사 면제 거절 |
| V25 | 미실행, advisor 미활성 | 실제 효과 검사와 회귀 |
| V26 | 미실행, advisor 미활성 | durable attempt·응답 유실 |
| V27 | 미실행 | 재시작/자식/새 세션에서도 잔여 예산 보존 |
| V28 | 부분: native steps 실패 반례 후 AgentDesk DB tool budget과 독립 deadline을 연결. 실제 native + fixture 통과, EOF는 비성공 | 일반 도구, 정상 장기 빌드, 실제 종료/격리와 실제 모델 |
| V29 | 미실행 | 권한 철회·원격 효과 후 crash 복구 |
| V30 | 부분: session 생성/전송 전 cancel guard 및 EOF cancel 회귀 | durable admission과 cancel의 서버 순서 |
| V31 | 부분: 실제 PG Campaign CAS/reconnect/retention 회귀 | 실행 metadata와 checkpoint의 경쟁 통합 |
| V32 | 부분: co-resident hard-kill 조건, 실제 setsid writer 관측·독립 프로세스 생존, 고정 node/runtime binding, PG unknown의 TTL/재연결/관측 후 예산 보존 | authoritative 정지/격리와 backend 정산, 재부팅, unknown 해제 후 실제 C 진척 |
| V33 | 미실행 | input/evidence GC와 DB 복원 |
| V34 | 부분: 보호 config snapshot/UID/binary metadata warm identity, schema 실패, 기존 정책 회귀 | 실제 bundle admission, 배포 canary, 실행 중 파일 교체·drain·구버전 차단 |
| V35 | 미실행 | 실제 local E1/E2/E3 |
| V36 | 미실행 | 실제 24시간, 로그인/재부팅, ROI 표본 |

## E1~E5 및 24시간

- E1: 실제 모델 인수 미충족. 후속 구현은 기존 서비스 공급, 검증된 파일 전달, 보호 검사와 FSM의 목표 완료를 연결했다. fixture 검증과 실제 모델 여정은 최신 운영 감사에서 구분한다.
- E2/E3: 미실행. 외부 advisor는 활성화하지 않았다.
- E4: 전체 여정은 미완료. 실제 native binary + 모의 모델로 steps 반례를 재현했다.
  AgentDesk의 read/edit 강제 한도와 deadline은 통합 시험을 통과했지만 실제 정지/격리와
  전체 A/B/C 여정은 아직 입증되지 않았다.
- E5: 미완료. initializer idempotency와 임시 Hub의 실제 HTTP/재시작은 검증했지만,
  운영 대상의 서비스 등록·전체 bundle 설치·OS 재부팅 인수는 하지 않았다.
- 실제 24시간: 미실행. 시작/종료 시각, 처리량, 기아, 비용, 개입 시간 표본이 없다.
  빌드 대기 시간을 운영 uptime이나 생산적인 실행 시간으로 집계하지 않는다.

다음 필수 경계는 W0의 나머지 runtime/network/IPC 격리와 W1 unknown의 실제 정지 대조·해제다.
DB에 충돌 writer의 다음 admission을 막는 fence는 있지만 자동 해제나 재시작 복구를
완료한 것은 아니다. W2의 첫 파일 과제 adapter는 기존 `test_phase_runs`와 FSM에 연결됐다.
그 증거는 [W2 첫 adapter](#w2-첫-adapter)다. 373개 lib 시험에 포함된 W3 DB helper를
실제 A→B 파일 전달이나 E1 완료로 집계하지 않는다. 범용 빌드 검사, 실제 모델,
OS 설치, 재부팅, 24시간은 아직이다. 후속 W3 구현, 증거 검증기 보강과 새 HTTP 검증은
[운영 판정 보고서](OPERATING-READINESS.md)를 따른다.

## W2 첫 adapter

계약은 [MANAGED-CHECKS.md](MANAGED-CHECKS.md)다. 아래 횟수는 서로 합산하지 않는다.

| 검증 | 결과 | 증거 |
|---|---|---|
| Linux lib 회귀 | **369 passed / 0 failed / 0 ignored**, 360.67초. 9,343개는 filter로 제외. 실제 checker, kernel 거절, 취소·deadline 수거, FSM 1회, JS review gate를 포함한다 | `managed-checks-review-regression.log` |
| 직전 넓은 회귀의 스트림 테스트 | 351개 중 `accepted_stream_runs_past_virtual_day_until_real_eof` 1개 실패. 첫 프로토콜 출력 뒤에만 가상 하루를 넘기도록 테스트를 고친 뒤 위 369개에서 통과 | `managed-checks-final-regression.log`, `src/services/stream_json_cli/runner.rs` |
| Node 정책, 로그 시각 2026-09-27 06:25 KST | **161 passed / 0 failed / 0 skipped** | `managed-checks-node22-policy-tests.log` |
| 이전 일회용 Hub HTTP | **9개 조건 통과**, 97.099초, 서비스 기동 2회. 같은 `0d995bfe…` binary | `managed-checks-http-canary/result.json` |
| 최신 일회용 Hub HTTP | **9개 조건 통과**, 98.583초, 서비스 기동 2회. 테스트 종료 후 보존한 후보의 digest도 receipt와 일치 | `managed-checks-http-v1/result.json` |
| 기존 검사 저장 API HTTP | **6개 조건 통과**, 5.236초, 서비스 기동 2회. evidence는 합성값 | `managed-checks-storage-http/result.json` |
| 스트림 clock 회귀 반복 | **24/24 통과**, 4개 프로세스 병렬. 실제 프로토콜 수신 뒤 가상 시간을 넘기는 수정이 포함된 test binary 확인 | `managed-checks-stream-clock-stress.json` |

최신 source는 `MANAGED-CHECKS-VERIFIED-SOURCE.json`의 1,965개 입력이며 fingerprint는
`1edc3d1c4aaf85e687b69278d4dc7b1ff1d1f67af825ff700c65505dfa98034e`다. HTTP와 문서 정리 후
동일성을 다시 대조했다. `MANAGED-CHECKS-SOURCE.json`, `MANAGED-CHECKS-FINAL-SOURCE.json`,
`MANAGED-CHECKS-REVIEW-SOURCE.json`은 보강 전 입력을 보존하며 최종 판정을 대신하지 않는다.
최종 Rust test build는 7분 23초, application build는 13분 17초다.

정적 lib test manifest는 9,721개이고 compiled inventory와의 플랫폼별 예상 차이
(static-only 19, cargo-only 10)도 기존 allowlist와 일치한다. 실행 369개와 목록 9,721개를
혼동하지 않는다. PostgreSQL lane/CI 선택, migration checksum 135개, test-target integrity,
maintainability 강제 검사 10개, format과 diff 검사를 통과했다. baseline/allowlist는 확대하지 않았다.

HTTP 카나리가 확인한 조건은 관리 API 인증, 검사 전 raw `done` 거절, reconcile tick의 실제
checker와 FSM 완료, 완료 감사 1회, 공개 receipt 위조 거절, 고정 후보 보존, 인증된 현재
후보만의 사람 승인, 승인 후 완료, 재시작과 다음 tick의 중복 완료 없음이다. 임시 DB는
삭제했다(`cleanup.json`). 구현 dispatch와 `answer.txt=42`는 fixture다. 모델, 외부 메시지,
운영 DB, 설치 서비스는 사용하지 않았다. 이 결과를 범용 `cargo test` adapter나 24시간
운영 통과로 집계하지 않는다. 최신 두 HTTP probe는 각각 생성한 fixture DB의 삭제를
`cleanup.json`에 기록했고, 소유한 Hub 프로세스와 임시 인증 설정도 정리했다.

그 밖의 남은 경계: managed admission의 안정적인 instance ID와 runtime binding은 추가됐지만, 다른
boot/PID namespace의 기존 unknown을 자동 복구하지 않는다. capacity OFF의 DB 의존성은
기존 기능 호환성 gate로 남는다. OpenCode의 일반/managed 버전 계약 분리는 별도 snapshot으로
인수하며, 다른 실제 CLI 버전 전체의 호환성을 자동 인증하지 않는다.
