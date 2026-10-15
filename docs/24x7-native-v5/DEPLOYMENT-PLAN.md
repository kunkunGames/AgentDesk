# 24/7 v5 배포 준비 및 활성화 조건

2026-09-28 사용자 지정 대상은 **Mac mini의 기존 Qwen/OpenCode 여섯 Routine**이다.
내부 디스크 경로, 기존 인증, 제한된 모델 프로필, 기존 scheduler를 사용한다.
운영 DB 백업의 실제 복원과 migration 133 적용, native 서비스 배포를 수행했다.
구체 실행 결과와 경계는 [Mac 운영 기록](MAC-MINI-OPERATIONS-2026-09-28.md)을 따른다.

아래 Linux managed v1과 Mac의 기존 Routine 운영은 서로 다른 범위다. Mac 배포를
W1/E4 전체 인수로 간주하지 않으며 실제 재부팅·무개입 24시간 검증은 미완료다.

## 사용자 추가 조건: 여러 provider가 24/7 본선 실행을 담당

2026-09-26 사용자 지시에 따라 OpenCode뿐 아니라 Codex, Grok, Qwen CLI 등도
24/7 실행자가 될 수 있다. v5 원문의 **로컬 모델만 본선 실행, 외부 모델은 조언만**이라는
범위 제한은 이번 구현의 전체 제품 조건으로 사용하지 않는다. 로컬 전용 구성은
별도 운영 profile과 추가 인수 조건으로 유지한다.

- Campaign, Kanban/FSM, Auto-Queue, dispatch/outbox, 실행 lease, 검증/완료 CAS는
  provider 공통 소유자를 유지한다. 새 scheduler, provider별 목표 원장, LLM client는 만들지 않는다.
- 실행기(provider/adapter), 사용하는 model, 맡은 역할(executor/reviewer/advisor)을
  구분한다. frontier model도 승인된 과제의 executor가 될 수 있다.
- 기존 `ProviderKind`와 `provider/registry.rs`가 실행기 선택의 기준이다. 새로운
  24/7 전용 provider 목록이나 OpenCode 경유를 강제하는 라우터를 추가하지 않는다.
- 권한, 호출/도구 예산, retry/resume, 취소와 실제 정지 관측은 각 기존 adapter의
  계약을 검증한다. 단순 CLI 등록/프로세스 실행 성공은 managed 24/7 지원 증거가 아니다.
- 같은 attempt의 provider/model 변경도 예산·입력·산출물 계보를 새로 시작하는
  수단이 될 수 없다. 자동 대체 실행은 별도 명시적 정책과 검증이 필요하다.
- local-only profile에서는 보조 추론까지 local-only를 검사한다. frontier profile에서는
  승인 endpoint/model/profile과 비용·quota·인증 오류 처리를 검사한다.

| 실행기 | 기존 재사용 경로 | 이번 인수 범위 |
|---|---|---|
| OpenCode | 기존 warm HTTP/SSE adapter | 실제 CLI + Rust + PG의 read/edit 예약·한도 초과·무응답 deadline 시험 통과. 일반 도구·실제 정지/격리와 모델 인수는 미완료 |
| Codex | 기존 Codex process/tmux adapter | registry 경로 재사용. 설치 CLI의 권한/한도/취소/재개를 별도 인수 |
| Grok | 기존 StreamJson CLI dialect/runner | Discord의 OpenCode facade 우회 경로를 registry 기반 직접 경로로 정리. 실제 Grok CLI 인수는 미실행 |
| Qwen CLI | 기존 Qwen adapter 및 도구 정책 | 기존 설정/실행 경로 재사용. 새 managed 한도와 정지·재개 인수는 미실행 |

현재 새 managed admission 코드는 OpenCode 첫 제한된 native 계약의 통합 시험을 통과했다.
전체 provider가 이를 지원한다고 활성화하거나 광고할 단계는 아니다. 공통 DB 수명 계약 시험과
각 native 실행기의 실제 계약 시험을 구분하고, **managed v1 전체 운영 활성화는 W0~W3 인수 전 보류**한다.
사용자가 승인한 기존 Routine 운영의 별도 제한/증거는 Mac 운영 기록에 둔다.

## 확인된 격리 구성

| 항목 | 실제 검증 환경 |
|---|---|
| Source root | `C:\Users\12336\orca\workspaces\AgentDesk\agentdesk-24x7-native-v5` |
| Artifact root | source root 아래 `target/24x7-native` |
| Linux build | `adk24x7-native-build-20260926`, Rust 1.94.1, 20 GiB memory cap, source read-only bind |
| Cargo target | `adk24x7-native-target-20260926` volume |
| Test DB | `adk24x7-native-test-20260926`, PostgreSQL 17, loopback 44620, disposable test databases |
| Native permission/steps | Windows/Linux OpenCode 1.14.41, 임시 HOME/config/workspace와 임의 loopback port. Rust/PG 연결 통합은 Linux에서 실행 |
| Model | Python loopback fixture. 실제 모델/production profile 없음 |
| Channel/repo | 운영 채널 연결 없음, native probe는 disposable workspace만 사용 |
| Initializer | `target/24x7-native/starter-scaffold`에 starter 파일만 생성; 미실행 |

initializer의 기본 YAML은 운영 가능한 24/7 설정이 아니다. 기본 port 8791,
Claude starter agent, 상대 policy 경로가 있으므로 독립 DB·port·profile·policy
identity를 확정하기 전 실행하면 안 된다.

## 기존 서비스 관리 도구의 경계

- `scripts/install-windows-runtime-task.ps1`는 기본 사용자
  `~/.adk/release`를 고정 사용한다. `-TaskName`만 다르게 지정하면 binary/config
  격리가 되지 않는다. 현재 canary root에 이 installer를 실행하지 않았다.
- 이 Windows task는 `Interactive` logon과 `AtLogOn` trigger를 사용한다.
  로그인 전 L3를 보장하지 않는다. 별도 서비스 계정/로그인 없는 실행을 자동 가정하지 않는다.
- macOS에서는 기존 portable initializer/launchd generator/deploy 도구의
  실제 인자를 해당 호스트에서 확인한다. Windows의 시험을 LaunchAgent L1/L2
  또는 oMLX 실행 증거로 대체하지 않는다.

## Canary 활성화 전 필수 결정과 검사

1. 호스트, 지원 서비스 수준 L1/L2/L3, 독립 root/port/DB/channel/profile을 고정한다.
   프로세스 root만 다르고 DB/채널이 같은 구성을 격리로 인정하지 않는다.
2. 승인 provider/model/profile과 small model·compaction·subagent·MCP 추가 추론까지
   실제 요청 경로를 검사한다. 선택적 advisor는 초기 OFF로 유지한다.
3. task가 읽거나 바꿀 수 없는 운영 config·credential·check registry/collector,
   OS·network 경계를 확인한다. 자유 shell의 우회를 prompt로 제한했다고 주장하지 않는다.
4. source/binary/policy/config/schema/provider identity를 동일 bundle에 묶는다.
   중간 Windows check나 다른 platform binary로 최종 설치본을 인증하지 않는다.
5. W1 hard deadline/tool budget와 durable unknown fence, W2 실제 check driver 및
   완료 CAS, W3 A→B 입력/출력과 독립 C를 통과시킨다. `0b1e6c…`의 W2 성공 CAS와
   W3 DB helper 시험은 서로 다른 증거다. 실제 파일 전달을 연결한 후속 구현의
   결과는 [운영 감사](OPERATING-READINESS.md)를 따른다. 실제 모델 요청과
   OS 설치·재부팅·24시간은 fixture 통과로 대체하지 않는다.
6. 별도 DB와 산출물의 backup/restore를 확인한다. 복원 전 writer를 정지/격리하고,
   복원이 이미 발생한 외부 효과를 취소한다고 가정하지 않는다.
7. 선택한 provider 조합의 A→B/C, 실패·취소·재시작, E5 후에만 24시간 관측을 시작한다.
   원문 E1의 외부 추론 없음은 local-only profile에 추가 적용한다. 시간·완료량·정상 대기·기아·오류·
   재시도·권한/중복 효과·모델 호출·사람 개입을 실제 로그로 집계한다.

현재 즉시 막는 조건은 W0/W1이다. native `steps=3`만으로 한도를 강제할 수 없다는
반례 뒤 공통 DB 예산과 deadline을 연결했지만, 종료 불명 실행의 실제 정지 대조와
재시작 후 해제, runtime/config/OS 경계가 남아 있다. 단순 auto-restart와 advisor 추가는
이 조건을 해결하지 않는다.
native read/edit만 허용한 별도 fixture에서도 workspace 안 symlink/hardlink를 통해 밖의
합성 파일을 읽고 수정하는 반례를 확인했다. native permission을 OS 파일 격리로 간주하지
않는다. 보호 config/credential/check 파일에 대한 task의 실제 접근을 막고 재시험해야 한다.
Mac Routine 운영과 별도로 공통 W1~W3와 provider별 managed 격리 계약을 인수해야 한다.
기존 실행과의 호환성도 활성화 조건이다. capacity 비활성 경로의 새 DB 의존성은 현재
기준 HEAD와 다른 동작이며 미해결이다. 일반 OpenCode allowlist까지 적용되던 1.14.41
요구는 managed 실행에만 적용하도록 분리했다. 일반 실행의 permission API/schema와
session binding 확인은 유지한다. 이 변경의 새 source와 회귀 결과는 검증 보고에서 구분한다.
managed profile 활성화에는 기존 작업의 가용성과 잔존 writer 보호 인수가 필요하다.
현재 Mac 교체는 기존 Routine 경로이며 managed v1을 활성화하지 않는다.

호환성 차이의 정확한 범위는 `provider_dispatch::execute`가 호출하는
`execution_capacity::acquire`다. 기준 HEAD에서는 capacity OFF일 때 이 함수가 DB I/O 없이
반환했지만, 현재는 pool이 있으면 canonical agent 정책과 잔존 managed lease를 조회한다.
단, 사람 입력의 `try_route_intake`는 기준 HEAD에서도 routing Disabled 분기 전에 정책을
조회했다. 따라서 모든 기존 입력이 원래 DB 없이 실행됐다고 일반화하지 않는다.
일반 headless 경로, DB 장애, feature OFF 뒤 남은 writer를 함께 시험해야 한다.
OFF 조건을 함수 첫 줄로 옮기는 변경은 미완료 managed 작업의 보호를 없앨 수 있으므로
호환성 해결로 인정하지 않는다. 별도 one-shot helper의 직접 provider 호출까지 현재
managed admission을 거친다고 주장하지 않는다.

### Linux 파일 격리 template

첫 managed read/edit template의 launcher에 공통
`platform::managed_process::FilesystemBoundary`를 연결했다. Linux Landlock ABI 3 이상과
격리된 배포 안의 root launcher, 전용 non-root UID/GID가 소유한 private workspace가
필요하다. 현재 Windows 운영 프로세스의 권한이나 파일 소유권은 바꾸지 않았다.
Windows/macOS 및 일반 사용자 launcher에서 이 template은 시작 전에 실패한다.
이는 다른 OS나 CLI의 지원 완료를 뜻하지 않는다.

기존 named auth profile의 `xdg-config/opencode/opencode.json`을 root 소유 0600 파일로
보호한다. 서비스는 JSON과 부모 경로의 소유권/권한을 확인하고, plugin/MCP/LSP/formatter/
자동 compaction 조건을 spawn 전에 검사한다. 실행기에는 검증한 snapshot과 명시적
provider credential만 전달하며 별도 임시 HOME을 제공한다. profile catalog의 env 허용
목록을 넓히지 않는다. 파일 기반 로그인 credential은 이 template에서 인수하지 않았다.

작업 폴더는 동시 외부 writer가 없는 전용 영역이어야 하며, admission은 기존 hardlink,
다른 UID 소유 항목, 특수 파일, 100,000개 초과 항목을 거절한다. Landlock은 작업 폴더와
임시 HOME의 파일 효과를 허용하고 공용 runtime library/인증서 및 제한된 시스템 정보만
읽게 한다. 실행 허용은 선택한 CLI와 ELF loader로 좁힌다. 자식에서 supplementary group,
상속 fd, core dump를 제거하고 권한 상승을 금지한다. warm key에는 workspace UID/GID,
실행 파일 metadata와 redacted config snapshot identity가 포함된다.

이 경계는 **파일 접근 경계**다. ABI 3의 network/IPC 차단, 전체 metadata syscall 제한,
다른 UID/프로필의 실제 동시 운영, 범용 프로젝트 check adapter, unknown 복구, bundle 교체 시
drain/revocation은 별도 미완료 조건으로 남는다. Kernel의 지원 범위와 한계는
[Landlock 문서](https://docs.kernel.org/userspace-api/landlock.html)를 따른다.
독립 kernel 시험과 native bootstrap 뒤 실제 launcher/profile resolver/native CLI/PG를
포함한 **265개 회귀가 통과**했다. 링크 반례는 실제 도구 오류와 외부 파일 불변으로
확인했다. 정확한 source와 보장 범위는 [검증 보고](VALIDATION.md)의 후속 결과를 따른다.
managed admission에는 명시한 `cluster.instance_id` 또는 `AGENTDESK_INSTANCE_ID`와
bootstrap 때 등록한 ID의 일치를 요구한다. hostname/PID fallback은 managed 실행이나
해당 capability 광고에 사용하지 않는다. 실제 runtime의 boot ID, PID namespace, PID,
시작 시각, UID를 prompt 송신 전에 lease에 묶는다. 안정적인 노드 이름만으로 다른
호스트/container를 이전 실행과 동일하다고 인정하지 않는다.

기존 heartbeat의 종료 관측은 `alive`, `not_observed`, `unobservable`을 기록한다.
관측이 비어도 원자적 정지 증명이나 원격 모델 종료가 아니므로 `unknown`을 유지하고
예산을 환불하지 않는다. boot/namespace 교체, authoritative containment, backend 정산과
충돌 자원의 재개는 별도 인수가 필요하다. 이 구현·시험의 정확한 snapshot은
[검증 보고](VALIDATION.md)의 고정 노드 ID 후속 결과를 따른다.

## 재사용 우선순위와 판단 근거

초기 범위는 고정 backlog와 한 개의 검증된 executor profile로 제한한다. 공통 admission과
검증/완료 경로를 한 번 연결한 다음 다른 native adapter의 한도·취소·재개 계약을 인수한다.
여러 executor를 처음부터 동시에 활성화하면 실패 원인과 비용 표본을 분리하기 어렵다.
이는 유지보수 비용을 줄이는 설계 판단이며 측정된 최대 ROI라는 주장은 아니다.

W2에는 기존 `test_phase_runs`의 idempotency와 resource lock을 재사용할 수 있다.
검사 시작과 resource lease를 한 transaction에 기록하고, 현재 owner의 유효한 lease를
확인한 결과만 완료하도록 저장 경계를 보강했다. 완료 receipt 재전송은 멱등이며
다른 terminal evidence와 과거 owner의 늦은 완료를 거절한다. 자세한 API 계약은
[검사 실행 수명](../test-phase-run-lifecycle.md)을 따른다. 새 table/queue는 추가하지 않았다.
공개 `start_test_phase_run`은 상태와 lock만 기록하고 검사 프로세스를 시작하지 않는다.
`complete_test_phase_run`의 제출 evidence도 자체 진위 검증이 아니다. 보호된 check registry,
불변 candidate, 실제 비모델 실행/수집, 명시한 mandatory coverage와 성공 CAS는 별도의
server-owned `checks` 모듈을 통해 이 저장 소유자에 연결했다. 첫 Linux 파일 과제 adapter는
실제 checker HTTP와 재시작 인수를 통과했다. [보호 검사 계약](MANAGED-CHECKS.md)과
[최신 검증](VALIDATION.md#w2-첫-adapter)의 범위를 따른다. 일반 빌드·프로젝트 전체 suite·
다른 OS까지 인수한 것은 아니며 단순 endpoint 호출을 W2 완료로 집계하지 않는다.

W3는 기존 Campaign revision, 카드, Auto-Queue의 유한 run과 dispatch를 재사용한다.
원장은 checkpoint를 소유하고 `server/campaign_execution`이 보호된 실행 설정을
기존 Hub tick에 연결한다. `campaigns::handoff::publish`는 준비된 카드·계약·유한 batch를
원자적으로 기록한다. 보호 검사와 기존 FSM이 완료한 선행 산출물만 실제 파일로
전달하고, 실패 assertion의 재작업은 기존 dispatch 경로와 후보 한도를 사용한다.
일반 Campaign은 보호 registry에 등록되지 않으면 원장으로 남는다. pause/cancel은
새 공급과 실행 승격을 막지만 이미 실행 중인 provider의 실제 정지 증거가 아니다.
[실행 계약](CAMPAIGN-EXECUTION.md)의 지원 범위와 [최신 검증](OPERATING-READINESS.md)을
따르며, 실제 모델 E1 인수와 운영 배포를 별도로 진행한다.
W4 분해·W5 observer·W6 advisor는 이 기본 여정의 선행 조건으로 추가하지 않는다.
