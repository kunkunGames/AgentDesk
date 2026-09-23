# 허브, 실행 노드와 기능 모드

대시보드와 운영 안내에서는 역할을 **허브 / 실행 노드**, 기능 모드를
**전체 기능 / 실행 전용**으로 표시한다. YAML과 설정 API의 정식 값도 `hub`와 `runner`다.

| 구분 | 설정 값 | 한국어 표시 | 영어 표시 |
| --- | --- | --- | --- |
| 현재 역할 | `hub` | 허브 | Hub |
| 현재 역할 | `runner` | 실행 노드 | Runner |
| 역할 선택 정책 | `auto` | 자동 선택 | Automatic |
| 기능 모드 | `full` | 전체 기능 | Full features |
| 기능 모드 | `runner` | 실행 전용 | Execution only |

허브는 중앙 운영과 작업 배정을 담당하며 에이전트를 직접 실행할 수도 있다.
실행 노드는 배정된 작업을 실행하는 참여 장비다. 기능 모드는 해당 프로세스에서
어떤 모듈을 시작하는지 결정한다. 전체 기능 모드를 사용하더라도 현재 허브가
아닐 수 있으므로 두 항목을 별도로 표시한다.

화면의 현재 역할은 등록 정보의 `effective_role`을 기준으로 한다. 설정된 역할이나
`runtime_profile=full`만 보고 허브로 표시하지 않는다. 알 수 없는 역할·기능 모드는
미확인 상태로 표시한다. `standby` 값이 전달되면 대기로 표시하며 승계 가능 여부를
추정하지 않는다.

`cluster.enabled=false`이면 운영 화면에 **이 컴퓨터 · 단독 운영**을 표시한다.
등록 노드 수가 한 개라는 이유만으로 단독 운영으로 판단하지 않는다. 클러스터가
꺼진 상태에서는 이전 등록 장비의 카드와 원격 제어를 표시하지 않는다.

## 장비와 에이전트 설정

- Mac mini: **허브 · 전체 기능**.
- Windows PC, Mac Studio 또는 Linux 장비를 실행 전용으로 연결: **실행 노드 · 실행 전용**.
- 에이전트 상세의 **우선 실행 장비**: 새 Discord 세션을 우선 시작할 장비.
- **기본 정책 사용**: 저장한 우선 장비를 해제하고 기존 배정 정책을 사용.

장비는 hostname을 먼저 보여 주고 노드 목록에서 `instance_id`도 확인할 수 있다.
선택·저장·라우팅에는 계속 `instance_id`를 사용한다. 기존 세션 소유권,
필수 실행 조건 및 새 세션의 대체 배정 규칙은
[우선 실행 장비](agent-execution-node.md)에 설명돼 있다.

## 설정과 일괄 전환

허브 장비는 `role: hub`, `runtime_profile: full`을 사용한다.
실행 전용 장비에는 다음 설정을 사용한다. `instance_id`는 세션 소유권을
가리키므로 역할과 무관한 장비 이름을 권장한다. 역할을 바꿀 때는 ID를 유지한다.

```yaml
cluster:
  enabled: true
  instance_id: windows-pc
  role: runner
  runtime_profile: runner
  intake_routing:
    enabled: true
    mode: enforce
```

역할은 `hub`, `runner`, `auto`만, 기능 모드는 `full`, `runner`만 허용한다.
이전 역할·기능 모드 이름의 입력 별칭은 제공하지 않는다.
역할은 공통 `ClusterRole` 타입으로 검증하며 오타를 `auto`로 취급하지 않는다.
`runtime_profile: runner`는 활성 클러스터, `role: runner`, 활성 intake routing을
요구한다. 기능 모드를 생략하면 기존과 같은 `full`이다. `execution_only`는 설정 값이 아니다.

이 전환은 전체 노드를 함께 중지하고 적용하는 스키마 변경이다. 서로 다른 세대의
바이너리를 동시에 실행하는 순차 배포는 지원하지 않는다. migration 0129가 등록 테이블을
`cluster_nodes`, MCP 등록 테이블을 `node_mcp_endpoints`로 바꾸고 역할 값을 변환한다.
실행 준비 정보는 schema 2를 사용하고 오래된 probe는 삭제한 뒤 각 노드가 다시 수집한다.
health 모듈 키는 `hub_services`, intake 기능 키는 `intake_runner`다.
이미 적용한 번호 있는 migration과 과거 검증 보고서는 기록 그대로 보존한다.

먼저 DB·설정·runtime 상태를 백업하고 모든 실행을 종료한다. 모든 노드를 중지한 뒤
같은 버전의 바이너리와 설정을 설치하고 DB migration을 실행한다. Hub를 먼저, Runner를
나중에 시작한다. 구버전으로 되돌릴 때는 바이너리뿐 아니라 DB 백업과 설정도 함께
복원해야 한다. 상세 절차는 [구성 및 이동 안내](hub-runner-topology-and-migration.md)를 따른다.

Windows 신규 설치에는 `install-windows-runner-firewall.ps1 -HubAddress ...`를 사용한다.
규칙 이름은 역할과 독립적인 `AgentDeskAPI-TCP-8791`이다. 기존 설치의 규칙 교체는
`migrate-windows-hub-runner-firewall.ps1`이 실행 파일·포트·허용 Hub 주소를 확인한 후
새 규칙을 생성·검증하고 이전 규칙을 제거한다. 입력한 설치 경로가 잘못되면 중단한다.
예약 작업 기본 이름은 `AgentDeskRelease`, DB 터널은 `AgentDeskRelease-DatabaseTunnel`이다.

대시보드의 역할·기능 모드·OS 표시는
[`nodeLabels.ts`](../../dashboard/src/lib/nodeLabels.ts)를 공통으로 사용한다.
설정 검증은
[`cluster_role.rs`](../../src/config/cluster_role.rs)와
[`runtime_profile.rs`](../../src/config/runtime_profile.rs)에 모은다.
설치 구성과 활성 기능은 [실행 전용 기능 모드](runner-runtime-profile.md)를 참고한다.
검증과 실기기 적용 내역은 [변경 보고서](../reports/hub-runner-terminology-2026-09-23.md)에 기록한다.
