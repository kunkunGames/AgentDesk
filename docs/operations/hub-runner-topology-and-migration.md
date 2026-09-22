# Hub와 여러 Runner 구성 및 장비 이동

하나의 Hub와 여러 Runner가 같은 PostgreSQL을 사용한다. AgentDesk는 OS별 공통
바이너리를 배포하고 설정으로 역할과 기능 범위를 선택한다. Runner 전용 바이너리를
별도로 빌드하지 않는다. 새 Runner를 추가할 때 에이전트 DB를 복제하거나 별도
Discord gateway를 시작할 필요가 없다.

| 장비 예시 | 고정 장비 ID | 역할 | 기능 모드 | 담당 |
| --- | --- | --- | --- | --- |
| Mac mini | `mac-mini` | `hub` | `full` | Discord 수신, 대시보드/API, 중앙 운영, 로컬 실행 |
| Windows PC | `windows-pc` | `runner` | `runner` | Windows CLI와 작업 폴더에서 실행 |
| Mac Studio | `mac-studio` | `runner` | `runner` | macOS CLI와 작업 폴더에서 실행 |
| Linux 장비 | `linux-build` | `runner` | `runner` | Linux CLI와 작업 폴더에서 실행 |

Runner 수를 두 대로 제한하는 코드는 없다. 실제 동시 처리량은 노드별
`execution_slots`, provider/API 한도, DB·네트워크 자원에 의해 제한된다. Mac/Linux는
지원 backend와 CLI 설치 상태를, Windows는 ProcessBackend와 설치된 Windows CLI를
사용한다. OS가 다른 장비에서 모든 작업을 동일하게 실행할 수 있다는 의미는 아니다.
필수 OS·도구·저장소·backend는 [실행 조건](execution-requirements.md)으로 지정한다.

## 장비 추가

각 장비에 해당 OS/아키텍처의 같은 버전 패키지를 설치하고 고유한 ID를 지정한다.
다음은 Mac Studio의 예시이며 실제 주소와 실행 슬롯은 장비에 맞게 선택한다.

```yaml
cluster:
  enabled: true
  instance_id: mac-studio
  role: runner
  runtime_profile: runner
  api_base_url: http://192.168.1.150:8791
  execution_slots: 2
  intake_routing:
    enabled: true
    mode: enforce
```

공유 PostgreSQL 연결, API 인증과 신뢰할 노드 주소, 필요한 provider bot 설정,
CLI 로그인, 에이전트 prompt, 작업 폴더를 준비한다. 이 실행 전용 모드는 Discord REST
credential과 DB 연결을 사용한다. 별도의 무권한 실행 샌드박스나 token 없는 protocol은 아니다.
방화벽에는 실제 통신 상대와 포트만 허용한다. API와 PostgreSQL을 인터넷에 공개할 필요는 없다.

노드 등록의 OS·역할뿐 아니라 provider 준비 상태, intake poller, 실행 슬롯을 확인한다.
등록 성공만으로 CLI 인증과 실제 응답까지 검증된 것은 아니다. 새 장비를 추가한 뒤 해당
장비를 우선 장비로 설정한 테스트 에이전트로 Discord 수신 → 실행 → 답변을 확인한다.

## 에이전트별 장비 선택

대시보드의 에이전트 상세 → **우선 실행 장비**에서 각 에이전트의 장비를 선택한다.
`agents.default_execution_node_id`에 역할이 아닌 장비 ID를 저장한다. 우선값을 해제하면
기존 기본 정책을 사용한다. [상세 배정 규칙](agent-execution-node.md)은 다음 순서다.

1. 기존 세션 소유 장비.
2. 해당 채널/스레드의 명시적인 장비 선택.
3. 에이전트의 준비된 우선 장비.
4. 새 세션에 한해 필수 조건과 용량을 충족하는 대체 장비.

Windows가 꺼졌을 때 소유자가 없는 새 대화는 호환되는 Mac에서 시작할 수 있다.
Windows 필수 조건을 지정한 작업은 Mac으로 우회하지 않는다. 이미 Windows가 소유한
세션은 장비가 꺼졌다고 자동으로 복제해 실행하지 않는다. provider 세션 파일과
작업 폴더, 아직 끝나지 않은 파일 변경을 공유 DB만으로 안전하게 이동할 수 없기 때문이다.

## Hub 장비 교체

역할 교체와 DB 이동은 별도 작업이다. 장비 ID를 유지하면 기존 세션 소유권과
에이전트 선호값을 바꿀 필요가 없다. 예를 들어 Mac Studio를 Hub로 승격해도
`mac-studio`라는 ID를 유지한다.

1. 실행 중인 turn과 미완료 intake를 확인하고 종료를 기다린다. Hub, Runner의 설정과
   prompt·runtime 상태 및 PostgreSQL을 일관된 시점으로 백업한다.
2. 새 Hub에 같은 버전의 바이너리, 전체 기능 자산, 필요한 Discord gateway와 API 설정,
   DB 연결 및 Hub 작업에 필요한 계정을 준비한다. Runner 기능 모드만으로는 Hub를 맡지 못한다.
3. 기존 Hub를 중지한다. 새 Hub를 `role: hub`, `runtime_profile: full`로 시작한다.
   DB의 Hub lease 획득과 gateway 연결, API, DB 상태를 확인한다.
4. 기존 Hub를 계속 실행 장비로 사용할 경우 같은 ID를 유지하고 `role: runner`,
   `runtime_profile: runner`로 변경한 뒤 시작한다. 기존 대화는 원래 장비 소유권을 유지한다.
5. 대시보드 접속 주소, 운영 도구의 API 주소, 허용 Hub 주소가 바뀌었다면 갱신한다.
   새 Hub에서 각 Runner API 접근과 Discord 왕복을 검증한다.

`role: auto`와 전체 기능을 이용한 Hub lease 경쟁은 가능하지만, DB·계정·공유 파일의
가용성까지 자동으로 해결하지는 않는다. 단일 Hub와 필요에 따른 계획된 교체가 초기
운영 비용과 문제 분석 범위를 줄인다. 동시 Hub를 여러 개 운영하는 구성으로 해석하지 않는다.

## DB도 다른 장비로 이동할 때

현재 DB가 Mac mini에 있다면 Hub만 옮겨도 Mac mini를 끌 수는 없다. DB 호스트를
유지할지 별도 서버로 옮길지 먼저 결정한다. DB 이동은 다음 순서로 수행한다.

1. 모든 AgentDesk 노드를 중지하고 남은 실행·쓰기 연결을 확인한다.
2. 기존 DB를 `pg_dump -Fc`로 백업하고, 새 PostgreSQL에 `pg_restore`로 복원한다.
   DB 버전과 extension, 사용자 권한을 확인한다. 비밀값을 문서나 로그에 기록하지 않는다.
3. 모든 노드의 DB 연결을 새 주소로 바꾼다. SSH 터널을 사용하는 Windows는
   `DatabaseSshAlias`와 목적지 포트도 바꾼다. 설정 파일 한 대만 변경해서는 안 된다.
4. 에이전트 수, 설정, 세션 소유 장비, migration 버전, 최근 outbox 상태를 대조한다.
5. 새 Hub부터 시작하고 Runner를 시작한다. 새 DB에서 실제 메시지와 응답 기록을 확인한다.

동시에 두 DB를 쓰는 이중 운영은 하지 않는다. 실패 시 모든 새 노드를 멈추고 백업과
기존 연결로 되돌린다. 전환 후 새 DB에 생긴 쓰기를 무시하고 과거 백업을 복원하면
그 구간의 데이터를 잃으므로, 쓰기 재개 여부를 기준으로 복구 계획을 판단한다.

## 장비 ID를 바꿔야 하는 경우

역할 교체만으로는 ID를 바꾸지 않는다. 기존 ID 자체를 정리해야 할 때만 migration
0129의 `rename_cluster_node_identity`를 사용한다. 모든 노드를 중지하고 백업한 뒤
하나의 DB transaction에서 실행한다.

```sql
BEGIN;
SET LOCAL lock_timeout = '10s';
SELECT rename_cluster_node_identity('old-device-id', 'windows-pc');
COMMIT;
```

함수는 등록 정보, MCP 외래키, 세션·intake 소유권, lease·예약 정보, 에이전트 우선 장비,
실행 조건의 `nodes` 목록을 함께 갱신한다. 다른 JSON 내용과 provider 세션 ID는 유지한다.
실행 중인 lease/intake, 이미 있는 목적지 ID, 잘못된 ID 또는 실행 중인 Hub가 있으면
중단하며 SQL 오류 시 transaction 전체가 되돌아간다. 반환값은 변경한 참조별 행 수다.
각 장비 YAML의 `cluster.instance_id`도 같은 새 값으로 맞춘 뒤 시작한다.
다른 장비에 저장된 `cluster.nodes.<기존 ID>` 키, `gateway_preferred_instance_id`,
장비별 `blackout_windows` 키와 운영 설정의 장비 참조도 함께 확인한다. DB 함수는
YAML을 수정하지 않는다. 특히 `cluster.nodes` 키를 남겨 두면 새 ID에 대한
`trusted_forward_origin` 설정을 찾지 못해 원격 실행과 세션 제어가 차단된다.
키를 옮길 때 기존 신뢰 주소와 허용 정책은 유지하고, 기동 후 노드 API의
`forwarding_diagnostics.trust_validated`와 `reachability_verified`를 확인한다.
CLI 인증 파일, 작업 폴더, provider 세션 파일을 다른 장비로 옮기는 함수는 아니다.

## 기존 설치를 새 이름으로 일괄 전환

이름 전환 migration 0129는 구형·신형 바이너리의 혼용을 지원하지 않는다.
모든 turn을 정리하고 전체 노드를 중지한 뒤 DB 백업, migration, 전체 바이너리와 설정
교체를 완료한다. Hub를 먼저 시작하고 Runner의 schema 2 준비 정보가 갱신되는지 확인한다.
이전 번호의 migration과 과거 검증 보고서는 checksum·감사 기록 보존을 위해 수정하지 않는다.
구버전으로 복구하려면 모든 노드를 중지하고 DB·설정·바이너리를 함께 복원해야 한다.

기존 단일 머신 사용자는 cluster를 생략한 `enabled=false`, `runtime_profile=full`로
계속 운영한다. 에이전트의 우선 장비는 기본 `null`이다. 새 용어를 사용하는 다중 장비
배포를 선택하지 않았다는 이유로 기존 단일 머신에 Runner 설정을 요구하지 않는다.

Windows 방화벽의 `RuntimeRoot`는 실제 설치 폴더다. 예를 들어
`C:\Users\12336\.adk\release`에서 사용자 이름과 `.adk` 사이의 `\`를 생략하면
다른 경로가 되며 기존 규칙 검증을 통과하지 않는다.
