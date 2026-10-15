# v5 구현 검증과 운영 인수

기준일: 2026-09-28. 대상은 `AgentDesk_24x7_Reuse_Implementation_Deployment_Spec_v5_2026-09-25.md`,
base commit `9d34870a7e5ced95f9ad5b99259adb2f50726c20`의 v5 작업본이다.

**기존 Mac mini 루틴의 제한된 운영과 v5 전체 무인 운영 인수는 별개다.**
사용자가 지정한 Mac mini의 기존 Qwen/OpenCode 루틴을 내부 디스크 실행 경로와 제한된
프로필로 이관했다. 운영 DB 백업·실제 복원·migration 133 적용·네이티브 서비스 설치를
수행했다. 실제 루틴에서 발견한 설정/종료 판정 결함과 취소 후 helper 잔존 결함을 수정하고,
08:18 KST에 v9을 배포했다. 같은 운영 binary의 실제 Qwen lease를 만료시켜 별도 그룹
writer 중단·부모의 다음 명령 차단·무관 프로세스 생존을 검증했다. 이후 Routine timeout은
다른 종료 순서 때문에 고아 프로세스를 남겼다. 이를 수동 정리하고 모델 루틴을 중지한 뒤
v11의 공통 retry 제한과 Qwen 강제 종료 순서를 검증 중이다. v9의 취소 통과를 전체 종료
경로의 통과로 사용하지 않는다. v6의 Qwen 두 번과
OpenCode 한 번은 과거 완료 기록으로 보존한다. 그 업무 결과는 중복 방지 skip, 빈 큐
no-op, CI 차단에 대한 부분 검토이며 PR 검증/병합 성공으로 합산하지 않는다. 실행 증거는
[Mac mini 운영 기록](MAC-MINI-OPERATIONS-2026-09-28.md)을 따른다.

v5 전체의 **M1/E1/E4/E5 및 24시간 무인 운영 인수는 아직 완료되지 않았다.**
특히 Linux의 managed 실행 v1을 macOS/Qwen에서 지원한다고 광고하지 않는다.
기존 Routine의 timeout과 Qwen 자체 제한, OpenCode의 advisory steps는 서로 다른 계약이다.
선택 기능 W4~W6는 초기 운영에 추가하지 않았다.

## 문서와 제품 경로의 대응

| 범위 | 현재 구현과 재사용 | 남은 인수 범위 |
|---|---|---|
| W0 호환·기준 | 기존 provider registry, native CLI 식별, 보호 설정 snapshot, Linux UID/Landlock 경계 | macOS managed v1, 전체 network/IPC·credential 격리, 완전한 E5 bundle |
| W1 실행 수명 | PG admission/deadline/tool budget, 실행 identity, unknown 점유 보존 | 실제 provider 종료와 잔존 writer 격리의 인증된 정산, 재부팅과 DB 장애 호환성 |
| W2 검사→완료 | 불변 후보와 파일, 보호 checker, 실제 subprocess, receipt, 승인, FSM CAS, 재시작 재대조 | 범용 프로젝트 검사 adapter, 실제 모델 수정 여정, 추가 crash 경계 |
| W3 목표→실행 | 기존 tick→Auto-Queue→canonical dispatch/FSM, 보호 registry, 전역 task key, 유한 batch/rework, 승인 파일의 후속 materialization | 실제 모델이 구현한 A→B/C 운영 여정 |
| W4~W6 선택 기능 | 기존 plan/Routine/sidecar 기반은 유지 | 보호된 분해·조언·원자적 적용은 OFF |
| W7 운영 | Mac native build/기동/재시작, 실제 PG 복원, 기존 launchd 교체, 기존 여섯 루틴 재사용 | 로그인 전 실행, OS 재부팅, 무개입 24시간 측정 |

서버의 `campaign_execution`이 기존 30초 tick에서 실행 가능한 목표를 공급한다.
`db/campaigns/handoff`는 카드·검사 계약·유한 batch를 한 transaction으로 기록한다.
`checks::completed_artifact`에서만 `VerifiedArtifact`를 생성하고, 검증한 실제 파일과
hash manifest를 후속 입력으로 동결한다. raw done/합성 digest만으로 성공을 인정하지 않는다.
설정과 지원 범위는 [CAMPAIGN-EXECUTION.md](CAMPAIGN-EXECUTION.md)를 따른다.

`execution_capacity::recovery`는 정체 실행을 관측해도 `unknown`을 보존한다.
인증 없이 문자열만 받아 실행 용량을 반환하던 helper를 제거했다. TTL을 종료 증거로
사용하지 않으며, 실제 provider 정지·잔존 writer 격리·인증된 정산은 E4의 미완료 항목이다.

## 실제 실행에서 발견하여 수정한 문제

- 검사 불가능한 review 카드 16개가 batch를 독점하던 기아를 적격 query와 회전으로 수정했다.
  거절 상태는 유지하며 독립 후보가 진행한다. Hub 강제 종료 뒤 checker가 종료되고,
  실행권의 실제 만료 후 같은 후보가 한 번만 재검사되는 경로도 검증했다.
- Campaign이 조회상 active여도 실제 공급자가 없던 DB helper만의 상태를 기존 tick과
  연결했다. A의 오답은 bounded rework, 독립 C는 별도 완료, B는 승인된 A 파일을 읽는다.
- FSM의 취소 경쟁에서 generation이 NULL인 카드가 잘못 전이될 수 있어 카드 조회부터
  `FOR UPDATE` transaction에 포함했다.
- routine이 전달된 오류 문자열을 성공으로 받아들이는 경로를 수정했다. bridge가
  transcript와 같은 저장에 typed outcome을 남기며, routine은 해당 attempt의 실패를
  텍스트보다 먼저 판단한다. analytics flush 지연, 이전 시도, 전달 대기는 별도 처리한다.
- Qwen/Codex/Claude의 응답 전 process/tmux 종료는 공통 typed `Error`다. 사용자가 보는
  종료 알림을 `Done`으로 만들지 않는다. 이미 실제 결과가 전달된 뒤 Codex 재사용 세션만
  사라진 경우에는 완료 결과를 유지하고 handoff를 발행하지 않는다.
- Qwen의 per-turn 도구 제한은 운영 system settings를 확장한다. 모델·시간·호출 한도와
  deny 설정을 보존하며, 잘못된 base 설정은 실패한다. 임시 overlay는 Unix 0600이며 0700 부모 디렉터리에 둔다. CLI가 파일을 다시 쓰더라도
  부모 접근 제한이 유지되며 종료 시 정리된다.
  오래 실행 중인 tmux에도 해당 system settings 경로를 명시적으로 전달한다.
- persistent wrapper의 `result/is_error`를 즉시 전달하고 종료 결과로 처리한다. 오류 후에도
  wrapper가 살아 있다는 이유로 턴이 계속 대기하거나 빈 `Done`을 합성하지 않는다.
- 설정의 읽기·저장은 OpenCode native 도구 이름을 보존한다. Claude 정규화가 `edit`를
  `Edit`로 바꾸거나 `list`/`todowrite`를 제거하던 결함을 수정하고 실제 YAML 재로딩을 검증한다.
- Windows에서 만든 macOS tar가 실행 권한을 잃는 문제를 package 생성 단계의 POSIX mode
  지정으로 수정했다. verifier는 실행할 수 없는 native binary/shell 파일을 거절한다.
- ordinary OpenCode의 명시적 보조 권한과 뒤따르는 전역 거절·승인 요구를 native 순서로
  보존한다. global allow로 보조 권한을 추가하지 않으며 managed read/edit 계약은 계속
  보조 권한을 거절한다. 일반 운영의 권한 수정이 managed 격리를 약화시키지 않도록 검사한다.
- terminal result에서 처음 받은 Qwen session ID를 runtime handoff가 dirty 상태와
  비교해 거절하던 문제를 실제 저장 기준 전달로 수정했다. 같은 턴의 경쟁 handoff에 대한
  저장 거절도 유지한다. 이 경로를 포함한 bridge 전체 회귀를 검증 범위에 추가했다.
- 소스 구조를 확인하는 기존 bridge 검사 3개는 CRLF 입력을 LF로 정규화한다.
  검사하는 mailbox 소유권·completion 대기·watcher pin 계약은 그대로 유지한다.
- 실제 upstream routine 실패 뒤 별도 프로세스 그룹의 helper가 계속 push한 반례를
  보존했다. Qwen wrapper의 PID/identity와 기존 CancelToken generation fence를 연결하고,
  소유한 부모와 자식을 먼저 멈춘 후 정리하여 종료 중 부모의 다음 명령 진행을 막는다.
  복구 및 호환 세션 연결도 이미 발행한 소유권을 유지한다. 이미 분리된 모든 writer의
  OS 격리를 증명한 것은 아니며 실제 provider 취소 검증과 전체 E4 인수를 구분한다.
- lease 갱신의 일시적 DB 오류는 마지막으로 확인한 유효시간 안에서만 재시도한다.
  실제 소유권 거절·기한 만료는 취소하며 SQL의 nonce/유효시간 조건을 유지한다.
  실제 PG pool을 잠시 점유한 뒤 정상 갱신과 소유권 삭제 후 취소를 한 여정으로 검증한다.

## 검증 증거의 범위

최종 v9 수정본은 **1,680 passed / 0 failed / 0 ignored**, 648.96초다.
필터 밖 8,060개는 실행하지 않았다. 정적 inventory 9,749개와 Linux runtime 목록은
플랫폼 조건이 다르며, inventory 전체를 실행한 것으로 합산하지 않는다.

- source capture: 1,998개 입력,
  `90ddad65ec282a4de93b375eb11339d1950146329810dd58d73b3ce16bcacdc2`
- Linux binary: `db9491401d8d9ec3b9590278b19d68ff63d9c4271234ad02d4311d64f4b4fefb`
- 증거: `target/24x7-native/resume-20260928-acceptance-v9/OPERATING-READINESS-RESULT.json`
- 소스·binary·로그·검사·정리 기록 86개를 감사하여 `evidence_valid=true`,
  `operational_ready=false`를 확인했다. 개발 검증의 통과를 운영 전체 인수로 올리지 않는다.

아래 여정은 모두 같은 최종 Linux binary와 실제 Hub/PG/checker로 실행했다.
구현 응답은 fixture이며 실제 모델 업무와 분리한다.

| 여정 | 최종 결과 |
|---|---|
| 보호 checker HTTP/FSM | 9조건, 2기동, 95.845초 |
| 부적격 카드 뒤 독립 과제 | 2조건, 35.403초 |
| 영구 거절 후보 뒤 독립 과제 | 2조건, 95.493초 |
| 오답 뒤 새 후보 | 3조건, 95.539초 |
| Hub 강제 종료와 실제 재검사 | 4조건, 2기동, 127.064초 |
| Campaign 공급·A rework·C·B 실제 파일 소비·재시작 | 5조건, 3기동, 216.741초 |

취소·갱신 집중 회귀 24개와 권한·handoff 집중 회귀 47개도 통과했으며 위 1,680개에
포함되므로 더하지 않는다.
Mac native 취소 검사 16개와 Qwen 새 실행/복구 identity 검사 3개도 통과했다.
배포 binary는 `fdcb7d18034676885764580b01c2d4f83d6f210fad49901a20ab428a1e06339e`다.
격리 DB 2기동 canary와 운영 Qwen의 실제 취소 실험은 별도의 증거다.
정책 161개, 증거 감사 반례 24개, 배포 패키지 12개, Mac profile 3개는 v2의 통과 기록이다.
해당 구현·테스트 입력의 hash가 변하지 않았음을 대조해 재사용했고, 재실행했다고 표시하지 않는다.
출처와 변경 입력 목록은 `inherited-v2/provenance.json`에 있다.
PG lane은 902개/133파일이며 누락·분류 gate 위반은 0이다. Rust format과 diff 검사도
통과했다. 유지보수성 hard gate 10개가 통과했고 baseline을 확대하지 않았다.
기존 parent-test 경고 4개, route 경고 10개는 남아 있으므로 저장소 전체 부채가 0이라는
의미는 아니다.

Mac 운영 script의 후속 보완은 core snapshot과 분리했다. 기존 Routine checkpoint로
PR batch를 회전하고, 두 repo wrapper와 apply helper가 공유 선택 함수를 사용한다.
diff 조회 오류를 빈 변경으로 오인하던 close 경로는 실패를 전달하도록 수정했다.
실제 원본 반례를 포함한 12개 회귀, JS 등록/tick 9조건, 셸 문법 3개와 live read-only
배치 순환을 확인했다. 세부 source/hash/설치 기록은 v6 `mac/routine-hardening`에 있다.
PR head 검증과 보고 정확성에 관한 새 지침은 모델에 대한 지침이며 강제 OS 계약이 아니다.

중간 snapshot `resume-20260928-acceptance`는 1,039 passed / 1 failed였다.
공통 parser가 terminal error를 즉시 전달한 뒤 Grok codec이 EOF에서 다시 오류를 낼 수
있었다. codec이 첫 terminal만 전달하도록 수정하고, 뒤늦은 success 및 중복 EOF가
기존 실패를 덮어쓰지 않는 회귀까지 검증했다. 최초 실패 로그를 보존한다.
이전 970개 snapshot과 Campaign timeout 반례도 원본 그대로 남겼다.
v3의 1,041 통과/1 실패는 ordinary 보조 권한을 managed에도 넘긴 반례였고 모드를 분리해
수정했다. v4의 1,630 통과/3 실패는 기존 소스 구조 검사의 CRLF 반례였다. 조건을 유지하고
입력을 정규화한 뒤 v6의 전체 회귀에서 통과했다. v5의 중간 Linux 빌드는 새 테스트 수정본
검증을 위해 취소했으며 통과 기록으로 사용하지 않는다.

v6의 1,633개 통과 이후 실제 Mac upstream run에서 취소 뒤 helper가 계속 실행하는
결함이 발견됐다. v7 넓은 회귀는 부모의 후속 명령 진행 반례 때문에 1,678 통과/1 실패였고
배포하지 않았다. 부모와 확인된 자식을 먼저 멈추는 v8은 1,679개 통과했다. v9은 복구 경로의
generation 보존까지 포함하며, 이전 성공/실패 기록을 최종 수에 합산하지 않는다.

## 구조 품질 판단

| 기준 | 구현과 확인 근거 | 한계 |
|---|---|---|
| 응집도 | 실행 수명, checker, Campaign handoff, Qwen settings, OpenCode permissions를 각 소유 모듈로 분리 | 기존 저장소의 baseline 경고는 남음 |
| 재사용 | 기존 scheduler/tick, Auto-Queue, provider registry, dispatch/FSM, Routine 원장을 사용 | 신규 범용 scheduler나 모델 HTTP client 없음 |
| 확장성 | provider 공통 수명과 검증된 adapter capability를 분리하고 미지원 조합을 거절 | 새 OS/provider는 실제 계약 인수 후 확장해야 함 |
| 직관성 | terminal Error/Done, 같은 attempt 결과, 실제 파일·receipt·지원 범위를 명시 | timeout 후 자동 retry는 외부 효과 중복 위험 때문에 꺼짐 |

## 운영 여정 판정

| 여정 | 현재 판정 |
|---|---|
| E1 A→B와 독립 C | 제품 공급·승인 파일 소비·검사/FSM·제한 재작업은 실제 Hub에서 통과. 구현 응답은 fixture이므로 실제 모델 E1은 미인수 |
| E2/E3 점검·조언 적용 | 선택 기능 OFF |
| E4 한도·정지·해소 후 진척 | native/mock 한도와 unknown 보존은 통과. 인증된 backend 종료 정산과 writer 격리는 미완료 |
| E5 설치 재현 | Mac native 배포와 별도 DB canary, 실제 backup/restore 수행. 완전한 bundle·독립 채널·OS 재부팅 여정은 미인수 |
| V36 24시간 | 실제 무개입 24시간 관측은 미완료. 빌드 시간과 fixture uptime을 포함하지 않음 |

`scripts/activation_24x7_gate.py`는 검증된 partial receipt만 감사한다. 실제 소스·binary·
로그·probe·정리 기록의 hash를 재계산하며 플래그 변경으로 운영 인수를 승격하지 않는다.
Mac의 실제 모델/서비스 증거는 별도 운영 기록에 유지하여 Linux fixture의 scope와 섞지 않는다.

## 과거 기록

`target/24x7-native/OPERATING-READINESS-RESULT.json`의 373개 회귀와 1,971개 source capture는
이전 snapshot이다. 원본을 보존하며 현재 작업본에 대한 검증으로 재사용하지 않는다.
초기 Campaign 공급 불능 반례는 `resume-20260927-campaign-v2`에 보존되어 있다.
존재하지 않던 `resume-20260927-audit/CONTINUATION-RESULT.json` 참조는 제거했다.
