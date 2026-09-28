# Mac mini Qwen/OpenCode 루틴 운영 전환

기준일: 2026-09-28 KST. 대상 SSH alias는 `agentdesk-mac-mini`, runtime root는
`/Users/kunkun/.adk/release`, 서비스는 기존 `com.agentdesk.release` LaunchAgent다.
사용자가 기존 두 실행기의 routine을 24/7 형태로 옮기고 실제 운용하도록 지정했다.
새 scheduler나 LLM client, 별도 목표 원장을 만들지 않았다.

**v9 바이너리의 lease 상실 취소는 통과했지만, 이후 실제 Routine timeout에서
고아 Qwen 프로세스를 발견했다. 모델 루틴을 수동 중지하고 v11 수정본을 검증 중이다.**
v9은 08:18:25 KST에 배포했으며 실제 Qwen의 lease 상실 시 도구와 별도 그룹 writer의
동시 중단을 확인했다. 이 결과가 Routine timeout 경로의 종료까지 보장하지는 않았다.
Linux 회귀 1,680개와 Mac native 취소 16개·Qwen identity/복구 3개가 통과했다.
v6에서 완료했던 Qwen 2회와 OpenCode 1회는 과거 버전의 별도 업무 증거다.
그 결과는 중복 방지 skip, 빈 PR 큐 no-op, CI 실패로 남겨둔 부분 검토이며,
OpenCode가 기본 브랜치 검사를 PR 검증처럼 설명한 부분은 인수 근거에서 제외했다.
무개입 24시간, 로그인 전 기동, OS 재부팅 인수는 아직 완료하지 않았다.

## 유지한 루틴과 바꾼 실행 경계

| 기존 루틴 | 실행기 | KST cron | 실행 범위 |
|---|---|---|---|
| qwen-skill-sync-daily-backup | Qwen | `0 3 * * *` | 기존 daily wrapper, 동일 날짜 중복 금지, 메모리 후보 무승인 기록 금지 |
| qwen-agentdesk-upstream-sync-daily | Qwen | `0 5 * * *` | 기존 merge helper, fork 변경 보존, main 강제 동기화/배포/DB 수정 금지 |
| qwen-agentdesk-daily-pr-review | Qwen | `0 8 * * *` | AgentDesk PR 검토와 기존 close-empty 규칙 |
| monitoring-agentdesk-pr-merge-ready-daily | OpenCode | `0 13 * * *` | live head·checks·review gate를 통과한 PR만 병합 |
| monitoring-monolith-daily-pr-review | OpenCode | `30 10,15,20,1 * * *` | 기존 Monolith 검토/수정/병합 조건 유지 |
| monitoring-working-watchdog | deterministic script | `0,30 * * * *` | 기존 working 상태 점검 |

ID, agent 배정, cron, 기존 checkpoint와 fresh 실행 전략을 유지했다. 루틴 API로
agent 작업 timeout을 1,800초, watchdog을 120초, 자동 retry를 0, fallback을 없음으로
설정했다. 실패하면 기존 루틴의 pause 정책을 따른다. 작업 중 실패를 무조건 재시도하여
같은 댓글·병합 등의 외부 효과를 반복하지 않는다. 수동 검증 재개 전에 live 상태를 확인했다.
기존 실패 후 자동 재개 설정은 유지했다. 실제 기록에서 failure pause 약 31분 뒤 enabled로
복귀했으며, 이는 실패한 run을 성공으로 바꾸거나 같은 run을 즉시 retry하는 동작이 아니다.
현재 코드는 failure 이유에만 backoff를 적용하고 수동 pause는 자동 해제하지 않는다.
PR helper의 기존 환경변수로 **한 번에 PR 1개**를 공급한다. 관측한 모델 지연과 30분 제한에 맞춘
운영 batch이며, 나머지는 기존 시간표의 다음 실행에 남긴다. helper, prompt, checkpoint의
batchSize를 모두 1로 맞췄다.
Qwen PR의 10회, OpenCode PR의 12회 도구 호출은 prompt의 작업 목표다. 대표 실패 CI log는
하나만 조사하고, 집중 검토를 끝내지 못하면 남은 범위를 밝힌다. cleanup·live head 재확인·
최종 보고 시간을 남기며 기존 검사·병합 조건을 우회하지 않는다. 이 목표치를 native hard cap으로
표현하지 않는다. 두 OpenCode script는 Node 문법 검사와 실제 등록/tick 호출로 batch 1 및
native skill 사용 계약을 확인했다.

05:14 KST에 추가한 운영 script 보완은 기존 `runCount` checkpoint를 `batchSequence`로
전달한다. 공유 `routine-pr-preflight.py`가 오래된 순서를 기준으로 batch를 회전하므로,
변경되지 않는 차단 PR 하나가 매번 첫 자리를 점유하지 않는다. 같은 run의 재조회에는 같은
sequence를 사용한다. 두 repo wrapper와 기존 apply helper가 같은 선택 함수를 사용하며,
apply helper의 세 번째 인자는 동일 sequence다. 새 원장·상태 파일·scheduler는 없다.
실제 GitHub read-only 조회에서 sequence 0은 PR 2873, sequence 1은 PR 2874를 선택했다.
조회 당시 Monolith 열린 PR은 8개였고, AgentDesk에는 새 PR 2202가 생겨 1개였다.

추가로 `open-pr-apply.sh`가 diff 조회 실패를 `|| true`로 지운 뒤 빈 diff로 판단하던
결함을 수정했다. 격리된 fake GitHub fixture에서 원본은 조회 실패 후 comment/close를
호출했고 수정본은 두 호출 없이 nonzero로 중단했다. 실제 빈 diff에 대한 기존 정책은
유지했다. **운영 script 회귀 12개, 세 JS의 등록/tick 9조건, 셸 문법 3개**가 통과했다.
검증·설치·원본 백업·live 조회는 `acceptance-v6-operations-20260928/routine-hardening`과
로컬 v6 `mac/routine-hardening`에 기록했다. 이는 아래 실제 모델 run들이 끝난 뒤 적용한
보완이며 새 prompt를 실제 모델이 끝까지 준수한 증거로 소급하지 않는다.

PR 지침은 live head와 일치하는 worktree에서 수행한 검사만 PR 검증으로 인정하고,
검사 SHA/명령/종료 코드를 함께 보고하도록 명시했다. CI의 원인은 check-run annotation으로
확인하며 Codex review 크레딧을 Actions 결제 근거로 쓰지 않는다. prompt는 모델 지침이므로
OS 권한 경계나 보호 checker의 강제 검증을 대체하지 않는다.

사용한 native 버전은 Qwen **0.22.3**, OpenCode **1.18.25**, GitHub CLI **2.92.0**,
내부 Rust **1.94.1**이다. 모델은 기존 인증 binding을 사용하는 **GLM 5.3 Flash**이며,
Qwen model ID는 `z-ai/glm-5.3-flash`, OpenCode는 `deepseek/z-ai/glm-5.3-flash`다.
CLI `--version`, 인증된 GitHub 조회, 모델의 실제 completion, AgentDesk 실제 루틴은 각각
별도의 검증이다. CLI marker 응답만으로 루틴 완료를 주장하지 않는다.

## 내부 디스크와 프로필

- GitHub CLI 실행 파일, Rust toolchain/Cargo cache, XDG cache, Qwen 프로젝트 기록 경로,
  OpenCode snapshot/tool-output 경로를 `~/.adk` 내부 디스크 영역으로 연결했다.
- `release/reports`도 외장 SSD 링크여서 daily helper의 `Path.write_text`가 멈췄다.
  링크를 `reports.external-20260928`로 보존하고 같은 운영 경로에 내부 0700 디렉터리를
  만들었다. 실제 daily helper는 0.09초에 종료했고, 03시 이전이라는 이유로 `skipped`를
  기록했다. 동기화 완료로 계산하지 않으며 날짜 완료 marker도 생성하지 않았다.
  실제 source/provider 경로를 도는 `sync_provider_skills.py --dry-run`도 0.15초/exit 0으로
  확인했다. 변경 계획의 검증이며 provider skill의 실제 교체·03시 백업 완료는 아니다.
- 기존 외장 SSD 링크는 `.external-20260927` 이름으로 보존했다. 과거 프로젝트 기록의
  전체 복사는 `Interrupted system call`로 완료하지 못했으므로, 현재 경로는 새 내부 기록이다.
  과거 기록을 복원했다고 표시하지 않으며 기존 외장 원본을 제거하지 않았다.
- repo는 `~/.adk/repositories/agentdesk`와 `~/.adk/repositories/monolith`다.
  기존 외장 repo와 작업 파일을 강제 이동/초기화하지 않았다. 새 clone과 upstream remote를
  실제 조회로 확인했다. 미완료 최초 clone도 별도 이름으로 보존했다.
- `~/.adk/profiles/routine24x7-20260928`의 CLI wrapper는 기존 인증을 그대로 사용한다.
  비밀값을 추출하거나 report에 넣지 않고 endpoint와 환경변수 **이름**만 재사용한다.
  wrapper `umask 077`, 내부 cache/PATH/Git credential helper를 적용한다.

`scripts/stage_macos_routine_profile.py`는 이 프로필의 재현 도구다. 기존 profile을
덮어쓰지 않고 새 경로에 staging하며, 서비스 활성화나 scheduler 설치는 하지 않는다.
Qwen 제한은 1,800초, 60 tool calls, 30 turns, session token limit 180,000이다.
`agent` 도구를 제외하며 subagent depth는 1이다. OpenCode는 기존 primary agent를 쓰고
steps 24, task deny, plugin 없음, MCP 비활성이다. **OpenCode steps는 hard tool budget이 아니다.**
1,800초 Routine 경계와 macOS에서 완전한 writer 격리를 보장하는 OS 계약도 같은 뜻이 아니다.

Qwen 0.22.3의 `action_stagnation`은 인자가 다른 셸 명령도 같은 tool name이 8번 이어지면
중단한다. 실제 PR 검토가 이 조건에 걸렸다. 설치된 `chunks/chunk-PZ66FRIC.js` 및 client
호출 경로를 확인하여 이 프로필에 `model.skipLoopDetection=true`를 적용했다.
해당 버전의 **동일 tool+args 연속 반복, 셸 조회 반복, 명시적 tool-call cap은 always-on**이며
이 설정으로 해제되지 않는다. 명시한 시간/호출/턴 한도를 유지한다. native 버전 변경 시
이 구분도 다시 인수해야 한다.

## 실제 실행에서 고친 결함

1. 환경변수의 CLI 경로보다 provider registry의 agent별 binding이 우선했다. 직접 CLI
   marker가 성공해도 실제 routine은 기존 실행 파일을 사용했다. 기존 `provider-cli run`
   명령으로 `qwen` agent와 `monitoring` agent에만 새 wrapper를 연결했다. 실제 launch
   receipt에서 새 경로와 버전을 확인했다. 다른 OpenCode agent를 전역 승격하지 않았다.
2. org agent뿐 아니라 channel override에도 이전 모델/workspace가 남아 있어 해당
   Qwen/monitoring binding을 함께 수정했다. monitoring 도구 목록은 native category다.
3. 설정 read/write가 OpenCode `edit`를 Claude `Edit`로 변경하고 `list`/`todowrite`를
   제거했다. provider별 정규화로 수정했다. 지원하지 않는 입력은 strict adapter가
   거절하도록 보존한다. YAML 저장→runtime reload 회귀를 추가했다.
4. Qwen의 per-turn 도구 설정이 operator system settings를 대체하여 제한을 지웠다.
   기존 설정을 읽고 core 도구만 교체하며 invalid/missing base는 실패하도록 수정했다.
   tmux 환경도 명시적으로 전달한다. CLI가 임시 파일을 0644로 다시 쓰는 동작이 있어
   0700 private parent를 추가하고 wrapper의 umask도 고정했다.
5. 세션 종료 알림을 `Done`으로 보내던 adapter를 typed error로 바꿨다. `result/is_error`
   수신 후 persistent wrapper의 종료를 기다리던 공통 reader도 즉시 실패를 전달한다.
   routine은 transcript와 함께 저장된 해당 attempt의 typed outcome을 먼저 판정한다.
6. Windows에서 생성한 macOS tar의 native 실행 권한이 0644여서 최초 설치가 실패했다.
   migration 이전 실패라 기존 서비스를 복구했다. packager가 POSIX mode를 명시하고
   verifier가 실행 권한을 검사하도록 수정하여 설치를 다시 수행했다.
7. OpenCode 1.18.25는 비활성 MCP에도 `context7*` 등의 category glob을 만든다.
   allowlist에 있는 native category와 문자 prefix가 확실히 겹치지 않는 패턴만 제외한다.
   나머지 도구는 첫 deny-all로 계속 차단하며, `r*`, 선행 wildcard, 불명확한 prefix는
   거절한다. native path/command 패턴 해석을 자체 구현하거나 전체 허용하지 않는다.
8. OpenCode가 외부 Codex 경로의 `git-help/SKILL.md`를 `read`로 여는 것은 native
   `external_directory` 경계에 거절됐다. 해당 세션의 `skill` category는 이미 허용되어
   있고 `opencode debug skill`에서도 `git-help` 등록을 확인했다. 두 OpenCode PR 루틴은
   native `skill` 도구로 로드하도록 맞췄다. 등록된 파일과 Codex 원본의 SHA-256이 같으며
   외부 디렉터리 권한을 추가하거나 스킬 사본을 새로 만들지 않았다. 등록 확인은 실제
   routine에서의 도구 호출 완료와 별도 증거다.
9. ordinary OpenCode session의 tool projection이 운영 설정의 `external_directory`와
   `doom_loop`를 누락했다. native의 기존 명시적 보조 규칙만 원래 순서대로 보존하고,
   빈 도구 목록과 global wildcard로 새 도구 권한을 만들지 않는다. Linux managed v1은
   기존 read/edit 계약에 맞게 이 보조 권한을 계속 거절한다. 첫 수정의 managed 회귀
   실패를 보존하고 모드를 분리했으며 실제 native budget/deadline 검사로 재검증한다.
10. Qwen의 terminal result에서 session ID가 처음 주어지면 runtime handoff가 수정 중인
    상태를 저장 기준으로 복제하여 자신의 완료를 `AuthorityPinned`로 거절했다.
    stream loop가 보유한 실제 저장 기준을 넘기도록 고쳤다. legacy tmux와 process 양쪽에서
    늦은 session ID를 저장하되 같은 턴의 경쟁 handoff를 덮어쓰지 않는 회귀를 추가했다.
11. daily skill wrapper의 PATH가 CLT Python 3.9.6을 선택하여 설치된 pytest를 찾지 못했다.
    기존 Homebrew Python 3.14.5/pytest 9.0.3을 검증하고 `~/.adk/runtime-bin/python3`에
    명시적으로 연결했다. 시스템 Python을 바꾸거나 의존성을 새로 설치하지 않았다.
    실제 routine PATH로 기존 regression 함수만 실행하여 **20 suite 통과, 0 실패,
    테스트가 없는 5 suite skip**을 확인했다. 동기화·push·메시지를 재실행하지 않았다.
12. PR batch를 1개로 줄였을 때 오래된 차단 PR이 계속 선택되는 문제를 기존 checkpoint와
    공유 선택 함수로 해결했다. GitHub diff 조회 오류를 빈 diff로 처리하던 별도 helper도
    실패를 그대로 전달하도록 수정하고, 실제 원본 반례와 수정본을 격리 fixture로 대조했다.
13. 내부 clone에는 `.gitattributes`의 generated inventory merge 지정만 있고 로컬 merge
    driver 등록이 없었다. 저장소의 기존 `setup-merge-drivers.sh`로 등록하고, upstream helper가
    실제 merge worktree를 만들기 전에 같은 초기화를 수행하도록 했다. 조회만 하는
    `--noop-check`는 driver 설정을 바꾸지 않는다. 원본 helper와 작업 파일을 보존했다.
14. upstream helper의 기본 dev 빌드는 현재 Mac에서 `sqlx_macros`의 `mis-aligned LINKEDIT
    string pool` 오류를 냈다. 저장소가 이미 정의한 `release-fast` 프로필과 내부
    `~/.adk/build/target-24x7`을 사용하도록 맞췄다. 기존 clean/diff/build/remote-head 검증,
    일반 push 및 push 뒤 SHA 확인은 유지한다. 자체 build gate를 건너뛰지 않는다.
15. `build_token.py`의 sccache 기본 경로와 Homebrew 캐시가 외장 SSD로 해석됐다.
    `~/.adk/runtime-bin/sccache`와 `brew`가 각각 `~/.adk/cache/sccache`,
    `~/.adk/cache/homebrew`를 명시하게 했다. sccache는 기존 daemon과 겹치지 않는 내부
    Unix socket, 40 GiB 용량, idle timeout 0을 사용한다. 실제 Rust compile/cache hit와
    Homebrew `--prefix opus` 0.376초/exit 0을 확인했다. 이전 외장 캐시와 공용 daemon은
    보존했다. 이 wrapper도 기존 두 provider 프로필의 공통 PATH로 재사용된다.

## 백업·설치·복구

private backup은 `~/.adk/backups/24x7-native-20260928`(0700)에 있다.
운영 PostgreSQL custom dump **173,682,701 bytes**, 기존 binary/config/plist/policies,
여섯 routine script와 원본 binding을 보관했다. `pg_restore --list`뿐 아니라 별도 DB에
실제 restore(33.58초)와 새 binary의 canonical migrator로 **migration 133 / 실패 0**을
확인했다. 운영 DB의 migration도 기존 release migrator로 적용했다. 생성한 복원 검증 DB는
정확한 이름을 확인하고 삭제했으며 `restore-cleanup.json`에 완료를 기록했다.

native macOS build와 ad-hoc code signing 후 `scripts/probe_macos_runtime.py`로 별도 root,
임의 DB/port에서 인증 거절, ledger write, 두 번의 실제 기동과 저장 생존을 검증했다.
canary DB는 도구가 생성한 정확한 이름만 정리한다. 실제 교체 전에 23 agent의 idle과
inflight routine 부재를 확인했다. 후속 bootout/bootstrap에서 일시적인 bootstrap exit 5가
발생한 기록과 복구를 보존했다. 최종 교체는 등록된 job을 유지한 채 binary를 원자적으로
교체하고 `launchctl kickstart -k gui/501/com.agentdesk.release`로 수행했다.
`/`는 200 HTML, `/api/health`는 200 JSON/healthy, `/api/auth/session`은 200 JSON이다.
`/health`의 SPA 200을 API 정상 근거로 쓰지 않는다.

첫 133 설치 후 binary를 더 수정한 과정의 binary hash와 config/profile hash는
`~/.adk/runtime-data/*install-20260928/activation-result.json` 및
`~/.adk/release/runtime/24x7-*-patch.json`에 남는다. 최종 결과는 아래 증거 표를 따른다.
기존 dashboard를 보존한 운영 patch이며 완전한 E5 공개 bundle 인수로 표시하지 않는다.

이전 v6 native SHA-256은
`09353beb1d25e0e9f18f5972864854388f96b7a8c137021254de0607036fb356`다.
build 473.886초, 별도 DB canary 2기동/3조건/2.421초 통과 후
`2026-09-27T19:21:18Z`에 운영 준비 상태를 확인했다. 당시 source capture와 Mac의
실제 build input 1,971개를 비교해 누락 0/불일치 0이었다. 실제 Qwen overlay의
부모 권한도 0700으로 확인했다.

**migration 130의 최초 binary로 단순 rollback하면 안 된다.** 후속 patch마다 보관한
`agentdesk.previous-migration133`은 같은 schema를 이해하는 복구 후보다. DB 복원은 writer를
먼저 멈춘 뒤 별도 계획으로 수행하며 이미 발생한 GitHub 효과를 되돌리지 않는다.

## 실행 증거와 현재 판정

03:00 KST 자연 cron의 skill-sync run `684b543b-19b4-45af-9de9-fb6d8521a560`은
실제 동기화와 Git mirror를 수행했다. `codex-skills`는 변경이 없었고,
`codex-mac-skills`는 `29256863e9bb`를 push했다. 당시 regression은 Python 경로 결함으로
19 실패/1 통과/5 skip이었으며 기존 wrapper가 보고서를 전송했다. native result는
03:08:14에 나왔지만 handoff 결함으로 플랫폼은 03:31:21에 timeout/failed를 기록했다.
동기화·백업의 부분 완료를 전체 성공으로 표시하지 않는다. 당일 failed marker를 보존하고,
Python 수정 뒤의 20 suite 통과도 별도의 재검사 결과로 기록한다.

v6 배포 후 Qwen skill-sync run `a74869d3-e5e3-4191-a08d-3ddc9044f3b3`은
04:21:29~04:28:13 KST에 **succeeded**, retry 0, `completion_evidence=session_transcripts`,
Discord log `ok`로 종료했다. 실제 PostgreSQL의 같은 turn에서 1,474자 응답과
`agentdesk.turn_outcome.v1 / completed`를 확인했다. fresh runtime log는 완료 후 정리됐으며
마지막 native frame을 별도 보존했다고 표시하지 않는다. 최종 업무 결과는 **skipped**,
이유는 `already completed today (failed)`다. 당일 실패 marker를 보존하고 동기화·push·
스킬 wrapper의 보고서 전송을 반복하지 않았다. 이 결과는 종료 인계와 중복 방지의 실제
검증이며, 최초 daily regression 실패를 성공으로 바꾼 기록이 아니다.

Qwen PR run `b0c3e34a-d9c0-4e76-a5f0-79e19c46ba30`도
04:29:23~04:38:13 KST에 **succeeded**, retry 0, 새 컨텍스트, 저장된 completed transcript,
Discord log `ok`로 끝났다. preflight와 live 조회에서 AgentDesk 열린 PR이 0개여서
**업무 결과는 no-op**이다. 04:40:07의 독립 GitHub 조회도 열린 PR 0개를 확인했다.
PR 2201은 그보다 앞선 03:34:31 KST에 닫혔으므로 이 run의 close/merge 성과로 계산하지 않는다.
모델이 UTC 스냅샷을 약 24시간 전이라고 서술한 부분은 실제 시각과 맞지 않아 근거로 채택하지
않았다. 보고 내용의 진위는 routine의 transport 성공과 별도로 대조한다.

OpenCode Monolith run `fdd368cd-49ca-407f-bed9-97c8a2ac1196`은
04:39:09~05:04:13 KST에 **succeeded**, retry 0, 새 컨텍스트, 같은 turn의
`agentdesk.turn_outcome.v1 / completed`, Discord log `ok`로 종료했다.
native session은 내부 Monolith repo에서 시작했고 `skill(name=git-help)`가 완료됐다.
운영자가 이미 설정한 보조 permission 규칙이 native session에 보존된 것도 확인했다.

초기 7개 PR 중 2873 한 개를 살펴보고 CI run `36307170503`을 한 번 재실행했다.
재실행 job `108695965642`도 실패하여 PR은 **OPEN**, head는 기존
`1c83732a7ec8393c92e6ee978cdf0c9d358b60b8`를 유지했다. 댓글·본문 수정·push·병합은 없었다.
독립 GitHub API 조회의 check-run annotation은 결제 실패 또는 지출 한도 때문에 job이
시작되지 않았음을 명시했다. 계정 설정을 변경하거나 검사 gate를 우회하지 않았다.

이 run은 **부분 검토/차단 보고**로 평가한다. 로컬 static CI는 `master`의
`71e22baf6b94f206838d1b697418a4f39492cb44`에서 실행됐으므로 PR head의 검사 통과가 아니다.
최종 큐 재조회·본문 갱신·댓글 작성과 전체 코드 검토 완료도 확인되지 않았다.
native tool 12개가 `completed`였지만 그 안에는 exit 1인 조회 두 개와 shell timeout으로
종료된 `sleep 150`도 있다. tool 상태, 명령 종료 코드, 업무 성공을 같은 것으로 계산하지 않는다.
원문과 실제 명령·PR 상태는 `mac/opencode-monolith-evidence.json`, 공식 CI 원인은
`mac/opencode-ci-annotation.json`에 보존했다.

- 초기 직접 모델 canary는 Qwen 29.19초, OpenCode 43.58초에 marker 응답했다.
- 초기 OpenCode routine은 `Unsupported ... Edit`로 실패했고 실패 상태로 기록됐다.
- 첫 Qwen routine의 session-ended false success를 실제로 재현하여 수정했다.
- 새 프로필 Qwen PR run `2fe581bc-b1e9-4172-8b6e-9457b70d916a`는 실제 조회·댓글·
  close-empty 조치 후 native heuristic에 중단됐다. **부분 효과가 있지만 run 전체는 실패**다.
  [AgentDesk PR 2200](https://github.com/kunkunGames/AgentDesk/pull/2200)은
  `2026-09-27T16:09:28Z`에 CLOSED임을 별도 GitHub 조회로 확인했다.
  PR 2201은 당시 OPEN/UNSTABLE이었으며 성공한 병합으로 기록하지 않았다.
- 중간 Qwen run `a07a0db3-cd05-4826-9a99-419ca2e3c2f1`은 실행 lease 갱신 상실로
  취소됐다. 당시 pool pressure와 native build 동시 실행을 관측했으나 CPU 부하가 원인이라고
  단정하지 않는다. lease 취소를 해제하지 않고 다음 검증은 native build 종료 후 수행했다.
- 중간 배포 검증의 OpenCode run `a6284afe-f515-4027-86e5-f69330685fd5`는 native `/config`
  capability 조회가 30초에 응답하지 않아 실패했다. session/tool 실행 이전 실패이며,
  native config/agent 조회 통과를 확인한 뒤 새 run으로 재실행했다.
- 기존 watchdog run `76b68737-a443-4eaa-a0af-6d81ef3dbff6`은 `succeeded`다.
  watchdog은 상태 점검 script이며 실제 provider를 종료하는 supervisor로 설명하지 않는다.
- upstream helper의 `--noop-check`는 exit 0 / `needs_sync`였다. 실제 merge나 동기화를
  수행한 결과가 아니며 기존 05시 루틴이 맡는다.
- Qwen `2e84c5f5-ad12-456b-9186-f22e27df132e`와 OpenCode
  `af5f7aff-94d1-4858-9870-2693da1bfbe3`는 각각 900초 실행 제한으로 실패했다.
  실제 metadata/diff/CI 조회를 진행했지만 업무 완료를 인정하지 않았다.
  다음 run부터 실행 한도를 1,800초로 맞추고 60 tools/30 turns/PR 1개 제한을 유지했다.
  routine 문구와 checkpoint의 batchSize도 1로 일치시켰다.
- 기존 Monolith thread의 실제 부모는 Qwen 채널이었다. thread별 명시적 org binding을
  추가하여 AgentDesk PR은 내부 agentdesk repo, Monolith PR은 내부 monolith repo에서
  실행한다. 임의 디렉터리 접근 권한을 넓히는 대신 작업 경로를 바로잡았다.
- v2 바이너리의 Qwen run `84db4d1b-667a-4769-8a02-9555af3dd9d6`도 1,800초
  안에 최종 보고를 내지 못해 실패했다. 여러 CI 작업 로그와 workflow를 조사하며 범위가
  늘어났다. 한도를 더 늘리지 않고, 모든 check 상태를 읽되 대표 실패 job log는 하나만
  조사하고 차단 원인이 확인되면 종료하도록 review 지침을 구체화했다. 10 tool calls는
  **prompt의 목표치**이며 native hard cap 60/30 turns 및 Routine 1,800초와 구분한다.
  검토하지 못한 범위는 partial review로 명시하며 승인·테스트 통과로 표현하지 않는다.
- v2 OpenCode run `4bfa8828-bfaf-416a-b34f-50300e1c1fc5`는 02:47:24부터
  03:18:17 KST까지 실행하고 timeout/failed로 종료했다. native session은 올바른 내부
  Monolith repo에서 시작했지만 보조 권한 누락으로 skill 파일·임시 worktree 접근이
  거절됐다. 이후 03:36:11 조회에서 내부 repo는 clean/단일 worktree였고 PR 2873은
  기존 head와 댓글을 유지했다. 이 run이 새 push·댓글·병합을 수행했다고 표시하지 않는다.
- 실제 모델 검증은 동시 요청에 의한 endpoint 지연 가능성을 분리하기 위해 순서대로
  실행한다. cron은 원래 시간표를 유지한다.
- 05:00:13 KST에는 기존 cron이 Qwen upstream sync run
  `b24c7939-5f2c-4c7e-b8de-0a57d24699a1`을 자동 시작했고 watchdog도 1.169초에 성공했다.
  upstream helper는 내부 SSD의 별도 worktree에서 실제 fetch/merge를 수행하다 세 파일의
  충돌을 발견했다. 이 run은 **05:31:17 timeout/failed, retry 0**으로 끝났다.
  최종 transcript/native terminal은 저장되지 않았으며 완료 증거로 계산하지 않는다.
  관측 시 해당 tmux session은 없었고 원격 main은 기존 `4a10f1882c6d`였다.
  부분 충돌 해결이 남은 worktree와 원래 실패 기록을 보존한 뒤 아래 보조 복구를 수행했다.

## 자연 upstream 실패의 보조 복구

원래 Qwen run의 성공으로 소급하지 않고 운영자가 개입한 복구로 따로 기록한다.
Qwen이 남긴 route 변경은 fork의 hooks 이동과 새 upstream `agents_turn_deliver`를
함께 보존했다. generated route inventory는 canonical generator로 다시 만들었고,
SQL inventory는 자동 병합된 양쪽 records를 유지하면서 충돌한 `measured_at_sha`만
해결했다. 한쪽 부모의 전체 inventory로 덮어쓰지 않았다.

unmerged 상태의 `--finalize`는 exit 7로 거절됐다. 경로를 정확히 stage한 뒤 SQL inventory
canonical check, 관련 Python **13개 테스트**, `cargo fmt --all --check`, diff check를 통과했다.
내부 Rust 1.94.1의 누락된 rustfmt component도 같은 toolchain에 설치했다.
진행 중 발견한 외장 캐시 대기와 dev profile 링크 실패는 각각 별도 실패 로그로 보존했다.
멈춘 복구 build의 소유 PID/parent/cwd를 확인한 뒤 그 자식만 종료했고, 기존 서비스와
무관한 daemon/process는 종료하지 않았다. 이는 인증된 일반 E4 종료·writer 격리의
증명이 아니라 해당 보조 복구에서 확인한 범위다.

복구 source, 원본, 거절 반례, 검사와 설치 기록은 v6 `mac/upstream-recovery`에 묶는다.
upstream 병합 검증 대상과 배포한 v6 source는 별개이며, 병합 결과를 운영 binary로
자동 배포하지 않는다. 이 보조 복구 시점의 서비스는 v6 SHA였고, 아래 별도 검증을
거쳐 08:18에 v9으로 교체했다.

06:12:40 KST에 기존 helper가 **exit 0**으로 종료하고 원격 main을
`95c4b746626af7365d35589d52d4ed3e59d25402`로 정상 push했다. 부모는 fork
`4a10f1882c6d86cd64b2416a644e76aa772e4225`와 원래 대상 upstream
`78909b9f299453a7bf5d6b728bb03fbf30f9bd89`다. `release-fast` 최초 build는
5분 40초, 같은 입력 재검사는 0.68초였다. 중간 보조 실행이 기존 전역 Git credential
helper를 사용해 외장 gh에 멈춘 기록도 보존했고, 실제 routine에 이미 적용한 내부
credential helper 환경과 일치시켜 재검증했다. 인증 비밀값을 읽거나 변경하지 않았다.
원격 SHA, upstream ancestry, main worktree clean, 해당 복구 worktree 정리와 운영 v6
binary 불변을 독립 확인했다. 다른 보존 worktree까지 제거했다고 표시하지 않는다.

06:12:48 재조회에서는 upstream이 `79b07a06930901dd364579f5b4851c4aa27470fd`로
한 commit 더 전진해 `needs_sync`였다. 이 새 변경은 앞선 복구의 완료 범위에 포함하지 않는다.

## v6 종료 결함과 v7~v9 보완

새 upstream을 처리하도록 실행한 `ad29e12c-af64-47a8-b906-a0565ab560d6`은
06:13:06 KST 시작 후 **06:19:27 failed / retry 0**으로 기록됐다. 실제 로그에는
06:19:06 execution lease 상실과 provider 취소가 있고, 완료 transcript는 없다.
그런데 자식 helper/build는 계속 실행해 원격 main을
`829c29da365d16c513e53cfddb3cfbc82a7af923`로 push했다. 이 commit은 새 upstream
`79b07a06930901dd364579f5b4851c4aa27470fd`를 포함한다. **업무 변경이 남았다는 이유로
실패 run을 성공으로 계산하지 않는다.** 종료 이후 외부 변경이 발생한 운영 결함이다.

Qwen CancelToken에는 tmux 이름만 연결되어 wrapper PID가 없었다. 기존 PID 종료도
단일 프로세스 그룹 중심이어서 도구가 별도 그룹으로 실행되면 남을 수 있었다.
같은 시각 runtime stall/DB 응답 지연은 관측했지만 당시 갱신 로그로는 실제 lease
소유권 상실과 일시적 DB timeout을 구분할 수 없다. 특정 원인으로 단정하지 않는다.

06:22:20 KST upstream routine을 **manual pause**했다. run ID가 일치하는 잔존
Node PID 세 개만 확인해 종료했고, unrelated process와 출처가 불명확한 보존
worktree는 제거하지 않았다. 증거는 v6 `mac/qwen-upstream-after-recovery*.json`과
`mac/upstream-post-failure-containment.json`에 보존한다.

v7 수정은 기존 CancelToken generation fence에 Qwen wrapper PID/identity를 연결하고,
root가 살아 있을 때 두 번 확인한 descendant ancestry와 PID identity로 별도 그룹을
정리한다. 이전 실행의 cleanup은 새 실행을 종료할 수 없다. DB transport 오류는
마지막으로 확인된 lease 유효시간 안에서만 재시도하며, 소유권 거절이나 만료는 계속
취소한다. snapshot 전에 이미 재부모화된 writer까지 OS 격리로 봉쇄한다는 보장은 아니다.

v7 집중 회귀 9개는 통과했지만 넓은 1,679개 회귀에서는 **1,678 통과 / 1 실패**였다.
자식 종료를 기다리던 shell이 자식 먼저 정리하는 동안 exit 0으로 진행하는 기존 반례가
발생했다. 이 동작은 helper의 다음 명령이 실행될 수 있다는 실제 계약 문제이므로 검사를
완화하지 않았다. v7은 운영에 배포하지 않았다. v8은 identity가 맞는 root와 확인된
descendant를 먼저 SIGSTOP으로 멈추고 종료한다. 회귀 fixture에도 child wait 이후의
파일 쓰기를 넣어, 종료 과정에서 부모가 다음 단계로 진행하지 않는지 확인한다.

06:43:46 KST 유지보수 build 기본 병렬 수를 1, nice를 10으로 조정했다. 전용 내부
sccache 서버에도 nice 10을 적용하고 기존 원본 및 SHA를 보존했다. 실행 lease를 끄거나
업무 timeout을 늘려 실패를 숨기지 않았다. 후속 source 검증은 v6 결과와 별도로 기록한다.

v8의 넓은 Linux 회귀는 **1,679 통과 / 0 실패**다. 첫 집중 검사에서는 build container의
PID 1이 `sleep`이라 이미 종료한 자식이 zombie로 남는 반례가 있었다. 동일 test binary를
`tini -s` 아래에서 실행해 17개 process 검사와 23개 집중 검사를 통과했다. 프로세스 그룹이
실제로 사라져야 한다는 기존 assertion을 완화하지 않았고 최초 실패 로그도 남겼다.

v9은 Qwen 새 실행뿐 아니라 복구된 tmux 세션에도 같은 generation fence와 wrapper PID를
연결한다. 호환 경로의 이름 재설정은 이미 발행한 소유권을 제거할 수 없다. 실제 tmux를
사용해 이전 turn의 cleanup이 새 turn의 pane을 종료하지 않는지 검증한다. Linux의 최종
집중 검사는 **24개 통과**, OpenCode 권한/종료 handoff 집중 검사는 **47개 통과**다.
전체 회귀와 Mac native 결과는 각 v9 원본 로그의 완료 기록을 따른다.

07:33:41 KST 나머지 모델 루틴 네 개에도 검증용 manual pause를 적용했다. upstream은
06:22의 실패 조사 pause를 유지했다. 30분 deterministic watchdog은 계속 enabled이며
07:30 주기도 완료했다. 일정, checkpoint, routine ID를 보존한 기록은 v9
`mac/maintenance-pause.json`에 있다. 배포 후 실제 실행과 재개 기록으로 이 유지보수
구간의 종료를 별도 확인한다.

## v9 운영 배포와 실제 취소 검증

최종 Mac binary SHA-256은
`fdcb7d18034676885764580b01c2d4f83d6f210fad49901a20ab428a1e06339e`다.
source fingerprint는 Linux 검증과 같은
`90ddad65ec282a4de93b375eb11339d1950146329810dd58d73b3ce16bcacdc2`이며,
Mac build input 1,977개와 별도 `.cargo/config.toml`의 일치를 확인했다.
native build는 754.738초, 격리 DB canary는 2기동/3조건/3.444초 통과다.
canary DB는 정확한 이름으로 삭제됐다. migration 133과 기존 dashboard를 유지하고
23 agent idle 및 routine inflight 부재를 확인한 뒤 기존 launchd job을 재기동했다.
복구용 binary는 동일 migration 133을 지원하는 v6 사본이다.

08:23:45~08:24:25 KST에 운영 바이너리와 실제 Qwen으로 통제된 취소를 검증했다.
Qwen이 Python 도구를 실행하고, 도구는 별도 session/group에서 SIGTERM을 무시하며
파일을 쓰는 자식을 만들었다. API가 반환한 turn ID를 canonical inflight의
provider/channel/user message/finalizer ID와 대조한 다음, 그 실행이 소유한 lease의
nonce와 instance를 확인해 만료시켰다. 서비스는 08:24:06에
`identity_or_expiry_rejected`로 provider 취소를 기록했다.

도구와 writer가 모두 사라졌고 쓰기 횟수는 cleanup 후 44회에서 추가 10초 관측 뒤에도
44회였다. 자식 종료를 기다리던 부모의 후속 쓰기도 없었고 unrelated process는 살아
있었다. 검증기의 수동 fixture cleanup은 0개다. 이는 실제 provider 취소 경로의 증거이며
임의의 이미 분리된 daemon/writer 전체 격리나 인증된 E4 정산의 완전한 증거는 아니다.
`mac/live-qwen-cancellation.json`과 별도 service proof에 결과를 보존했다.

08:18의 첫 probe는 lease를 만료시키기 전에 실행 식별 대조에서 실패했다. 최신 agent
요약을 현재 턴의 식별자로 쓰고 긴 session label의 잘림을 고려하지 못한 검증기 결함이다.
해당 fixture만 수동 정리한 실패 기록을 보존했고 이를 통과로 계산하지 않는다.
수정 probe는 짧은 label과 canonical inflight의 정확한 turn identity를 사용한다.
실제 중단·후속 쓰기 방지·무관 프로세스 생존 조건은 완화하지 않았다.

## v9 실제 timeout 반례와 후속 수정

08:25:28~08:56:26 KST upstream run `59542b0d-23ec-4be3-854d-a70f742b56d8`는
1,800초 제한으로 실패했다. Qwen은 도구 실행 이력이 있는 비정상 종료를 새 세션으로
다시 실행했고, Routine 종료는 wrapper에 먼저 Ctrl-C를 보내 별도 그룹의 CLI를
부모 없는 프로세스로 남겼다. 실패 후 09:00:15에 두 번째 merge worktree가 생성됐다.
처음의 wrapper 이름 중심 조회는 이 자식들을 놓쳤으므로 정리 성공 근거에서 제외한다.

09:03:07에 정확한 run/turn 표식으로 소유권을 대조한 PID 27378·27379·27383·32823을
중지·정리했다. 다른 장기 실행 Qwen PID 29261·29269는 보존했다. 09:16:41에는
`worktree-20260928T000015Z`를 `upstream-worktrees-quarantine`으로 `git worktree move`했다.
branch, HEAD, MERGE_HEAD와 모든 수정 상태가 같음을 확인했으며 파일을 삭제하지 않았다.

수정은 기존 수명·소유권 모듈에 둔다. session을 없애는 Qwen stop은 generation fence
안에서 소유 process tree를 먼저 정리한다. 직접 실행과 tmux wrapper는 공통 retry 판정을
사용하여 관측된 도구·응답·thinking 진행 또는 terminal 결과가 있으면 새 세션으로
재실행하지 않는다. 시작 전 handshake 실패만 기존 1회 재시도를 허용한다.

09:16:52에 설치한 upstream continuation helper는 별도 원장을 만들지 않는다.
등록된 Git worktree의 branch·HEAD·MERGE_HEAD·upstream ancestry로 원래 병합을 찾고
기존 편집을 보존한다. 후보가 여러 개거나 identity가 달라지면 중단하며, 보존 작업
폴더가 4개 이상이면 새 폴더를 만들지 않는다. 보존된 첫 병합은
`worktree-20260927T233238Z`, origin `829c29da365d16c513e53cfddb3cfbc82a7af923`,
captured upstream `f2a2a471ff31d54f7a9af4156a8ff89b56f91bd8`이다.
원격 head 재확인, 실제 Rust/dashboard 검사와 일반 push gate는 유지한다.
실제 Git fixture 10개는 3.366초에 통과했고 shell/Node 문법도 통과했다.

upstream Qwen과 AgentDesk merge OpenCode의 기존 thread가 상위 Codex 작업 폴더를
상속하던 설정도 각각 Qwen/monitoring agent 및 내부 AgentDesk 저장소에 명시적으로
연결했다. 다른 thread와 인증·권한 설정은 변경하지 않았다. 후속 실제 실행에서 cwd를
별도로 확인해야 한다.

v10의 첫 집중 검사는 27 통과/2 실패였다. 원본 Qwen 입력이 필요한 테스트에 wrapper가
정규화한 프레임을 넣은 fixture 결함이었다. v11에서는 원본 `message.role`과
`error.message`를 사용하도록 수정한다. 재시도 금지·오류 전달·출력 보존 조건은 완화하지
않는다. v10은 배포하지 않았고 빌드/실패 기록을 보존했다.

## 전체 호스트 인수에 남은 관측

`/api/health`의 core DB/dashboard는 healthy이지만 startup doctor 전체는 실패 1/경고 2다.
`edge-tts`가 현재 서비스 PATH에 없고 기존 실행 파일도 외장 SSD의 uv 환경을 가리켜
직접 version 확인이 timeout됐다. 이번 두 provider의 유지보수 루틴과 별개인 voice 구성은
성공으로 표시하지 않는다. 경고는 미등록 script 19개와 기존 Codex 중복 binding이다.

운영 DB에는 현재 source migration에 없는 과거 `external_delivery_plan_id`와
`agentdesk_require_scheduled_external_delivery_consumer_v1` trigger가 남아 있다.
기존 scheduled-message loop의 capability 거절 로그가 반복된다. 보호 trigger를 제거하거나
지원하지 않는 consumer capability를 선언하지 않았다. 이 레거시 예약 전송까지 포함한
호스트 전체의 24/7 정상 인수는 미완료이며, 여섯 Routine의 실행 결과와 구분한다.

## 운영 확인 명령

Mac의 release root에서 기존 인증을 쓰는 CLI로 조회한다. 응답에 작업 prompt와 checkpoint가
포함될 수 있으므로 외부 공유 시 run ID/status/시간/error/실제 조치만 선택한다.

```sh
cd ~/.adk/release
./bin/agentdesk --json api GET /api/health
./bin/agentdesk --json api GET '/api/routines?status=enabled'
./bin/agentdesk --json api GET '/api/routines/8519cc7e-d0a1-46fb-877d-875e36c4fd08/runs?limit=3'
./bin/agentdesk --json api GET '/api/routines/95b23dc5-b045-4adc-9c58-e20b59304fbb/runs?limit=3'
```

실패한 routine을 재개할 때는 먼저 마지막 run의 GitHub 효과·실행 프로세스·checkpoint를
확인한다. 기존 API의 `POST /api/routines/{id}/resume {}`와 `run-now {}`를 사용하며
inflight 중복을 강제로 지우지 않는다. 재개는 실패를 성공으로 바꾸지 않는다.
LaunchAgent의 KeepAlive/RunAtLoad는 확인했지만 사용자 로그인 이전 L3 및 실제 재부팅
보장은 아니다. `pmset -g custom`에서 system sleep 0과 autorestart 1을 확인했지만
실제 전원 장애·OS 재부팅 시험으로 계산하지 않는다. 24시간 관측은 새 운영 profile의 실제 시간과 run 결과로 별도 인수한다.

v9의 읽기 전용 관측기는 `~/.adk/operations/routine-observer-v9.py`와
`com.agentdesk.routine-observer.v9` LaunchAgent로 구성한다. 300초 간격, nice 10이며
기존 routine API를 조회할 뿐 실행·재개·취소·외부 메시지를 보내지 않는다. source와
설치 기록은 v9 `mac` 증거에 보존한다. Mac에서 observer 계약 테스트 4개가 통과했다.
설치 직후 첫 표본이 정상이고 여섯 routine이 enabled인지 확인한 후 활성화 기록을 쓴다.

관측 파일은 `~/.adk/runtime-data/24x7-routine-observation-v9/window.json`이다.
최근 300개 표본과 routine당 최근 200개 실행만 보관한다. 660초를 넘는 관측 공백,
비정상 표본, 배포 binary 변경과 실제 run 상태를 따로 기록한다. 시작 후 하루가 지나면
`first-24h-window.json`을 한 번만 생성한다. `elapsed_24_hours`는 경과 시간이고,
업무 성공·연속 정상 상태·전체 v5 인수를 뜻하지 않는다. `full_v5_acceptance`는 false를
유지한다. 첫 24시간 자료를 평가할 때 실패/공백과 각 작업 결과도 함께 확인해야 한다.

유지보수 pause를 해제하기 전에는 원본 `paused_updated_at`과 현재 `updated_at`이 같은지
확인한다. 사용자가 나중에 별도로 멈춘 상태는 덮어쓰지 않는다. `{}` resume는 기존의
`next_due_at`을 보존하므로, 지나간 08시 daily PR 점검을 명시적으로 한 번 실행할 때는
다음 날의 원래 예정 시각을 지정해 scheduler와 run-now의 중복 경쟁을 피한다. cron과
checkpoint는 유지하며 보충 실행의 실제 run ID와 다음 예정 시각을 함께 보존한다.
