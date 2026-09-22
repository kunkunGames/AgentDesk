# Discord ↔ Windows worker 실제 송수신·DB 재검증

검증일: 2026-09-22, 완료 시각 16:11:31 KST.
사용자가 실제 Discord 테스트를 요청한 뒤 새 스레드와 임시 agent로 검증했다.
이전 결과를 재인용한 것이 아니라 현재 설치본에 새 메시지를 보낸 결과다.

**결과: Windows Codex 실행, Discord 수신·응답, PostgreSQL 작업 기록,
세션 연속성, 첨부 전달, 중앙 출력 조회와 실행 취소가 모두 통과했다.**

## 설치본과 검증 경로

| 항목 | 확인한 값 |
| --- | --- |
| 두 장비의 실행 코드 | `3befd20797ce29d893a7ce13f04d6fb714d94924` |
| Mac mini | `single-node`, leader / full, `192.168.1.147:8791` |
| Windows | `windows-worker-1`, worker / worker, `192.168.1.222:8791` |
| PostgreSQL schema | 127 |
| 검증 DB session | 111134, owner `windows-worker-1` |
| Discord 검증 스레드 | [discord-windows-roundtrip-20260922](https://discord.com/channels/1469509996621594686/1551852492508102747) |

실제 경로는 Discord 입력 → Mac leader → PostgreSQL intake 기록 → Windows provider
실행 → Discord 응답이다. 운영자용 announce 봇으로 실제 Discord 메시지를 게시하고,
응답 봇은 해당 채널의 Codex 봇인지 확인했다. 입력·응답의 실제 Discord message ID를
DB의 `user_msg_id`와 대조했다. 검증 중 서버 재시작이나 운영 agent의 설정 변경은 없었다.

## 수행 결과

| 검사 | 결과와 근거 |
| --- | --- |
| 사전 상태 | 양쪽 `healthy`, `fully_recovered=true`, `db=true`. Windows Codex 실행 준비와 leader의 인증된 worker 도달 확인 통과 |
| 새 Windows 세션 | 우선 머신을 Windows로 지정한 임시 agent의 새 요청이 session 111134에 배정됨 |
| Windows 실행 | PowerShell로 대기 후 임의 검증 문자열·OS·호스트를 출력하도록 요청. Discord에 해당 문자열과 `Win32NT`, `DESKTOP-F0AF6E7`이 돌아옴 |
| 실행 중 DB·출력 조회 | DB에서 Windows session의 `turn_active`와 유효한 실행 lease 관찰. Mac 중앙 출력 API에서 `backend=process`, `alive=true`, `available=true` 확인 |
| 후속 대화 | 임시 agent의 우선 머신을 Mac으로 바꾼 뒤에도 같은 DB session과 provider session이 Windows에서 유지됨. 직전 검증 문자열을 정확히 다시 답함 |
| 첨부파일 | Discord 첨부 파일 안에만 넣은 별도 임의 문자열을 Windows가 읽어 정확히 답함. 해당 intake의 첨부 수는 1 |
| 응답 대상·중복 | 일반 실행·후속 대화·첨부의 각 답변이 올바른 Codex 봇으로 한 번씩 게시됨. 실제 guild/channel/input을 가리키는 원문 링크 확인 |
| 중앙 실행 취소 | 별도 45초 PowerShell 대기를 실행하고 실제 하위 PID를 관찰한 뒤 Mac API로 취소. 관찰 시작부터 2.41초 안에 해당 PID가 종료됨 |
| 취소 후 상태 | 실행 lease 해제, 취소된 작업의 완료 문자열 미게시. 처음 관찰한 PID를 직접 조회해 고아 프로세스도 검사 |
| 지연 중복 확인 | 모든 turn과 취소가 끝난 뒤 다시 메시지 전체를 검사. 다른 provider 봇의 답변과 `headless_turn` 중복 전송 없음 |

## PostgreSQL 확인

실제 운영 API를 통해 생성된 이 검증 채널의 행을 읽기 전용 SQL로 대조했다.
운영 DB를 단위 테스트용 DB로 사용하거나 운영 행을 임의 SQL로 수정하지 않았다.

| intake_outbox ID | 요청 | target | 최종 전달 상태 | attempt / retry |
| --- | --- | --- | --- | --- |
| 16 | Windows 명령 실행 | `windows-worker-1` | `done` | 1 / 0 |
| 17 | 후속 대화 | `windows-worker-1` | `done` | 1 / 0 |
| 18 | Discord 첨부 | `windows-worker-1` | `done` | 1 / 0 |
| 19 | 실행 취소 검증용 대기 | `windows-worker-1` | `done` | 1 / 0 |

네 행 모두 leader `single-node`가 전달했고 Windows가 claim했다. claimed/accepted/
spawned/completed 시각이 모두 기록됐으며 `last_error`는 없었다. 요청 ID별로 한 행만
존재하고 재시도·추가 attempt는 없었다.

`intake_outbox.done`은 intake 전달 처리 상태다. 이 값만으로 모델 답변 완료를 판단하지
않았다. 실행 중에는 intake가 `done`이어도 session이 `turn_active`이고 lease가 유효한
상태를 실제 관찰했다. 별도로 Discord 답변, session의 idle 복귀, 취소된 자식의 종료와
lease 해제를 확인해 실행 수명주기를 판정했다.

최종 실행 검사에서 session은 Windows owner의 `idle`, 유효한 channel lease는 0건,
해당 채널의 `message_outbox.source=headless_turn`도 0건이었다.

## 정리와 보존

검증용 session 111134는 실행 프로세스가 종료된 것을 확인한 후 정상 API로 제거했다.
임시 agent도 삭제했고 Discord 스레드는 검증 기록을 남긴 채 archive했다.

기존 23개 agent의 전체 행 checksum은 검증 전후
`3a34d55f1b525aa0dc7673ad7885e81e`로 동일했다. 이 비교는 migration 127의 새 열까지
포함한다. schema 126 열만 비교했던 이전 보고서의 checksum과 계산 범위가 다르다.

정리 후 양쪽 서버 모두 `healthy`, `fully_recovered=true`, `db=true`였고
active/finalizing/queue는 모두 0이었다. 운영자의 기존 에이전트 기본 머신은 변경하지 않았다.

## 검증 범위와 증거

이번 결과는 **현재 Mac leader·Windows worker·Codex 실제 계정**의 단일 검증 채널에서
확인했다. 일반 사용자 Discord 클라이언트의 UI 조작, 다른 provider 계정, 동시 부하와
장시간 안정성은 별도 범위다. 이전에 기록한 Mac 원문 링크·idle force-kill 제한을
이번 Windows 성공으로 해소됐다고 판단하지 않는다.

로컬 증거:

- `target/heterogeneous-worker-validation/discord-windows-roundtrip-20260922.json`
- `target/heterogeneous-worker-validation/verify_discord_windows_roundtrip.py`

기존 검증 helper를 재사용했으며 receipt에는 인증값과 원문 provider session ID를
기록하지 않았다. 전체 배포·운영 조건은 [Mac mini / Windows 배포 보고서](heterogeneous-worker-rollout-2026-09-22.md)를 따른다.
