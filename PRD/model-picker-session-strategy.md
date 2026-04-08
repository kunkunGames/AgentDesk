# PRD: Model Picker Session Strategy

문서 버전: `v1`
기준 날짜: `2026-04-09`
상태: `proposed`
소유자: `AgentDesk runtime`

## 1. 배경

AgentDesk Discord runtime은 model picker로 채널별 모델 override를 바꿀 수 있다. 하지만 운영 관점에서는 두 가지 전략이 섞여 있었다.

- `같은 live session을 유지한 채 model만 바꾸는 hot swap`
- `model picker submit 이후 새 프로세스를 시작하고, provider가 지원하면 resume를 붙여 문맥을 이어가는 방식`

이 차이가 명확히 문서화되지 않으면 provider별 동작 차이, 재시작 복구, session_id 저장 규칙, UX 기대치가 계속 흔들린다.

## 2. 문제 정의

- 운영자는 model picker를 눌렀을 때 `바로 같은 세션이 바뀌는지`, `다음 턴부터 새 세션인지`를 예측하기 어렵다.
- provider마다 CLI 제약이 달라서 동일 UX를 가장하기 어렵다.
- `hot swap` 을 허용하면 live process의 tool state, prompt state, memory state와 model state가 어긋날 수 있다.
- `new process + resume` 로 가면 더 일관되지만, Gemini처럼 `resume` 와 `model override` 동시 처리 제약이 있는 provider가 있다.

## 3. 목표

- model picker submit 이후의 세션 계약을 provider별로 명확히 고정한다.
- runtime이 어떤 경우에 live session을 종료하고, 어떤 경우에 resume/fresh를 택하는지 정한다.
- 운영자와 사용자가 예측 가능한 UX를 갖게 한다.

## 4. 비목표

- provider CLI가 원래 지원하지 않는 model mutation API를 AgentDesk가 임의 구현하는 것
- provider별 숨은 내부 세션 포맷을 역공학해서 강제 호환하는 것
- model picker 외 모든 runtime config 변경을 같은 정책으로 묶는 것

## 5. 사용자 시나리오

### 운영자

- 운영자는 `/model` 또는 picker UI에서 채널 모델을 바꾼다.
- submit 후 다음 메시지부터 어떤 세션이 쓰이는지 예측 가능해야 한다.

### 일반 사용자

- 사용자는 “모델이 바뀌었는데 왜 이전 세션과 다르게/같게 동작하냐”를 헷갈리지 않아야 한다.

### 재시작 복구

- dcserver 재시작 후에도 override는 복구돼야 한다.
- 다만 이전 live provider session을 무조건 재사용한다고 기대하면 안 된다.

## 6. 현재 구현 기준

기준 코드 경로:

- `src/services/discord/model_picker_interaction.rs`
- `src/services/discord/commands/config.rs`
- `src/services/discord/commands/control.rs`
- `src/services/discord/router/message_handler.rs`
- `src/services/claude.rs`
- `src/services/codex.rs`
- `src/services/gemini.rs`
- `src/services/qwen.rs`

현재 구현은 model picker submit 시 channel override를 저장하고, 다음 턴에 `model_session_reset_pending` 을 보고 provider session을 새로 시작하는 구조다. 즉 `hot swap` 이 아니라 `deferred reset` 에 가깝다.

## 7. 옵션

### 옵션 A: 같은 세션 유지인 hot swap

정의:

- 기존 live provider session을 죽이지 않는다.
- model picker submit 후 같은 tmux/process/session_id 위에서 모델만 바꾼다.

장점:

- 세션 재생성 비용이 없다.
- live tool state와 일시적인 in-memory context를 더 오래 유지할 수 있다.
- Claude/Codex/Qwen의 managed session에서는 체감 latency가 낮다.

단점:

- provider CLI가 안전한 runtime model mutation을 공식 지원하지 않는다.
- 기존 session이 가진 system prompt, tool cache, compact state, provider 내부 memory와 새 model의 의미가 엇갈릴 수 있다.
- 장애 분석 시 “이 세션이 어느 모델로 시작됐는지” 추적이 어렵다.
- Gemini에는 현재 적용 경로가 없다.

provider 적합성:

- Claude: live stdin/tmux reuse는 가능하지만 safe model hot swap 계약은 없음
- Codex: live session reuse는 가능하지만 safe model hot swap 계약은 없음
- Gemini: 불가
- Qwen: live session reuse는 가능하지만 safe model hot swap 계약은 없음

판단:

- v1 채택 불가
- 연구/실험 항목으로만 유지

### 옵션 B: model picker submit 시 새 프로세스 + resume 적용

정의:

- submit 시 override를 저장한다.
- 다음 턴 시작 전에 기존 provider session을 reset 대상으로 표시한다.
- 다음 턴은 새 프로세스로 시작한다.
- provider가 안전하게 지원하면 `resume + model` 또는 provider-native continuation을 사용한다.
- provider가 해당 조합을 안전하게 지원하지 않으면 fresh session으로 시작한다.

장점:

- 세션 경계와 모델 경계가 명확하다.
- 로그, DB session_id, restart recovery, stale session 정리가 쉬워진다.
- provider별 제약을 분기 처리해도 UX 문구를 일관되게 만들 수 있다.

단점:

- submit 직후 바로 바뀌는 것이 아니라 다음 턴부터 적용된다.
- provider별 continuation 품질 차이가 남는다.
- live session이 길게 유지되던 채널은 첫 턴 latency가 약간 증가할 수 있다.

provider 적합성:

- Claude: 새 프로세스 + `--resume + --model` 가능
- Codex: 새 프로세스 + `-m + resume` 가능
- Gemini: 현재 구현은 `resume` 경로에서 `-m` 을 같이 넘기지 않음. 따라서 `resume 유지` 또는 `fresh + target model` 중 하나를 택해야 함
- Qwen: `--model + --resume` 가능, `--model + --continue` 도 가능

판단:

- v1 채택안

## 8. 결정

채택안은 `옵션 B` 다.

핵심 결정:

- model picker submit은 `즉시 hot swap` 이 아니라 `다음 턴부터 새 세션 적용` 으로 정의한다.
- override 값은 저장하고, 다음 턴 시작 전에 기존 provider session을 리셋한다.
- provider별 resume/continue 지원 범위는 다를 수 있지만, `모델이 바뀐 세션은 새로 시작한다` 는 상위 계약은 공통으로 유지한다.

## 9. Provider 계약

### Claude

- live managed session은 유지 가능하지만 model 변경 시에는 종료 후 새 세션 시작
- 새 프로세스 경로에서 `--model + --resume` 허용

### Codex

- live managed session은 유지 가능하지만 model 변경 시에는 종료 후 새 세션 시작
- 새 프로세스 경로에서 `-m + resume` 허용

### Gemini

- live managed session 없음
- model 변경 후에는 새 프로세스 필요
- current implementation 기준으로 `resume + model override` 를 동시에 쓰지 않음
- 따라서 v1에서는 `resume selector 유지` 와 `target model fresh start` 중 provider-safe한 쪽을 택해야 함

### Qwen

- live managed session 유지 가능
- model 변경 시 새 세션 시작
- 새 프로세스 경로에서 `--model + --resume` 또는 `--model + --continue` 가능

## 10. UX 계약

- picker에서 `저장`을 누르면 override 저장은 즉시 완료된다.
- 사용자에게는 “다음 턴부터 적용된다”는 문구를 준다.
- 현재 진행 중인 응답은 중간에 model이 바뀌지 않는다.
- 다음 사용자 메시지에서 runtime은 reset pending을 확인하고, 새 세션으로 전환한다.

추천 사용자 문구:

- `모델 변경이 저장되었습니다. 다음 메시지부터 새 세션으로 적용됩니다.`

## 11. 기술 설계

저장 상태:

- channel별 model override는 bot settings에 저장
- runtime memory에는 `model_session_reset_pending` 만 둠
- provider session_id는 다음 턴 직전에 clear

세션 처리:

1. model picker submit
2. override 저장
3. channel을 reset pending으로 마킹
4. 다음 턴 진입 시 기존 session_id 제거
5. managed session provider는 기존 tmux/process 종료
6. provider별 규칙에 따라 `fresh / resume / continue` 로 새 실행

중요 원칙:

- `모델 변경` 과 `세션 변경` 을 같은 이벤트로 본다
- live session hot swap은 하지 않는다
- provider-safe하지 않은 `resume + model` 조합은 강제하지 않는다

## 12. 리스크와 완화

리스크:

- Gemini만 UX가 다르게 느껴질 수 있음
- submit 직후 곧바로 model이 바뀐다고 기대한 사용자와 어긋날 수 있음
- stale provider session이 남으면 여전히 잘못된 model로 복원될 수 있음

완화:

- UI/응답 문구에서 `다음 턴부터 새 세션` 을 명시
- restart recovery와 model reset pending을 함께 유지
- provider별 통합 테스트 추가

## 13. 구현 단계

1. PRD와 runtime UX 문구를 같은 계약으로 맞춘다.
2. provider matrix를 테스트에 고정한다.
3. Gemini fallback 규칙을 문서와 코드에 같은 문장으로 반영한다.
4. 운영 문서에 `hot swap 비지원` 을 명시한다.

## 14. 검증 기준

수동 검증:

- Claude 채널에서 모델 변경 후 다음 메시지에 새 세션이 시작되는지 확인
- Codex 채널에서 동일 검증
- Gemini 채널에서 model override와 resume fallback 문구 확인
- Qwen 채널에서 `continue/resume` 동작 확인

자동 검증:

- model picker submit 시 override 저장 테스트
- next turn 진입 시 reset pending 처리 테스트
- provider별 build args 테스트
- restart 후 override 복구 테스트

## 15. 권장안 요약

- v1 운영 권장안은 `새 프로세스 + provider-safe continuation`
- `같은 세션 hot swap` 은 문서상 비채택
- 특히 Gemini는 `resume + model` 을 하나의 보장 계약으로 묶지 않는다
