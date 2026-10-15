# W2 보호 검사 실행과 조건부 완료

이 문서는 기존 `kanban_cards`, `task_dispatches`, `test_phase_runs`, `resource_locks`와
FSM을 연결한 첫 managed check adapter의 계약이다. 별도 검사 큐나 Campaign 원장은
만들지 않는다. 검증 결과와 실행 파일의 정확한 범위는 [VALIDATION.md](VALIDATION.md)를 따른다.

## 적용 범위

첫 adapter는 Linux의 보호된 단일 프로세스로 검사할 수 있는 작은 파일 과제용이다.
정식 registry에 등록한 실행 파일과 wrapper만 실행한다. 후보는 `.git`을 제외한 전체
파일 내용으로 고정하며 untracked 파일도 포함한다. 최대 4,096개 항목, 32 MiB이고,
symlink·hardlink·특수 파일·역슬래시가 들어간 경로를 거절한다. 빈 디렉터리나 Git
메타데이터의 존재를 acceptance로 삼는 과제, LFS를 다운로드해야 하는 과제, 외부 의존성
해결이 필요한 빌드는 이 adapter의 인수 범위가 아니다.

Linux root launcher, 사설 UID/GID의 mode 0700 workspace, Landlock ABI 3 이상,
보호된 실행 파일·wrapper·artifact 디렉터리를 요구한다. 검사 프로세스는 해당 UID로
내려가고 후보는 읽기만 가능하다. 프로세스 생성·thread 생성·네트워크·ptrace 등을
seccomp로 거절한다. 따라서 일반 `cargo test`, Node worker나 병렬 컴파일러를 실행하는
범용 build adapter가 아니다. 다른 실행 환경은 별도 보호 경계와 실제 인수가 필요하다.

## 운영자 registry와 등록

`AGENTDESK_CHECK_REGISTRY`는 root 소유의 비공개 JSON 파일을 가리킨다. 각 이름은
완전한 template 하나를 선택한다. HTTP 요청이나 모델 출력으로 command를 받지 않는다.
다음은 형식 예시이며 digest는 해당 호스트의 실제 파일 SHA-256으로 채워야 한다.

```json
{
  "answer": {
    "workspace": "/srv/agentdesk/workspaces/answer",
    "artifact_root": "/srv/agentdesk/verified-artifacts",
    "allowed_changes": ["answer.txt"],
    "acceptance": [{"id": "correct-answer", "checks": ["answer-check"]}],
    "checks": {
      "answer-check": {
        "executable": "/usr/bin/python3.13",
        "wrapper": "/srv/agentdesk/protected/check-answer.py",
        "executable_sha256": "<actual executable SHA-256>",
        "wrapper_sha256": "<actual wrapper SHA-256>",
        "timeout_secs": 5,
        "kind": {"type": "assertion"}
      }
    },
    "review": "none",
    "max_candidates": 3,
    "max_check_attempts": 2
  }
}
```

workspace와 artifact root는 실제 canonical 절대 경로여야 하며 artifact root는
workspace 밖에 둔다. artifact 디렉터리와 상위 경로는 root 소유로 보호한다. 실행 파일과
wrapper도 모델 UID가 바꿀 수 없어야 한다. 동적 라이브러리나 언어 런타임 전체를 이
두 digest로 인증했다는 뜻은 아니므로 인수한 호스트 이미지와 도구 환경을 유지해야 한다.

`POST /api/managed-checks/{card_id}/enroll`에 `{"template":"answer"}`를 보내면 현재
파일 baseline과 template을 불변 계약으로 저장한다. **첫 dispatch 전의 idle
`backlog`/`ready` 카드만 등록 가능**하다. 등록과 dispatch 삽입은 같은 카드 잠금으로
직렬화한다. baseline 파일 읽기는 transaction 밖에서 수행한다.

등록·조회·승인은 설정된 서버 admin bearer를 요구한다. loopback 예외나 인증 토큰 없는
구성으로 이 API를 사용할 수 없다. bearer와 registry 내용은 모델 실행 환경에 전달하지 않는다.
계약을 지우거나 바꾸는 API는 제공하지 않으며 새 계약에는 새 카드를 사용한다.

## 실행과 증거

서버의 기존 30초 reconcile tick이 `review` 상태의 managed 카드를 찾는다. 최신
implementation/rework가 완료되고 새 실행이 없을 때만 후보를 만든다. workspace에
살아 있거나 종료 여부가 미확정인 managed lease가 남으면
`verification_await_execution_quiescence`로 대기한다. 검사 성공은 writer lease를
반환하거나 기존 budget을 환불하는 근거가 아니다.

짧은 `managed-capture:` resource lock과 writer admission이 같은 PostgreSQL advisory
lock을 사용한다. 파일 복사·검사는 DB transaction이나 QuickJS tick 밖에서 수행한다.
후보 내용을 hash로 고정하고, 각 검사에는 task/dispatch/result/contract/candidate/check
식별자로 만든 `managed-check:` idempotency key를 사용한다.

wrapper는 executable의 첫 인수이고 후보 디렉터리는 다음 인수다. 예시 assertion
wrapper는 후보 `answer.txt`가 `42`인지 직접 검사한 뒤 다음 형태의 JSON 하나를 stdout에
출력한다. 임의 progress 로그를 같은 stdout에 섞으면 report 파싱에 실패한다.

```json
{"passed": true}
```

`kind.type="test_suite"`이면 `suites`, `min_tests`, `max_skipped`를 template에 명시한다.
report의 각 suite에는 `id`, `discovered`, `executed`, `skipped`, `failed`가 필요하다.
필수 suite 누락·중복·0회 실행·발견 수 불일치·허용 밖 skip은 통과할 수 없다.
`assertion`에는 테스트 개수를 강제하지 않는다.

수집기는 실제 프로세스 종료와 wait/reap, exit status, report, 후보 digest를 확인한다.
통과 JSON을 먼저 출력했더라도 프로세스가 계속 살아 있으면 pass가 아니다. deadline이나
카드 취소·새 결과가 감지되면 프로세스를 종료하고 수거한다. receipt에는 collector,
contract/dispatch/check 식별자, 실제 결과, 종료 여부와 stdout/stderr hash를 보관한다.
stdout/stderr 원문을 모델 prompt나 카드에 전달하지 않는다.

공개 phase-run upsert/start/complete API는 `managed-check:` namespace를 거절한다.
정상 receipt의 재사용과 충돌 처리는 기존 phase-run lifecycle을 따른다. 검사 소실이나
report 오류는 제한된 `max_check_attempts` 안에서만 재시도하며 통과로 바꾸지 않는다.
입력 범위 위반처럼 후보 준비가 실패하면 `unverifiable`을 저장하고 사람의 조치를 기다린다.
파일만 몰래 고쳐서 같은 실패 시도를 성공으로 바꿀 수 없다. 수정 작업은 새 implementation
시도와 기존 `max_candidates` 한도를 따른다.

## 리뷰와 완료

`agentdesk_check_disposition`이 Rust collector, JS review hook, Rust review 보정과
timeout/reconcile의 공통 판단을 제공한다.

| 판단 | 동작 |
|---|---|
| `await_checks` | 현재 후보의 필수 검사를 기다림 |
| `run_local_review` | 기존 로컬 리뷰 hook/dispatch 복구 경로 실행 |
| `await_human` | 실패·검증 불가 또는 계약상 사람 승인을 기다림 |
| `ready_to_commit` | 기존 FSM으로 조건부 완료 시도 |
| `completed` | 완료된 managed 카드의 조회 결과 |
| `unmanaged` | 기존 카드의 동작 유지 |

`review=none`은 필수 검사를 모두 통과하면 완료할 수 있다. `review=local`은 같은
dispatch/후보/계약에 묶인 현재 리뷰의 pass/approved가 추가로 필요하다. counter-model
채널이 없으면 자동 승인하지 않는다. `review=human`은 필수 검사 통과 뒤 운영자 승인이 필요하다.

`GET /api/managed-checks/{card_id}`로 계약·후보·receipt 연결과 판단을 조회한다.
사람 승인은 `POST /api/managed-checks/{card_id}/approve`에 아래 필드를 보낸다.

```json
{
  "candidate_digest": "<current candidate digest>",
  "dispatch_id": "<current implementation dispatch>",
  "contract_digest": "<enrolled contract digest>",
  "reason": "검사 결과와 대상 파일을 확인한 사유"
}
```

승인은 현재 후보에만 유효하며 `server_admin` 권한과 사유가 state에 남는다. 오래된
후보나 필수 검사 미통과 상태를 승인할 수 없다. 승인 실패를 검증 예외로 취급하지 않는다.

검사 완료는 force 없이 기존 FSM gate를 거친다. 상태 UPDATE는 기대했던 이전 상태와
일치해야 하며 0행 변경을 성공으로 기록하지 않는다. DB trigger는 완료 직전에 현재 결과와
receipt를 다시 확인하고, 같은 transaction의 상태·감사·queue sync를 보호한다. 먼저 확정된
새 결과·새 시도·취소·agent/repo/stage 변경이나 동시 승인을 과거 수집기가 덮을 수 없다.
취소·실패 종료에는 검사 통과를 요구하지 않는다.

## 검증 재현과 다음 경계

정확한 명령과 artifact는 [RUNBOOK.md](RUNBOOK.md), 결과는 [VALIDATION.md](VALIDATION.md)에
기록한다. `scripts/probe_managed_checks.py`는 별도 Hub와 새 fixture DB를 만들어 실제
HTTP 등록·비LLM 검사·FSM 완료·사람 승인·재시작을 검사한다. implementation 완료만
fixture로 주입하며 모델 수행 품질이나 실제 프로젝트의 테스트 충분성을 주장하지 않는다.
HTTP fixture의 Python DB 접근에는 테스트 이미지의 `libpq5`가 필요하다.

기존 Phase Gate registry의 PR 확인·통합·배포 의미는 유지한다. 개별 후보의 receipt를
통합 또는 배포 통과 증거로 재해석하지 않는다. Campaign의 A→B 입력 전달, finite batch
membership, 통합 ref 승격, 다중 호스트 검사, 실제 provider별 수명 인수와 운영 24시간은
별도 단계다. 이 문서만으로 전체 v5 배포 완료를 의미하지 않는다.
