# 공통 바이너리 릴리스와 노드 배포

같은 OS/architecture의 leader와 worker는 같은 실행 파일과 공통 자산을 사용한다.
역할은 노드 설정과 런타임 시작 계획으로 정한다. worker 수나 역할을 빌드 matrix에
추가하지 않는다. 새 노드는 동일 artifact를 설치하고 고유 ID·주소·로컬 환경을 설정한다.

## 구성과 소유권

- [build-release.sh](../../scripts/build-release.sh): 기존 build token을 통한 Rust 빌드,
  dashboard 검증, 패키징 호출. Cargo.lock을 고정하고 agentdesk binary만 빌드한다.
- [package_release.py](../../scripts/package_release.py): OS별 압축 형식, 공통 디렉터리 구조,
  실행 파일 OS/CPU 확인, manifest와 SHA-256 생성. Python 표준 라이브러리를 사용한다.
- [verify_release_artifacts.py](../../scripts/verify_release_artifacts.py): 배포 commit/version,
  matrix 완전성, archive 및 내부 파일 hash, 런타임 identity 일치 검증.
- [release.yml](../../.github/workflows/release.yml): 위 모듈을 호출하는 GitHub Actions.
  대시보드를 한 번 검증·빌드하고 세 native build에서 재사용한다.

패키징 대상은 명시적으로 선택한 **Git 추적 공통 자산**, 빌드한 실행 파일 및 dashboard다.
운영자 YAML, 인증 파일, 로컬 프롬프트/정책 추가 파일, 작업 중인 worktree를 수집하지 않는다.
운영 배포는 공통 자산과 node-local 자산의 소유권을 유지해야 한다.
[Source of Truth](../source-of-truth.md)의 operator-private routines 규칙도 그대로 적용된다.

## 빌드와 산출물

필수 도구는 저장소가 고정한 Rust toolchain, Python 3.11 이상, 해당 OS의 native C/C++
도구체인이다. 대시보드를 빌드할 때는 `.nvmrc`의 Node 최소 버전과 npm이 필요하다.
Windows에서는 Git Bash에서 공통 빌드 스크립트를 실행할 수 있으며, 압축을 위한 외부
`zip` 또는 `rsync` 설치는 필요하지 않다.

```bash
# Native build + dashboard install/audit/build/test + package
bash scripts/build-release.sh

# CI 등에서 같은 commit의 dashboard 검증이 이미 끝난 경우
bash scripts/build-release.sh --prebuilt-dashboard

# UI를 제외하는 로컬 패키지. 빌드·검증·복사 모두 dashboard를 건너뛴다.
# 일반 공개 Release는 dashboard를 포함해야 publish 검증을 통과한다.
bash scripts/build-release.sh --skip-dashboard

# 명시적 Rust target. 이 옵션이 cross compiler/SDK를 설치하지는 않는다.
bash scripts/build-release.sh --target aarch64-apple-darwin --prebuilt-dashboard

# 기존 운영 빌드와 같은 빠른 최적화 프로파일
bash scripts/build-release.sh --profile release-fast --prebuilt-dashboard
```

`AGENTDESK_PYTHON`으로 Python 실행 파일을 지정할 수 있다. `CARGO_TARGET_DIR`을 설정한
빌드도 해당 위치의 산출물을 사용한다. `--target` 생략 시 rustc host triple을 기준으로 한다.

GitHub workflow는 기존 `release-fast` 프로파일을 세 OS에 공통으로 사용한다.
최초 macOS CI에서 전체 LTO `release` 빌드가 90분 제한에 도달한 반면, Mac mini의
격리된 `release-fast` 빌드는 7분 47초에 완료됐다. 이는 서로 다른 장비의 빌드 시간이며
실행 성능 비교는 아니다. 수동 스크립트 기본값은 `release`이고 `--profile`로 선택한다.
프로파일은 artifact/runtime manifest에 기록하며 게시 검증은 matrix의 프로파일도 확인한다.
심볼을 제거해 장애 진단을 잃거나 역할별 프로그램으로 코드를 복제하지 않는다.

### 크기와 분리 기준

2026-09-22 검증 환경에서 Mac mini `release-fast` 실행 파일은 147,673,560 bytes였고,
로컬 `dashboard/dist`는 240개 파일, 120,429,627 bytes였다. 대시보드의 큰 파일은
대부분 PNG sprite였다. 이 값은 압축 전 크기이며 최종 운영 commit의 측정값은 아니다.
공통 소스를 사용하는 release workflow의 Windows/Linux/macOS native matrix는
run `35651612825`에서 모두 성공했다. 이후 worker 기능의 최종 commit은 별도 검증한다.

현 단계에서는 OS/CPU별 공통 바이너리와 런타임 역할 선택을 유지한다. worker의 UI 자산
설치 생략이 별도 worker 프로그램을 만드는 것보다 작은 변경으로 배포량을 줄인다.
기존 `--skip-dashboard` 옵션으로 효과를 측정할 수 있다. 공개 release는 설치 편의를 위해
완전한 공통 archive를 유지하고, 필요할 때 같은 빌드 결과에서 자산 구성만 나눈다.
서로 다른 역할의 소스·프로토콜·버전·패치 일정을 추가로 관리할 근거는 아직 없다.

초기 workflow의 조합은 다음과 같다. CPU 검사가 실제 runner와 일치하지 않으면 빌드를
중단하며 다른 architecture 파일에 잘못된 이름을 붙이지 않는다.

| 대상 | Rust target | 공개 artifact |
| --- | --- | --- |
| Apple Silicon macOS | aarch64-apple-darwin | agentdesk-darwin-aarch64.tar.gz |
| Windows x86-64 MSVC | x86_64-pc-windows-msvc | agentdesk-windows-x86_64.zip |
| Linux x86-64 GNU, Ubuntu 22.04 빌드 | x86_64-unknown-linux-gnu | agentdesk-linux-x86_64.tar.gz |

모든 archive는 `agentdesk-{os}-{arch}/` 최상위 폴더를 가진다. 실행 파일, 선택한 공통
자산, `VERSION`, `release-manifest.json`, `runtime/release-source.json`을 포함한다.
manifest에는 source commit, Rust target, version, dashboard 포함 여부와 파일별 SHA-256이
기록된다. runtime manifest는 기존 health API의 release identity reader와 같은 형식이다.

압축 파일별 `.sha256`과 기존 installer가 읽는 `checksums.txt`를 생성한다. CI의 각 native
job은 이름이 겹치지 않는 sidecar를 업로드하고 publish job이 검증 후 checksums.txt를 합친다.

## GitHub 실행과 게시

- 패키징 관련 PR에서는 검증 및 세 플랫폼 artifact 빌드를 실행하며 Release는 게시하지 않는다.
- 수동 실행의 기본 `publish=false`도 artifact 생성까지만 실행한다.
- `vVERSION` tag push는 자동 게시한다. `vVERSION-prerelease`는 prerelease로 게시한다.
- 수동 게시에는 `publish=true`와 현재 workflow commit을 가리키는 기존 tag가 필요하다.
- version은 Cargo.toml과 일치해야 하며 모든 artifact가 동일한 깨끗한 commit에서 나와야 한다.
- 모든 platform job이 성공하고 전체 matrix와 파일 hash가 일치해야 게시 job이 진행된다.
- 업로드 중 실패하면 draft 상태가 남는다. 이미 존재하는 Release를 자동 덮어쓰지 않는다.
  재실행 전 운영자가 해당 draft/asset 상태를 확인해야 한다.

빌드 job에는 저장소 읽기 권한만 부여하고 Release 게시 job에만 contents 쓰기 권한을 부여한다.
PR 코드 실행에 배포용 SSH 키나 운영 설정을 제공하지 않는다.

**GitHub Release 게시와 LAN 서버 적용은 별도 단계다.** GitHub-hosted runner에서 사설망
주소로 직접 접속한다고 가정하지 않는다. LAN 배포는 접근 가능한 운영 환경에서 검증된
artifact를 받아 drain → 설치 → migration/기동 → health/readiness 확인을 수행한다.
Mac은 기존 `deploy-release.sh`의 서비스·drain 계약을 재사용한다. Windows는 아래의
사용자 작업 스케줄러 경로를 사용할 수 있다. 실제 두 장비의 적용·테스트 완료 여부는
[클러스터 구현 검토](../design/heterogeneous-worker-cluster-implementation-review.md)에 기록한다.

Mac 배포 시 `AGENTDESK_POST_DEPLOY_SMOKE_SCOPE=api`를 지정하면 기존 drain·migration·
서비스 교체·API·복구 상태 검증을 유지하면서 실제 provider turn과 Discord 테스트 메시지를
생성하는 E-1/E-35만 생략한다. 기본값 `full`은 기존 검증을 유지한다. `api` 결과는
실제 Discord 응답이나 durable delivery E2E 통과를 의미하지 않으며 coverage에 기록한다.

### Windows 사용자 worker

`scripts/install-windows-runtime-task.ps1`은 기존 `%USERPROFILE%\.adk\release` 레이아웃의
`bin\agentdesk.exe`와 `config\agentdesk.yaml`을 확인하고, 해당 사용자로 로그온할 때
`agentdesk dcserver`를 직접 실행하는 작업을 등록한다. PowerShell launcher를 상주시키지
않는다. 중복 시작은 무시하고, 비정상 종료 후 1분 간격으로 최대 999회 재시도한다.
제한된 권한의 `Interactive` principal을 사용하며 암호나 provider 인증 파일을 복사하지 않는다.

```powershell
# 파일과 기존 작업 소유권 확인 후 변경 내용을 미리 보기
.\scripts\install-windows-runtime-task.ps1 -DatabaseSshAlias agentdesk-mac-mini -WhatIf
# 설치된 설정·DB migration 검증 이후 등록하고 시작
.\scripts\install-windows-runtime-task.ps1 -DatabaseSshAlias agentdesk-mac-mini -Start
```

SSH alias를 지정하면 PostgreSQL 터널도 별도 작업으로 등록한다. 기본값은 Windows의
`127.0.0.1:15433`에서 SSH 서버의 `127.0.0.1:5432`로 전달하는 연결이다. SSH는 기존
사용자 설정과 고정된 host key를 사용하며, 비대화식 인증 실패와 포트 충돌 시 종료한다.
worker DB 설정은 이 로컬 포트를 가리켜야 한다. 터널과 worker 시작 순서의 일시적인
경쟁은 worker의 실패 후 재시도로 복구한다. PostgreSQL LAN listen/HBA 변경은 필요 없다.

이 방식은 사용자가 로그인한 Windows PC의 worker에 적합하다. 재부팅 후 로그온 전이나
로그아웃 상태에서의 실행은 제공하지 않는다. 항상 무인 실행해야 하는 Windows 서버는
별도 서비스 계정과 검증된 service host가 필요하다. 일반 console 실행 파일을 `sc create`에
등록하는 것만으로 서비스 계약이 생긴다고 가정하지 않는다. 설정에 service host가 없는 이
설치 경로의 재시작은 `Stop-ScheduledTask`와 `Start-ScheduledTask`로 관리한다.

## 검증

```bash
python -m unittest tests.test_package_release
bash -n scripts/build-release.sh
```

패키징 테스트는 세 OS archive, manifest의 내부 hash, 운영자 파일 제외, dashboard 제외/누락,
잘못된 binary target, source 경계, 잘못된 commit, dirty checkout, 변조되거나 빠진 파일을
검사한다. 이는 native 실행이나 혼합 OS 클러스터 E2E의 대체 증거가 아니다.
