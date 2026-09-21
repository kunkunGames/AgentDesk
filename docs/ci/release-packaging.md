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
```

`AGENTDESK_PYTHON`으로 Python 실행 파일을 지정할 수 있다. `CARGO_TARGET_DIR`을 설정한
빌드도 해당 위치의 산출물을 사용한다. `--target` 생략 시 rustc host triple을 기준으로 한다.

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
Mac은 기존 `deploy-release.sh`의 서비스·drain 계약을 재사용하고 Windows는 native 서비스
관리 경로를 사용한다. 실제 두 장비의 적용·테스트 완료 여부는
[클러스터 구현 검토](../design/heterogeneous-worker-cluster-implementation-review.md)에 기록한다.

## 검증

```bash
python -m unittest tests.test_package_release
bash -n scripts/build-release.sh
```

패키징 테스트는 세 OS archive, manifest의 내부 hash, 운영자 파일 제외, dashboard 제외/누락,
잘못된 binary target, source 경계, 잘못된 commit, dirty checkout, 변조되거나 빠진 파일을
검사한다. 이는 native 실행이나 혼합 OS 클러스터 E2E의 대체 증거가 아니다.
