# PostgreSQL migration 번호 이관

2026-09-16 fork 릴리스(`d06d1d234`)에서 Kakao Calendar migration은 120/121번이었다.
upstream 병합 후 같은 SQL은 122/123번이며, 현재 120/121번에는 각각 relay redelivery와
campaigns migration이 있다. 기존 DB에 일반 SQLx migration만 실행하면 checksum 충돌로
업그레이드가 중단된다.

`src/db/postgres/migration_compat.rs`는 이 두 건의 **이전 번호, 설명, 고정 SHA-384,
현재 SQL의 SHA-384**가 모두 일치할 때 migration 이력의 번호만 바꾼다. 적용 시각,
checksum, 실행 시간과 업무 데이터는 수정하지 않는다. 이후 SQLx가 기존 검증을 그대로
수행하며 빠진 migration을 적용한다.

| 이전 번호 | 설명 | 현재 번호 |
| --- | --- | --- |
| 120 | kakao calendar sync | 122 |
| 121 | kakao calendar lease expiry | 123 |

전체 적용 이력에 알 수 없는 번호, 수정된 SQL, 미완료 migration, 이미 점유된 목적지
번호가 있으면 이관 전에 실패한다. 비슷한 SQL을 추정해 이동하거나 checksum을 갱신하지
않는다. 기존 startup lock과 별도로 SQLx 자체의 PostgreSQL advisory lock을 같은 연결에서
잡아 구버전 migrator와의 경쟁도 막는다. 이관은 단일 transaction으로 실행하며 migration
연결은 성공·실패·취소 시 pool에 반환하지 않고 닫는다.

운영 적용 순서:

1. 전체 노드의 신규 작업을 중지하고 실행 중 작업을 drain한다.
2. 운영 DB의 `pg_dump --format=custom` 백업과 migration 이력을 보관한다. 백업은 운영자
   전용 경로에서 관리하며 저장소나 공개 artifact에 넣지 않는다.
3. 백업 복원본에서 새 binary의 `release-migrate-postgres`를 먼저 검증한다. 운영에서
   같은 명령으로 이관한 뒤 새 릴리스의 허브를 시작하고 health/schema를 확인한다.
4. 동일 릴리스 실행 노드를 시작하고 중앙 설정 보존과 원격 작업을 확인한다.

두 프로세스가 동시에 업그레이드해도 migration lock이 직렬화한다. 이미 이관한 DB나
새 DB에서는 같은 이관을 반복하지 않는다. 새 schema 적용 후에는 구버전 binary만으로
되돌리지 않는다. rollback에는 drain 후 검증된 백업 복원과 구버전 runtime 복원이 필요하다.

기존 `deploy-release.sh`의 읽기 전용 doctor는 아직 이관하지 않은 120/121을 checksum
충돌로 표시하고 중단한다. 이 한 번의 번호 이관은 위 명령으로 먼저 적용한다. 이후의
배포는 수정하지 않은 doctor의 엄격한 checksum 검증을 그대로 통과해야 한다.

격리 PostgreSQL 검증은 `postgres_migration_relocation_` 테스트에서 구버전 schema를
재구성하고 업무 데이터 보존, 동시 업그레이드, 재실행, drift/충돌 시 무변경,
취소 시 lock 반환을 확인한다. 운영 DB 적용 완료 여부는 클러스터 구현 문서의 배포 기록에
별도로 남긴다.
