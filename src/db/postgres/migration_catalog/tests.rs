use super::*;
use crate::db::auto_queue::test_support::TestPostgresDb;
use serde_json::{Value, json};

pub(super) async fn install_fixture(pool: &PgPool, lineage: Lineage, version: i64) {
    let catalog = lineage.migrator();
    let fixture = Migrator {
        migrations: Cow::Owned(
            catalog
                .iter()
                .filter(|m| m.version <= version)
                .cloned()
                .collect(),
        ),
        ..Migrator::DEFAULT
    };
    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(pool)
        .await
        .unwrap();
    assert!(database.starts_with("agentdesk_db_auto_queue_"));
    let mut conn = pool.acquire().await.unwrap().detach();
    let result = fixture.run_direct(&mut conn).await;
    conn.close().await.unwrap();
    result.unwrap();
}

async fn empty_fixture() -> (TestPostgresDb, PgPool) {
    let fixture = TestPostgresDb::create().await;
    let pool = crate::db::postgres::connect_test_pool_with_max_connections(
        &fixture.database_url,
        "migration lineage tests",
        4,
    )
    .await
    .unwrap();
    (fixture, pool)
}

async fn ledger(pool: &PgPool) -> Value {
    sqlx::query_scalar("SELECT COALESCE(jsonb_agg(to_jsonb(m) ORDER BY version),'[]'::jsonb) FROM _sqlx_migrations m")
        .fetch_one(pool).await.unwrap()
}

async fn close(fixture: TestPostgresDb, pool: PgPool) {
    pool.close().await;
    fixture.drop().await;
}

fn catalog_receipts(lineage: Lineage) -> Vec<Receipt> {
    lineage
        .migrator()
        .iter()
        .filter(|m| !m.migration_type.is_down_migration())
        .map(|m| Receipt {
            version: m.version,
            description: m.description.to_string(),
            success: true,
            checksum: m.checksum.to_vec(),
        })
        .collect()
}

#[test]
fn immutable_catalogs_preserve_gaps_up_checksums_and_convergence() {
    for lineage in [Lineage::Upstream, Lineage::CampaignV5] {
        let rows = catalog_receipts(lineage);
        assert_eq!(rows.last().unwrap().version, 146);
        assert!(!rows.iter().any(|r| r.version == 17));
        assert_eq!(inferred_lineage(&rows), Some(lineage));
        for end in 0..=rows.len() {
            validate_prefix(&rows[..end], lineage).unwrap();
        }
    }
    let native = catalog_receipts(Lineage::CampaignV5);
    let checksum = &native.iter().find(|r| r.version == 140).unwrap().checksum;
    assert_eq!(
        hex::encode(checksum),
        "cd511419d9365160f041dc4e78f02dbcc3b8e4272601f700398a4f0102173d8c5861ca9842245dfe49cf34c9e0474258"
    );
    assert!(
        super::super::POSTGRES_MIGRATOR
            .iter()
            .all(|m| m.version <= 140 || m.version == 146)
    );
    assert!(SHARED.iter().all(|m| m.version >= 147));
}

#[test]
fn mixed_failed_missing_and_unknown_receipts_never_admit() {
    let mut rows = catalog_receipts(Lineage::CampaignV5);
    rows.iter_mut().find(|r| r.version == 132).unwrap().checksum =
        catalog_receipts(Lineage::Upstream)
            .into_iter()
            .find(|r| r.version == 132)
            .unwrap()
            .checksum;
    assert_eq!(inferred_lineage(&rows), None);
    for lineage in [Lineage::Upstream, Lineage::CampaignV5] {
        assert!(validate_prefix(&rows, lineage).is_err());
    }
    let mut rows = catalog_receipts(Lineage::CampaignV5);
    rows[3].success = false;
    assert!(validate_prefix(&rows, Lineage::CampaignV5).is_err());
    rows[3].success = true;
    rows.remove(3);
    assert!(validate_prefix(&rows, Lineage::CampaignV5).is_err());
    rows[0].version = -1;
    assert!(validate_prefix(&rows, Lineage::CampaignV5).is_err());
}

#[tokio::test]
async fn native_history_and_office_rows_survive_convergence_and_reentry_pg() {
    let (fixture, pool) = empty_fixture().await;
    install_fixture(&pool, Lineage::CampaignV5, 145).await;
    sqlx::raw_sql("INSERT INTO offices(id,name) VALUES('preserved-office','legacy office');
        INSERT INTO departments(id,name,office_id) VALUES('preserved-department','legacy department','preserved-office');")
        .execute(&pool).await.unwrap();
    let before = ledger(&pool).await;
    migrate(&pool).await.unwrap();
    let after = ledger(&pool).await;
    assert_eq!(
        &after.as_array().unwrap()[..before.as_array().unwrap().len()],
        before.as_array().unwrap()
    );
    assert_eq!(after.as_array().unwrap().last().unwrap()["version"], 146);
    let office: String =
        sqlx::query_scalar("SELECT office_id FROM departments WHERE id='preserved-department'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(office, "preserved-office");
    let objects: bool = sqlx::query_scalar("SELECT to_regclass('channel_recovery_custody') IS NOT NULL AND to_regclass('campaign_graph_occurrences') IS NOT NULL")
        .fetch_one(&pool).await.unwrap();
    assert!(objects);
    migrate(&pool).await.unwrap();
    assert_eq!(ledger(&pool).await, after);
    assert!(
        crate::db::postgres::applied_migration_checksum_mismatch_details(&pool)
            .await
            .unwrap()
            .is_empty()
    );
    close(fixture, pool).await;
}

#[tokio::test]
async fn upstream_history_converges_and_concurrent_starters_apply_once_pg() {
    let (fixture, pool) = empty_fixture().await;
    install_fixture(&pool, Lineage::Upstream, 140).await;
    let before = ledger(&pool).await;
    let other = crate::db::postgres::connect_test_pool_with_max_connections(
        &fixture.database_url,
        "second migration starter",
        2,
    )
    .await
    .unwrap();
    let (first, second) = tokio::join!(migrate(&pool), migrate(&other));
    first.unwrap();
    second.unwrap();
    let after = ledger(&pool).await;
    assert_eq!(
        &after.as_array().unwrap()[..before.as_array().unwrap().len()],
        before.as_array().unwrap()
    );
    assert_eq!(
        after.as_array().unwrap().len(),
        before.as_array().unwrap().len() + 1
    );
    let tables: bool = sqlx::query_scalar("SELECT to_regclass('campaign_graph_occurrences') IS NOT NULL AND to_regclass('offices') IS NULL")
        .fetch_one(&pool).await.unwrap();
    assert!(tables);
    other.close().await;
    close(fixture, pool).await;
}

#[tokio::test]
async fn fresh_database_uses_upstream_and_has_both_capabilities_pg() {
    let (fixture, pool) = empty_fixture().await;
    migrate(&pool).await.unwrap();
    let mut conn = pool.acquire().await.unwrap();
    assert_eq!(
        inferred_lineage(&receipts(&mut conn).await.unwrap()),
        Some(Lineage::Upstream)
    );
    drop(conn);
    let status = crate::db::postgres::migration_status(&pool).await.unwrap();
    assert!(status.pending_versions.is_empty() && status.missing_from_resolved.is_empty());
    assert!(status.history_issues.is_empty());
    close(fixture, pool).await;
}

#[tokio::test]
async fn refused_history_is_unchanged_and_diagnostics_remain_available_pg() {
    let (fixture, pool) = empty_fixture().await;
    install_fixture(&pool, Lineage::CampaignV5, 145).await;
    let pending = crate::db::postgres::migration_status(&pool).await.unwrap();
    assert_eq!(pending.pending_versions, vec![146]);
    assert!(pending.history_issues.is_empty());
    sqlx::query("DELETE FROM _sqlx_migrations WHERE version=141")
        .execute(&pool)
        .await
        .unwrap();
    let before = ledger(&pool).await;
    assert!(
        migrate(&pool)
            .await
            .unwrap_err()
            .contains("exact successful prefix")
    );
    assert_eq!(ledger(&pool).await, before);
    assert!(
        crate::db::postgres::migration_status(&pool)
            .await
            .unwrap()
            .pending_versions
            .contains(&141)
    );
    sqlx::query("INSERT INTO _sqlx_migrations(version,description,success,checksum,execution_time) VALUES(999,'future',true,decode('00','hex'),0)")
        .execute(&pool).await.unwrap();
    let status = crate::db::postgres::migration_status(&pool).await.unwrap();
    assert_eq!(status.missing_from_resolved, vec![999]);
    assert!(!status.history_issues.is_empty());
    assert!(migrate(&pool).await.is_err());
    close(fixture, pool).await;
}

#[tokio::test]
async fn convergence_ddl_failure_rolls_back_and_releases_connection_lock_pg() {
    let (fixture, pool) = empty_fixture().await;
    install_fixture(&pool, Lineage::CampaignV5, 145).await;
    sqlx::query("CREATE TABLE o_channel_homes(conflicting_fixture bool)")
        .execute(&pool)
        .await
        .unwrap();
    let before = ledger(&pool).await;
    assert!(migrate(&pool).await.is_err());
    assert_eq!(ledger(&pool).await, before);
    let partially_applied: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name='sessions' AND column_name='hosted_execution')")
        .fetch_one(&pool).await.unwrap();
    assert!(!partially_applied);
    sqlx::query("DROP TABLE o_channel_homes")
        .execute(&pool)
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(10), migrate(&pool))
        .await
        .unwrap()
        .unwrap();
    close(fixture, pool).await;
}

#[tokio::test]
async fn native_140_forward_upgrade_keeps_first_activation_budget_pg() {
    let (fixture, pool) = empty_fixture().await;
    install_fixture(&pool, Lineage::CampaignV5, 140).await;
    sqlx::query("INSERT INTO campaigns(id,revision,document) VALUES('legacy-budget',1,$1)")
        .bind(json!({"id":"legacy-budget","revision":1,"status":"paused"}))
        .execute(&pool)
        .await
        .unwrap();
    let original = json!({"schedule":{"timezone":"UTC","daily_minutes":[0]},"budget":{"max_elapsed_secs":600,"max_evolutions":3}});
    let revised = json!({"schedule":{"timezone":"UTC","daily_minutes":[0]},"budget":{"max_elapsed_secs":2400,"max_evolutions":15}});
    sqlx::query(
        "INSERT INTO campaign_execution_specs(campaign_id,spec) VALUES('legacy-budget',$1)",
    )
    .bind(&revised)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO campaign_schedule_revisions(campaign_id,request_id,request_digest,campaign_revision,previous_goal,revised_goal,evidence) VALUES('legacy-budget','first','receipt',1,$1,$2,'historical activation')")
        .bind(&original).bind(&revised).execute(&pool).await.unwrap();
    migrate(&pool).await.unwrap();
    let budget: Value = sqlx::query_scalar(
        "SELECT occurrence_budget FROM campaign_execution_specs WHERE campaign_id='legacy-budget'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(budget, original["budget"]);
    close(fixture, pool).await;
}

#[tokio::test]
async fn unrecorded_schema_and_ambiguous_common_prefix_are_not_migrated_pg() {
    let (fixture, pool) = empty_fixture().await;
    sqlx::query("CREATE TABLE existing_application_data(value text)")
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        migrate(&pool)
            .await
            .unwrap_err()
            .contains("without migration receipts")
    );
    sqlx::query("DROP TABLE existing_application_data")
        .execute(&pool)
        .await
        .unwrap();
    install_fixture(&pool, Lineage::Upstream, 130).await;
    let before = ledger(&pool).await;
    assert!(migrate(&pool).await.unwrap_err().contains("ambiguous"));
    assert_eq!(ledger(&pool).await, before);
    close(fixture, pool).await;
}

#[tokio::test]
async fn unrecorded_functions_types_and_secondary_search_schema_are_preserved_pg() {
    let (fixture, pool) = empty_fixture().await;
    sqlx::raw_sql("CREATE FUNCTION json_extract(jsonb,text) RETURNS text LANGUAGE sql AS 'SELECT ''preserved''::text';
        CREATE TYPE preserved_enum AS ENUM ('original');")
        .execute(&pool).await.unwrap();
    assert!(
        migrate(&pool)
            .await
            .unwrap_err()
            .contains("without migration receipts")
    );
    let value: String = sqlx::query_scalar("SELECT json_extract('{}'::jsonb,'x')")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(value, "preserved");
    sqlx::query("DROP FUNCTION json_extract(jsonb,text)")
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        migrate(&pool)
            .await
            .unwrap_err()
            .contains("without migration receipts")
    );
    sqlx::raw_sql(
        "DROP TYPE preserved_enum; CREATE SCHEMA retained;
        CREATE DOMAIN retained.preserved_domain AS text;",
    )
    .execute(&pool)
    .await
    .unwrap();
    let mut conn = pool.acquire().await.unwrap();
    sqlx::query("SET search_path=public,retained")
        .execute(&mut *conn)
        .await
        .unwrap();
    assert!(
        choose_for_execution(&mut conn, &[])
            .await
            .unwrap_err()
            .contains("without migration receipts")
    );
    let unchanged: bool = sqlx::query_scalar("SELECT to_regtype('retained.preserved_domain') IS NOT NULL AND to_regclass('_sqlx_migrations') IS NULL")
        .fetch_one(&mut *conn).await.unwrap();
    assert!(unchanged);
    drop(conn);
    close(fixture, pool).await;
}
