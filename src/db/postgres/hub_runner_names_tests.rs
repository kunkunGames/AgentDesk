use std::borrow::Cow;

use serde_json::json;
use sqlx::migrate::Migrator;

use super::{POSTGRES_MIGRATOR, connect_test_pool};
use crate::db::auto_queue::test_support::TestPostgresDb;

#[tokio::test]
async fn hub_runner_migration_preserves_references_and_capacity_enforcement_pg() {
    let fixture = TestPostgresDb::create().await;
    let pool = connect_test_pool(&fixture.database_url, "hub runner schema cutover")
        .await
        .unwrap();
    let before = Migrator {
        migrations: Cow::Owned(
            POSTGRES_MIGRATOR
                .iter()
                .filter(|m| m.version < 129)
                .cloned()
                .collect(),
        ),
        ..Migrator::DEFAULT
    };
    before.run(&pool).await.unwrap();
    sqlx::query("INSERT INTO worker_nodes(instance_id,role,effective_role,capabilities) VALUES('device-a','worker','worker',$1)")
        .bind(json!({"intake_worker":{"providers":["codex"]},"execution_capacity":{"version":1,"slots":1},"execution_readiness":{"runtime_profile":"worker"},"execution_readiness_version":1,"operator_marker":"preserved"}))
        .execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO worker_mcp_endpoints(instance_id,endpoint_name,healthy) VALUES('device-a','fixture',true)")
        .execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO agents(id,name,default_execution_node_id) VALUES('fixture-agent','fixture','device-a')")
        .execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO node_execution_leases VALUES('device-a','codex','active-channel',$1,NOW()+INTERVAL '1 minute')")
        .bind(uuid::Uuid::new_v4()).execute(&pool).await.unwrap();
    assert!(POSTGRES_MIGRATOR.run(&pool).await.is_err());
    assert!(
        sqlx::query_scalar::<_, bool>("SELECT to_regclass('worker_nodes') IS NOT NULL")
            .fetch_one(&pool)
            .await
            .unwrap()
    );
    sqlx::query("DELETE FROM node_execution_leases")
        .execute(&pool)
        .await
        .unwrap();
    POSTGRES_MIGRATOR.run(&pool).await.unwrap();
    POSTGRES_MIGRATOR.run(&pool).await.unwrap();
    let node: serde_json::Value =
        sqlx::query_scalar("SELECT to_jsonb(n) FROM cluster_nodes n WHERE instance_id='device-a'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(node["role"], "runner");
    assert_eq!(node["effective_role"], "runner");
    assert_eq!(node["status"], "offline");
    assert_eq!(node["capabilities"]["operator_marker"], "preserved");
    assert_eq!(
        node["capabilities"]["intake_runner"]["providers"],
        json!(["codex"])
    );
    assert!(node["capabilities"].get("execution_readiness").is_none());
    assert!(node["capabilities"].get("intake_worker").is_none());
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT default_execution_node_id FROM agents WHERE id='fixture-agent'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        "device-a"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM node_mcp_endpoints WHERE instance_id='device-a'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );
    assert!(sqlx::query_scalar::<_, bool>("SELECT to_regclass('worker_nodes') IS NULL AND to_regclass('worker_mcp_endpoints') IS NULL").fetch_one(&pool).await.unwrap());
    assert!(
        sqlx::query("UPDATE cluster_nodes SET role='worker' WHERE instance_id='device-a'")
            .execute(&pool)
            .await
            .is_err()
    );
    sqlx::query("INSERT INTO node_execution_leases VALUES('device-a','codex','channel-a',$1,NOW()+INTERVAL '1 minute')")
        .bind(uuid::Uuid::new_v4()).execute(&pool).await.unwrap();
    let rejected = sqlx::query("INSERT INTO node_execution_leases VALUES('device-a','codex','channel-b',$1,NOW()+INTERVAL '1 minute')")
        .bind(uuid::Uuid::new_v4()).execute(&pool).await.unwrap_err();
    assert_eq!(
        rejected.as_database_error().unwrap().constraint(),
        Some("node_execution_capacity_available")
    );
    assert!(
        sqlx::query("SELECT rename_cluster_node_identity('device-a','device-b')")
            .execute(&pool)
            .await
            .is_err()
    );
    sqlx::query("DELETE FROM node_execution_leases")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO sessions(session_key,agent_id,instance_id) VALUES('fixture-session','fixture-agent','device-a')")
        .execute(&pool).await.unwrap();
    sqlx::query("UPDATE agents SET execution_requirements=$1 WHERE id='fixture-agent'")
        .bind(json!({"nodes":["device-a","other-device"],"os":["windows"]}))
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO task_dispatches(id,required_capabilities) VALUES('fixture-dispatch',$1)",
    )
    .bind(json!({"execution":{"nodes":["device-a"],"tools":["git"]},"marker":"device-a"}))
    .execute(&pool)
    .await
    .unwrap();
    for invalid in ["device-a", "bad/node", ""] {
        assert!(
            sqlx::query("SELECT rename_cluster_node_identity('device-a',$1)")
                .bind(invalid)
                .execute(&pool)
                .await
                .is_err()
        );
    }
    let renamed: serde_json::Value =
        sqlx::query_scalar("SELECT rename_cluster_node_identity('device-a','device-b')")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(renamed["sessions.instance_id"], 1);
    assert_eq!(renamed["agents.default_execution_node_id"], 1);
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT instance_id FROM sessions WHERE session_key='fixture-session'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        "device-b"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT instance_id FROM node_mcp_endpoints WHERE endpoint_name='fixture'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        "device-b"
    );
    assert_eq!(
        sqlx::query_scalar::<_, serde_json::Value>(
            "SELECT execution_requirements FROM agents WHERE id='fixture-agent'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        json!({"nodes":["device-b","other-device"],"os":["windows"]})
    );
    assert_eq!(
        sqlx::query_scalar::<_, serde_json::Value>(
            "SELECT required_capabilities FROM task_dispatches WHERE id='fixture-dispatch'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        json!({"execution":{"nodes":["device-b"],"tools":["git"]},"marker":"device-a"})
    );
    sqlx::query(
        "INSERT INTO cluster_nodes(instance_id,role,effective_role) VALUES('taken','hub','hub')",
    )
    .execute(&pool)
    .await
    .unwrap();
    assert!(
        sqlx::query("SELECT rename_cluster_node_identity('device-b','taken')")
            .execute(&pool)
            .await
            .is_err()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT default_execution_node_id FROM agents WHERE id='fixture-agent'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        "device-b"
    );
    pool.close().await;
    fixture.drop().await;
}
