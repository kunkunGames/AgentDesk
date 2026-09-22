use super::pg_tests::{
    ctx_for_channel, seed_agent_with_preference, seed_session_owner,
    seed_worker_node_with_capabilities,
};
use super::*;
use crate::db::auto_queue::test_support::TestPostgresDb;
use crate::services::cluster::execution_requirements::tests::ready_node;
use serde_json::json;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn agent_default_inherits_into_thread_and_pins_retry_without_moving_live_owner_pg() {
    let fixture = TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    seed_agent_with_preference(&pool, "default-agent", "8331", json!([])).await;
    sqlx::query("UPDATE agents SET default_execution_node_id='win' WHERE id='default-agent'")
        .execute(&pool)
        .await
        .unwrap();
    for (id, os) in [("leader-1", "macos"), ("win", "windows")] {
        seed_worker_node_with_capabilities(
            &pool,
            id,
            json!([]),
            "online",
            ready_node(id, os)["capabilities"].clone(),
        )
        .await;
    }
    let mut ctx = ctx_for_channel(IntakeRoutingMode::Enforce, "8332");
    ctx.policy_channel_id = "8331";
    let id = match try_route_intake(&pool, &ctx).await {
        IntakeRouterDecision::Forwarded {
            target_instance_id,
            outbox_id,
            ..
        } => {
            assert_eq!(target_instance_id, "win");
            outbox_id
        }
        other => panic!("{other:?}"),
    };
    let snapshot: (String, String, serde_json::Value) = sqlx::query_as(
        "SELECT channel_id, agent_id, execution_requirements FROM intake_outbox WHERE id=$1",
    )
    .bind(id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        snapshot,
        (
            "8332".into(),
            "default-agent".into(),
            json!({"nodes":["win"]})
        )
    );
    let central: serde_json::Value =
        sqlx::query_scalar("SELECT execution_requirements FROM agents WHERE id='default-agent'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(central, json!({}));

    // A different pre-existing conversation on this agent keeps its Mac owner.
    seed_session_owner(
        &pool,
        "claude:default-owner",
        "claude",
        "8333",
        "leader-1",
        "turn_active",
    )
    .await;
    ctx.channel_id = "8333";
    assert!(matches!(
        try_route_intake(&pool, &ctx).await,
        IntakeRouterDecision::RanLocal {
            reason: RanLocalReason::LiveSessionOwnerIsLocal
        }
    ));

    // A channel override outranks the agent default for an ownerless conversation.
    ctx.channel_id = "8334";
    ctx.node_override_instance_id = Some("leader-1");
    assert!(matches!(
        try_route_intake(&pool, &ctx).await,
        IntakeRouterDecision::RanLocal {
            reason: RanLocalReason::NodeOverrideIsLeader
        }
    ));
    pool.close().await;
    fixture.drop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unavailable_agent_default_rejects_instead_of_falling_back_pg() {
    let fixture = TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    seed_agent_with_preference(&pool, "offline-default", "8441", json!([])).await;
    sqlx::query(
        "UPDATE agents SET default_execution_node_id='missing-worker' WHERE id='offline-default'",
    )
    .execute(&pool)
    .await
    .unwrap();
    let mut ctx = ctx_for_channel(IntakeRoutingMode::Enforce, "8441");
    for mode in [
        IntakeRoutingMode::Enforce,
        IntakeRoutingMode::Disabled,
        IntakeRoutingMode::Observe,
    ] {
        ctx.mode = mode;
        assert!(matches!(
            try_route_intake(&pool, &ctx).await,
            IntakeRouterDecision::Blocked { .. }
        ));
    }
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM intake_outbox")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
    pool.close().await;
    fixture.drop().await;
}
