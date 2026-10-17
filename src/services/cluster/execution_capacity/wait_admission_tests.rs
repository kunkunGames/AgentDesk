//! Run the actual blocking admission in a child: LOCAL and node identity are process singletons.
use super::*;
use crate::db::auto_queue::test_support::TestPostgresDb;
use crate::services::platform::with_provider_execution_context;
use crate::services::provider_cli::ProviderExecutionContext;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

struct Policy {
    pool: PgPool,
    deadline: Instant,
    calls: AtomicUsize,
    entered: tokio::sync::Notify,
    reject_reserved: bool,
}

impl AdmissionCheck for Policy {
    fn deadline(&self) -> Instant {
        self.deadline
    }

    fn verify(&self) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + '_>> {
        Box::pin(async {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.entered.notify_one();
            if self.reject_reserved {
                let exists: bool = sqlx::query_scalar(
                    "SELECT EXISTS(SELECT 1 FROM node_execution_leases WHERE instance_id='wait-node' AND channel_id='100')",
                ).fetch_one(&self.pool).await.map_err(|e| e.to_string())?;
                if exists {
                    return Err("authority revoked after reservation".into());
                }
            }
            Ok(())
        })
    }
}

fn attempt(
    pool: PgPool,
    working_dir: String,
    policy: Arc<Policy>,
    continuations: Arc<AtomicUsize>,
) -> tokio::task::JoinHandle<Result<(), String>> {
    tokio::task::spawn_blocking(move || {
        with_provider_execution_context(
            ProviderExecutionContext {
                provider: "opencode".into(),
                agent_id: Some("wait-agent".into()),
                channel_id: Some("100".into()),
                execution_mode: Some("discord_turn".into()),
                ..Default::default()
            },
            || {
                let guard = acquire(
                    Some(&pool),
                    "opencode",
                    100,
                    &working_dir,
                    false,
                    Arc::new(CancelToken::new()),
                    Some(policy.as_ref()),
                )?;
                continuations.fetch_add(1, Ordering::SeqCst);
                drop(guard);
                Ok(())
            },
        )
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn campaign_capacity_wait_rechecks_node_policy_and_finalizes_refused_lease_pg() {
    const CHILD: &str = "AGENTDESK_CAPACITY_WAIT_TEST_CHILD";
    const DATABASE: &str = "AGENTDESK_CAPACITY_WAIT_TEST_DATABASE";
    if std::env::var(CHILD).ok().as_deref() != Some("1") {
        // The parent owns cleanup even if the child panics or its watchdog fires.
        let fixture = TestPostgresDb::create().await;
        fixture.connect_and_migrate().await.close().await;
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("config.yaml");
        let mut config = crate::config::Config::default();
        config.cluster.enabled = true;
        config.cluster.execution_slots = Some(1);
        config.cluster.instance_id = Some("wait-node".into());
        config.data.dir = root.path().join("data");
        std::fs::write(&path, serde_yaml::to_string(&config).unwrap()).unwrap();
        let name = concat!(
            module_path!(),
            "::campaign_capacity_wait_rechecks_node_policy_and_finalizes_refused_lease_pg"
        );
        let name = name.split_once("::").unwrap().1;
        let log_path = root.path().join("child.log");
        let log = std::fs::File::create(&log_path).unwrap();
        let mut command = tokio::process::Command::new(std::env::current_exe().unwrap());
        command
            .args(["--exact", name, "--nocapture", "--test-threads=1"])
            .env(CHILD, "1")
            .env(DATABASE, &fixture.database_url)
            .env("AGENTDESK_CONFIG", &path)
            .env("AGENTDESK_ROOT_DIR", root.path())
            .env("AGENTDESK_INSTANCE_ID", "wait-node")
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .kill_on_drop(true);
        let mut child = command.spawn().unwrap();
        let status = match tokio::time::timeout(Duration::from_secs(180), child.wait()).await {
            Ok(status) => status.unwrap(),
            Err(_) => {
                child.kill().await.unwrap();
                child.wait().await.unwrap();
                fixture.drop().await;
                panic!(
                    "admission child timed out: {}",
                    std::fs::read_to_string(&log_path).unwrap()
                );
            }
        };
        fixture.drop().await;
        let output = std::fs::read_to_string(log_path).unwrap();
        assert!(status.success(), "{status}: {output}");
        assert!(output.contains("1 passed; 0 failed"), "{output}");
        return;
    }

    let pool = crate::db::postgres::connect_test_pool_with_max_connections(
        &std::env::var(DATABASE).unwrap(),
        "campaign admission child",
        4,
    )
    .await
    .unwrap();
    for node in ["wait-node", "foreign-node"] {
        sqlx::query("INSERT INTO worker_nodes(instance_id,last_heartbeat_at,capabilities) VALUES($1,NOW(),$2)")
            .bind(node).bind(json!({"execution_capacity":{"version":1,"slots":1}}))
            .execute(&pool).await.unwrap();
    }
    sqlx::query("INSERT INTO agents(id,name,discord_channel_id,execution_requirements) VALUES('wait-agent','wait-agent','100','{}')")
        .execute(&pool).await.unwrap();
    let foreign = uuid::Uuid::new_v4();
    store::acquire(&pool, "foreign-node", "opencode", "foreign", foreign)
        .await
        .unwrap();
    let (_, slots) = LOCAL.get_or_init(|| (1, Arc::new(Semaphore::new(1))));
    let path = crate::config::resolved_config_path();
    let original = std::fs::read(&path).unwrap();
    let limits = json!({"execution_limits":{"max_elapsed_secs":60,"max_tool_calls":3}});
    let continuations = Arc::new(AtomicUsize::new(0));
    for change in [
        "add_limits",
        "change_limits",
        "slots",
        "enabled",
        "instance",
        "resource",
        "post_pg",
        "successor",
    ] {
        std::fs::write(&path, &original).unwrap();
        let initial = if matches!(change, "change_limits" | "post_pg" | "successor") {
            limits.clone()
        } else {
            json!({})
        };
        assert!(
            super::super::execution_requirements::set(&pool, "wait-agent", &initial)
                .await
                .unwrap()
        );
        let workspace = tempfile::tempdir().unwrap();
        let working = if change == "resource" {
            workspace.path().join("new-workspace")
        } else {
            workspace.path().to_path_buf()
        };
        let policy = Arc::new(Policy {
            pool: pool.clone(),
            deadline: Instant::now() + Duration::from_secs(30),
            calls: AtomicUsize::new(0),
            entered: tokio::sync::Notify::new(),
            reject_reserved: change == "post_pg",
        });
        let busy = slots.clone().try_acquire_owned().unwrap();
        let worker = attempt(
            pool.clone(),
            working.to_string_lossy().into_owned(),
            policy.clone(),
            continuations.clone(),
        );
        tokio::time::timeout(Duration::from_secs(10), async {
            while policy.calls.load(Ordering::SeqCst) < 3 {
                policy.entered.notified().await;
            }
        })
        .await
        .unwrap();
        assert!(!worker.is_finished(), "must be queued before {change}");
        match change {
            "add_limits" | "change_limits" => {
                let next = if change == "add_limits" {
                    limits.clone()
                } else {
                    json!({"execution_limits":{"max_elapsed_secs":120,"max_tool_calls":9}})
                };
                assert!(
                    super::super::execution_requirements::set(&pool, "wait-agent", &next)
                        .await
                        .unwrap()
                );
            }
            "slots" | "enabled" | "instance" => {
                let mut changed: crate::config::Config = serde_yaml::from_slice(&original).unwrap();
                match change {
                    "slots" => changed.cluster.execution_slots = Some(2),
                    "enabled" => changed.cluster.enabled = false,
                    _ => changed.cluster.instance_id = Some("replacement-node".into()),
                }
                std::fs::write(&path, serde_yaml::to_string(&changed).unwrap()).unwrap();
            }
            "resource" => std::fs::create_dir(&working).unwrap(),
            _ => {}
        }
        drop(busy);
        let result = tokio::time::timeout(Duration::from_secs(15), worker)
            .await
            .unwrap()
            .unwrap();
        if change == "successor" {
            result.unwrap();
            assert_eq!(continuations.load(Ordering::SeqCst), 1);
        } else {
            let error = result.unwrap_err();
            assert!(
                error.contains(if change == "post_pg" {
                    "authority revoked"
                } else {
                    "settings changed"
                }),
                "{change}: {error}"
            );
            assert_eq!(continuations.load(Ordering::SeqCst), 0);
        }
        assert_eq!(slots.available_permits(), 1, "{change}");
        let remaining: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM node_execution_leases WHERE instance_id='wait-node'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(remaining, 0, "{change}: reserved lease must be finalized");
        assert!(
            store::renew(&pool, "foreign-node", "opencode", "foreign", foreign)
                .await
                .unwrap()
        );
    }
    store::release(&pool, "foreign-node", "opencode", "foreign", foreign)
        .await
        .unwrap();
    pool.close().await;
}
