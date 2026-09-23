use super::*;

fn node() -> Value {
    json!({"status":"online", "capabilities": {
        "intake_poller":{"codex":1_000_000},
        "execution_readiness": ExecutionProbe {
            schema:2, boot_id:"boot-a".into(), observed_at_ms:1_000_000, expires_at_ms:1_120_000,
            os:"windows".into(), arch:"x86_64".into(), runtime_profile:crate::config::RuntimeProfile::Runner,
            release:json!({}), providers:BTreeMap::from([("codex".into(),ProviderEvidence {
                cli_installed:true,
                cli_usable:true,version:Some("1.0".into()),failure:None,
                credential_profiles:BTreeMap::from([("default".into(),true)]),
                authentication_verified:false,quota_verified:false,
            })]), tools:BTreeMap::new(),repositories:BTreeMap::new(),backends:vec!["process".into()],disk_free_bytes:None,
        }
    }})
}

#[test]
fn runner_probes_use_canonical_names_and_reject_retired_protocol() {
    let published = node();
    assert_eq!(
        published["capabilities"]["execution_readiness"]["runtime_profile"],
        "runner"
    );
    assert!(evaluate(&published, "codex", "default", 1_000_100).eligible);
    let mut obsolete = published.clone();
    obsolete["capabilities"]["execution_readiness"]["schema"] = json!(1);
    assert_eq!(
        evidence(&obsolete, 1_000_100).unwrap_err(),
        "execution_evidence_protocol"
    );
    let mut invalid = published;
    invalid["capabilities"]["execution_readiness"]["runtime_profile"] = json!("worker");
    assert_eq!(
        evidence(&invalid, 1_000_100).unwrap_err(),
        "execution_evidence_missing"
    );
}

#[test]
fn execution_readiness_separates_liveness_freshness_poller_and_credentials() {
    assert!(evaluate(&node(), "codex", "default", 1_000_100).eligible);
    for (pointer, value, reason) in [
        ("/status", json!("offline"), "node_offline"),
        (
            "/capabilities/execution_readiness",
            Value::Null,
            "execution_evidence_missing",
        ),
        (
            "/capabilities/execution_readiness/expires_at_ms",
            json!(999_999),
            "execution_evidence_stale",
        ),
        (
            "/capabilities/execution_readiness/observed_at_ms",
            json!(1_020_000),
            "execution_evidence_stale",
        ),
        (
            "/capabilities/execution_readiness/providers/codex/cli_usable",
            json!(false),
            "provider_cli_unavailable",
        ),
        (
            "/capabilities/intake_poller/codex",
            json!(900_000),
            "intake_poller_stale",
        ),
        (
            "/capabilities/execution_readiness/providers/codex/credential_profiles/default",
            json!(false),
            "provider_credentials_missing",
        ),
    ] {
        let mut candidate = node();
        *candidate.pointer_mut(pointer).unwrap() = value;
        let report = evaluate(&candidate, "codex", "default", 1_000_100);
        assert!(!report.eligible, "{pointer}");
        assert!(report.reasons.iter().any(|r| r == reason), "{report:?}");
    }
    assert!(!evaluate(&node(), "codex", "unknown-profile", 1_000_100).eligible);
    assert!(!evaluate(&node(), "claude", "default", 1_000_100).eligible);
    // A fresh heartbeat/poller cannot renew an expired CLI/workspace probe.
    let mut candidate = node();
    candidate["capabilities"]["intake_poller"]["codex"] = json!(1_200_000);
    assert!(
        evaluate(&candidate, "codex", "default", 1_200_000)
            .reasons
            .contains(&"execution_evidence_stale".into())
    );
}
