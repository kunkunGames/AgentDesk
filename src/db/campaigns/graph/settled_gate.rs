//! Gate references use graph-local node IDs; persistent task keys are separate.
use super::{SettledEffectGate, Task};
use anyhow::{Result, bail, ensure};
use std::collections::{BTreeMap, BTreeSet};

pub(super) fn validate(gates: &[SettledEffectGate], nodes: &BTreeMap<String, &Task>) -> Result<()> {
    ensure!(gates.len() <= 16, "too many settled effect gates");
    let mut audits = BTreeSet::new();
    for (index, gate) in gates.iter().enumerate() {
        ensure!(
            gate.audit_sha256.len() == 64
                && gate
                    .audit_sha256
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
            "settled_effect_gates[{index}].audit_sha256 must be 64 lowercase hex characters"
        );
        ensure!(
            audits.insert(&gate.audit_sha256),
            "settled_effect_gates[{index}] repeats an audit_sha256"
        );
        ensure!(
            gate.inspection != gate.verification,
            "settled_effect_gates[{index}] requires distinct inspection and verification node IDs"
        );
        for (field, reference) in [
            ("inspection", &gate.inspection),
            ("verification", &gate.verification),
        ] {
            if nodes.contains_key(reference) {
                continue;
            }
            if let Some(node) = nodes.values().find(|node| node.task_key == *reference) {
                bail!(
                    "settled_effect_gates[{index}].{field} contains a task_key; use node.id '{}' instead",
                    node.id
                );
            }
            bail!("settled_effect_gates[{index}].{field} must reference an existing node.id");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::campaigns::graph::{Effect, Format, Kind, Output};

    fn task(id: &str) -> Task {
        Task {
            id: id.into(),
            task_key: format!("campaign:source:{id}"),
            title: id.into(),
            agent: "coder".into(),
            kind: Kind::Inspect,
            effect: Effect::ArtifactOnly,
            details: "Read current evidence".into(),
            acceptance: vec!["Current proof".into()],
            dependencies: BTreeSet::new(),
            inputs: vec![],
            outputs: vec![Output {
                path: format!("{id}.json"),
                format: Format::Json,
            }],
        }
    }

    fn check(gates: &[SettledEffectGate]) -> Result<()> {
        let inspect = task("settled_inspect");
        let verify = task("verify_settled");
        validate(
            gates,
            &[(inspect.id.clone(), &inspect), (verify.id.clone(), &verify)].into(),
        )
    }

    fn gate() -> SettledEffectGate {
        SettledEffectGate {
            audit_sha256: "a".repeat(64),
            inspection: "settled_inspect".into(),
            verification: "verify_settled".into(),
        }
    }

    #[test]
    fn existing_node_ids_are_valid_and_persistent_keys_are_not_rewritten() {
        check(&[gate()]).unwrap();
        for field in ["inspection", "verification"] {
            let mut bad = gate();
            let value = if field == "inspection" {
                &mut bad.inspection
            } else {
                &mut bad.verification
            };
            let expected = value.clone();
            *value = format!("campaign:source:{expected}");
            let original = bad.clone();
            let error = check(&[bad.clone()]).unwrap_err().to_string();
            assert!(error.contains(&format!("[0].{field} contains a task_key")));
            assert!(error.contains(&format!("use node.id '{expected}'")));
            assert_eq!(bad, original);
        }
    }

    #[test]
    fn malformed_audit_and_reference_failures_are_distinct() {
        let mut bad = gate();
        bad.inspection = "missing".into();
        assert!(
            check(&[bad])
                .unwrap_err()
                .to_string()
                .contains("inspection must reference an existing node.id")
        );
        let mut bad = gate();
        bad.audit_sha256 = "A".repeat(64);
        assert!(
            check(&[bad])
                .unwrap_err()
                .to_string()
                .contains("64 lowercase hex")
        );
        assert!(
            check(&[gate(), gate()])
                .unwrap_err()
                .to_string()
                .contains("[1] repeats an audit_sha256")
        );
        let mut bad = gate();
        bad.verification = bad.inspection.clone();
        assert!(
            check(&[bad])
                .unwrap_err()
                .to_string()
                .contains("distinct inspection and verification")
        );
        assert!(
            check(&vec![gate(); 17])
                .unwrap_err()
                .to_string()
                .contains("too many")
        );
    }
}
