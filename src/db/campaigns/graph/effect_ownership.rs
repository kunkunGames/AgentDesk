//! Native database ownership observation, never an execution lease or OS exit proof.
mod control_history;
mod rows;

use super::{Goal, Task, contract_digest, digest, recovery::replan};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::PgPool;
use std::{collections::BTreeSet, time::Duration};

pub const COVERAGE: [&str; 8] = [
    "all_dispatch_states",
    "assigned_slots",
    "cleanup_and_direct_rollback",
    "live_cwd_thread_sessions",
    "live_dispatch_sessions",
    "queue_runs_entries",
    "released_effect_latest_raw_dispatch",
    "unsettled_owner_stop",
];

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Query {
    /// Explicit configured business owner scope, not inferred from a repo name.
    pub owner_campaign_ids: String,
    pub active_version: i64,
    pub task_key: String,
    pub contract_digest: String,
    pub card_id: String,
    pub dispatch_id: String,
    pub agent_id: String,
}
impl Query {
    pub fn validate(&self, campaign: &str) -> Result<Vec<String>> {
        fn identity(value: &str, max: usize) -> bool {
            !value.is_empty()
                && value.len() <= max
                && value
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"._:-".contains(&b))
        }
        ensure!(
            identity(campaign, 128)
                && self.active_version > 0
                && (5..=200).contains(&self.task_key.len())
                && !self.task_key.contains('\0')
                && identity(&self.card_id, 128)
                && identity(&self.dispatch_id, 128)
                && identity(&self.agent_id, 128)
                && self.contract_digest.len() == 64
                && self
                    .contract_digest
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
            "bounded exact native owner identities required"
        );
        ensure!(
            self.owner_campaign_ids.len() <= 515,
            "bounded campaign owner scope required"
        );
        let mut owners = BTreeSet::new();
        for id in self.owner_campaign_ids.split(',') {
            ensure!(
                identity(id, 128) && owners.insert(id.to_owned()),
                "unique bounded owner campaigns required"
            );
        }
        ensure!(
            (1..=4).contains(&owners.len()) && owners.contains(campaign),
            "owner scope must include the selected campaign"
        );
        Ok(owners.into_iter().collect())
    }
}

#[derive(Debug, Serialize)]
pub struct Envelope {
    pub effect_ownership: Projection,
}
#[derive(Debug, Serialize)]
pub struct Projection {
    pub schema: &'static str,
    pub coverage: [&'static str; 8],
    pub queue_coverage: [&'static str; 4],
    pub campaign_ids: Vec<String>,
    pub active_version: i64,
    pub current_task_key: String,
    pub current_contract_digest: String,
    pub current_dispatch_id: String,
    pub current_card_id: String,
    pub current_agent_id: String,
    pub observed_at: String,
    pub truncated: bool,
    pub allowed: bool,
    pub current_owner_valid: bool,
    pub blocking_owners: Vec<Blocker>,
    pub unresolved_effects: Vec<UnresolvedEffect>,
    pub released_effects: Vec<ReleasedEffect>,
    pub retired_artifact_obligations: Vec<RetiredArtifact>,
    pub reconciled_control_obligations: Vec<ReconciledControl>,
    pub limitations: &'static str,
}
#[derive(Debug, Serialize)]
pub struct Blocker {
    pub kind: &'static str,
    pub id: String,
    pub reason: &'static str,
}
#[derive(Debug, Serialize)]
pub struct UnresolvedEffect {
    pub campaign_id: String,
    pub task_key: String,
    pub dispatch_id: String,
    pub reason: &'static str,
}
#[derive(Debug, Serialize)]
pub struct ReleasedEffect {
    pub campaign_id: String,
    pub task_key: String,
    pub audit_sha256: String,
    pub latest_dispatch_id: String,
    pub current_result_sha256: String,
    pub current_contract_digest: String,
    pub terminal: bool,
}
#[derive(Debug, Serialize)]
pub struct RetiredArtifact {
    pub campaign_id: String,
    pub task_key: String,
    pub dispatch_id: String,
    pub missing_terminal_timestamp: bool,
    pub limitation: &'static str,
}

#[derive(Debug, Serialize)]
pub struct ReconciledControl {
    pub campaign_id: String,
    pub task_key: String,
    pub dispatch_id: String,
    pub role: String,
    pub status: String,
    pub external_effect_requires_reconciliation: bool,
    pub missing_terminal_timestamp: bool,
    pub limitation: &'static str,
}

/// Five seconds includes pool admission, every SQL read, decoding and evaluation.
/// Cancellation rolls back the transaction; no persistent state is written.
pub async fn inspect(pool: &PgPool, campaign: &str, query: &Query) -> Result<Envelope> {
    let owners = query.validate(campaign)?;
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut tx = pool.begin().await?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *tx)
            .await?;
        sqlx::query("SET LOCAL statement_timeout='2s'")
            .execute(&mut *tx)
            .await?;
        let snapshot = rows::load(&mut tx, &owners, &query.task_key).await?;
        let projection = project(campaign, query, owners, &snapshot)?;
        tx.rollback().await?;
        Ok(Envelope {
            effect_ownership: projection,
        })
    })
    .await
    .map_err(|_| anyhow::anyhow!("native ownership observation deadline exceeded"))?
}

fn terminal(status: Option<&str>) -> bool {
    matches!(status, Some("completed" | "failed" | "cancelled"))
}
fn ended_entry(status: Option<&str>) -> bool {
    matches!(
        status,
        Some("done" | "failed" | "user_cancelled" | "skipped")
    )
}
fn captured(raw: &str) -> Result<Value> {
    let value: Value = serde_json::from_str(raw)?;
    if let Some(encoded) = value.as_str() {
        Ok(serde_json::from_str(encoded)?)
    } else {
        Ok(value)
    }
}
fn release(
    snapshot: &rows::Snapshot,
    task: &rows::Task,
    dispatch: &rows::Dispatch,
) -> Option<ReleasedEffect> {
    if task.accepted
        || !dispatch.latest
        || !dispatch.chronology_known
        || !dispatch.ended
        || dispatch.status.as_deref() != Some("completed")
        || dispatch.oversized
    {
        return None;
    }
    let raw = dispatch.raw.as_deref()?;
    let receipt = captured(raw).ok()?;
    let goal: Goal = serde_json::from_value(
        snapshot
            .campaigns
            .iter()
            .find(|c| c.id == task.campaign_id)?
            .spec
            .clone(),
    )
    .ok()?;
    for row in &snapshot.audits {
        if row.oversized
            || row.campaign_id != task.campaign_id
            || row.task_key != task.task_key
            || row.dispatch_id != dispatch.id
        {
            continue;
        }
        let Some(admitted) = row
            .admitted_goal
            .as_ref()
            .and_then(|value| serde_json::from_value::<Goal>(value.clone()).ok())
        else {
            continue;
        };
        let Ok(admitted_scope) = super::learning::scope_digest(&admitted) else {
            continue;
        };
        if super::learning::scope_digest(&goal).ok().as_deref() != Some(admitted_scope.as_str()) {
            continue;
        }
        let audit_sha256 = if let Some(value) = &row.settled {
            if row.authority.as_deref() != Some(replan::settled_effect::AUTHORITY) {
                continue;
            }
            let Ok(audit) = serde_json::from_value::<replan::SettledExternalEffect>(value.clone())
            else {
                continue;
            };
            if replan::settled_effect::validate(
                &audit,
                replan::settled_effect::ValidationContext {
                    campaign: &task.campaign_id,
                    task: &task.task_key,
                    contract: &task.contract_digest,
                    goal: &goal,
                    dispatch: &dispatch.id,
                    original: &row.original,
                    raw,
                    captured: &receipt,
                    fresh_admission: false,
                },
            )
            .is_err()
            {
                continue;
            }
            audit.audit_sha256
        } else if let Some(value) = &row.unattempted {
            let Ok(audit) = serde_json::from_value::<replan::UnattemptedEffect>(value.clone())
            else {
                continue;
            };
            if row
                .original
                .pointer("/runtime/contract_digest")
                .and_then(Value::as_str)
                != Some(task.contract_digest.as_str())
                || row.original["task_key"] != task.task_key
                || row.original["dispatch_id"] != dispatch.id
                || replan::validate_unattempted_effect(
                    &audit,
                    &dispatch.id,
                    &row.original,
                    raw,
                    &receipt,
                )
                .is_err()
            {
                continue;
            }
            audit.audit_sha256
        } else {
            continue;
        };
        return Some(ReleasedEffect {
            campaign_id: task.campaign_id.clone(),
            task_key: task.task_key.clone(),
            audit_sha256,
            latest_dispatch_id: dispatch.id.clone(),
            current_result_sha256: digest(raw.as_bytes()),
            current_contract_digest: task.contract_digest.clone(),
            terminal: true,
        });
    }
    None
}

fn project(
    campaign: &str,
    q: &Query,
    owners: Vec<String>,
    s: &rows::Snapshot,
) -> Result<Projection> {
    let mut p = Projection {
        schema: "campaign-effect-ownership/v1",
        coverage: COVERAGE,
        queue_coverage: [
            "auto_queue_runs",
            "auto_queue_entries",
            "dispatch_queue",
            "dispatch_outbox",
        ],
        campaign_ids: owners,
        active_version: q.active_version,
        current_task_key: q.task_key.clone(),
        current_contract_digest: q.contract_digest.clone(),
        current_dispatch_id: q.dispatch_id.clone(),
        current_card_id: q.card_id.clone(),
        current_agent_id: q.agent_id.clone(),
        observed_at: chrono::Utc::now().to_rfc3339(),
        truncated: s.truncated,
        allowed: false,
        current_owner_valid: false,
        blocking_owners: vec![],
        unresolved_effects: vec![],
        released_effects: vec![],
        retired_artifact_obligations: vec![],
        reconciled_control_obligations: vec![],
        limitations: "Read-only native DB snapshot of the explicitly requested campaigns with one exact selected owner excluded. Not caller execution authentication, an execution lease, OS exit evidence or atomic external-effect admission.",
    };
    let campaign_row = s
        .campaigns
        .iter()
        .find(|c| c.id == campaign)
        .ok_or_else(|| anyhow::anyhow!("selected campaign unavailable"))?;
    let task = s
        .tasks
        .iter()
        .find(|t| t.campaign_id == campaign && t.task_key == q.task_key);
    let dispatch = s.dispatches.iter().find(|d| d.id == q.dispatch_id);
    let node = task
        .and_then(|t| t.node.clone())
        .and_then(|node| serde_json::from_value::<Task>(node).ok());
    p.current_owner_valid = campaign_row.status.as_deref() == Some("active")
        && campaign_row.hold_reason.is_none()
        && campaign_row.active_version == Some(q.active_version)
        && task.is_some_and(|t| {
            !t.accepted
                && t.role == "work"
                && t.version == Some(q.active_version)
                && t.active_graph
                && t.card_id == q.card_id
                && t.contract_digest == q.contract_digest
                && t.assigned_agent_id.as_deref() == Some(q.agent_id.as_str())
                && matches!(t.provider.as_deref(), Some("codex" | "qwen" | "opencode"))
                && t.latest_dispatch_id.as_deref() == Some(q.dispatch_id.as_str())
        })
        && node.is_some_and(|n| {
            n.task_key == q.task_key
                && n.agent == q.agent_id
                && contract_digest(&n).ok().as_deref() == Some(q.contract_digest.as_str())
        })
        && dispatch.is_some_and(|d| {
            d.latest
                && d.chronology_known
                && !d.ended
                && d.card_id == q.card_id
                && d.agent_id.as_deref() == Some(q.agent_id.as_str())
                && d.status.as_deref() == Some("dispatched")
        });
    if !p.current_owner_valid {
        block(
            &mut p,
            "current_owner",
            &q.dispatch_id,
            "canonical current task/card/contract/agent/latest dispatch mismatch",
        );
    }
    if s.truncated {
        block(
            &mut p,
            "coverage",
            "bounded_snapshot",
            "native observation exceeded its finite row or byte budget",
        );
    }
    for d in &s.dispatches {
        let retired = d.historical_artifact_retired
            && d.latest
            && d.chronology_known
            && terminal(d.status.as_deref());
        let control_reconciled = control_history::reconciled(d.control_reconciliation.as_ref())
            && d.latest
            && d.chronology_known
            && terminal(d.status.as_deref());
        if !(p.current_owner_valid && d.id == q.dispatch_id)
            && (!terminal(d.status.as_deref())
                || (!d.ended && !retired && !control_reconciled)
                || !d.chronology_known)
        {
            block(
                &mut p,
                "dispatch",
                &d.id,
                "other dispatch has live, unknown, NULL or incomplete terminal state",
            );
        }
        let Some(t) = s.tasks.iter().find(|t| t.card_id == d.card_id) else {
            continue;
        };
        if retired && !d.ended {
            p.retired_artifact_obligations.push(RetiredArtifact{campaign_id:t.campaign_id.clone(),task_key:t.task_key.clone(),dispatch_id:d.id.clone(),missing_terminal_timestamp:true,
                limitation:"Exact native reconciliation of observed artifact-only nonexternal work; missing historical terminal timestamp and OS exit remain unknown. Live native owner and cleanup checks still apply."});
        }
        if control_reconciled && !d.ended {
            p.reconciled_control_obligations.push(ReconciledControl {
                campaign_id: t.campaign_id.clone(),
                task_key: t.task_key.clone(),
                dispatch_id: d.id.clone(),
                role: t.role.clone(),
                status: d.status.clone().unwrap_or_default(),
                external_effect_requires_reconciliation: false,
                missing_terminal_timestamp: true,
                limitation: "Exact audited native reconciliation of implicit nonexternal control history. Original terminal status is retained; missing historical terminal timestamp and OS exit remain unknown. No success, effect authority or replay is granted. Live native owner and cleanup checks still apply.",
            });
        }
        if ((t.role == "work" && t.effect != "artifact_only" && !t.accepted)
            || t.requires_reconciliation)
            && !(p.current_owner_valid && d.id == q.dispatch_id)
        {
            if let Some(audit) = release(s, t, d) {
                p.released_effects.push(audit);
            } else {
                p.unresolved_effects.push(UnresolvedEffect {campaign_id:t.campaign_id.clone(),task_key:t.task_key.clone(),dispatch_id:d.id.clone(),
                    reason:"external attempt has no current exact authenticated settlement or validated non-attempt"});
            }
        } else if !t.observed && !(p.current_owner_valid && d.id == q.dispatch_id) {
            block(
                &mut p,
                "observation",
                &t.task_key,
                "other published attempt has no canonical observation",
            );
        }
    }
    let own_entries: Vec<_> = s
        .entries
        .iter()
        .filter(|e| e.dispatch_id.as_deref() == Some(q.dispatch_id.as_str()))
        .collect();
    let own_entry = if p.current_owner_valid && own_entries.len() == 1 {
        let e = own_entries[0];
        let r = e
            .run_id
            .as_deref()
            .and_then(|id| s.runs.iter().find(|r| r.id == id));
        (e.card_id.as_deref() == Some(q.card_id.as_str())
            && e.dispatch_id.as_deref() == Some(q.dispatch_id.as_str())
            && e.agent_id.as_deref() == Some(q.agent_id.as_str())
            && e.status.as_deref() == Some("dispatched")
            && e.slot_index.is_some()
            && e.thread_group.is_some()
            && r.is_some_and(|r| {
                r.status.as_deref() == Some("active")
                    && r.agent_id.as_deref() == Some(q.agent_id.as_str())
                    && s.entries
                        .iter()
                        .filter(|other| other.run_id.as_deref() == Some(r.id.as_str()))
                        .count()
                        == 1
            })
            && s.slots
                .iter()
                .filter(|slot| {
                    slot.agent_id == q.agent_id
                        && slot.assigned_run_id == e.run_id
                        && Some(slot.slot_index) == e.slot_index
                        && slot.assigned_thread_group == e.thread_group
                })
                .count()
                == 1)
            .then_some(e)
    } else {
        None
    };
    for e in &s.entries {
        if own_entry.is_some_and(|own| own.id == e.id) {
            continue;
        }
        if !ended_entry(e.status.as_deref())
            || !s
                .runs
                .iter()
                .any(|r| e.run_id.as_deref() == Some(r.id.as_str()))
        {
            block(
                &mut p,
                "entry",
                &e.id,
                "other queue entry is live, unknown, NULL, orphaned or shares the current run",
            );
        }
    }
    for r in &s.runs {
        if own_entry.is_some_and(|e| e.run_id.as_deref() == Some(r.id.as_str())) {
            continue;
        }
        if !terminal(r.status.as_deref()) {
            block(
                &mut p,
                "run",
                &r.id,
                "other run is live, restoring, unknown or NULL",
            );
        }
    }
    for slot in &s.slots {
        if slot.assigned_run_id.is_none() {
            continue;
        }
        let own = own_entry.is_some_and(|e| {
            slot.agent_id == q.agent_id
                && slot.assigned_run_id == e.run_id
                && Some(slot.slot_index) == e.slot_index
                && slot.assigned_thread_group == e.thread_group
        });
        if !own {
            block(
                &mut p,
                "slot",
                &format!("{}:{}", slot.agent_id, slot.slot_index),
                "other assigned slot remains owned or current slot identity differs",
            );
        }
    }
    let live_sessions: Vec<_> = s
        .sessions
        .iter()
        .filter(|session| !matches!(session.status.as_deref(), Some("idle" | "disconnected")))
        .collect();
    for session in &live_sessions {
        let own = p.current_owner_valid
            && live_sessions.len() == 1
            && session.active_dispatch_id.as_deref() == Some(q.dispatch_id.as_str())
            && session.agent_id.as_deref() == Some(q.agent_id.as_str())
            && task.is_some_and(|t| t.provider.is_some() && t.provider == session.provider)
            && !session.workspace_conflict
            && session.card_ids == vec![q.card_id.clone()]
            && matches!(
                session.status.as_deref(),
                Some("turn_active" | "awaiting_bg")
            );
        if !own {
            block(
                &mut p,
                "session",
                &session.id.to_string(),
                "other or ambiguous live cwd/thread/dispatch session, unknown or NULL state",
            );
        }
    }
    for id in &s.cleanup {
        block(
            &mut p,
            "cleanup",
            &id.to_string(),
            "native cleanup is owed or malformed rollback coverage cannot be attributed",
        );
    }
    for id in &s.legacy_queue {
        block(
            &mut p,
            "legacy_queue",
            &id.to_string(),
            "legacy queue row has no exact current dispatch ownership identity",
        );
    }
    for id in &s.delivery_debt {
        block(
            &mut p,
            "delivery",
            &id.to_string(),
            "dispatch delivery or follow-up remains pending, failed or unknown",
        );
    }
    for id in &s.unsupported_stops {
        block(
            &mut p,
            "owner_stop",
            id,
            "owner-stop evidence has no trusted settlement producer in this frozen runtime",
        );
    }
    p.allowed = p.current_owner_valid
        && !p.truncated
        && p.blocking_owners.is_empty()
        && p.unresolved_effects.is_empty();
    Ok(p)
}
fn block(p: &mut Projection, kind: &'static str, id: &str, reason: &'static str) {
    p.blocking_owners.push(Blocker {
        kind,
        id: id.into(),
        reason,
    });
}
