//! A native graph task's accepted receipt/artifacts are its completion contract.
//! This is an ephemeral card-specific rule, never a persisted repo override or force.
use crate::pipeline::{PipelineConfig, TransitionConfig, TransitionType};
use anyhow::Result;
use sqlx::{Postgres, Transaction};

pub(super) async fn add_completion_rule(
    tx: &mut Transaction<'_, Postgres>,
    pipeline: &mut PipelineConfig,
    card: &str,
    from: &str,
    to: &str,
    latest_dispatch: Option<&str>,
) -> Result<()> {
    // Respect configured direct transitions and custom states. A graph task may
    // only settle its normal execution state; cancelled/terminal cards cannot reopen.
    if from != "in_progress"
        || to != "done"
        || !pipeline.is_terminal(to)
        || pipeline.find_transition(from, to).is_some()
    {
        return Ok(());
    }
    let accepted: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM campaign_graph_tasks t
         JOIN task_dispatches d ON d.kanban_card_id=t.card_id
         WHERE t.card_id=$1 AND t.accepted AND t.observation->>'outcome'='accepted'
           AND t.observation->>'dispatch_id'=d.id AND d.id=$2
           AND d.status='completed' AND d.dispatch_type IN ('implementation','rework')
           AND (t.role<>'work' OR t.artifact_manifest->>'bundle' IS NOT NULL)
           AND NOT EXISTS(SELECT 1 FROM task_dispatches live WHERE live.kanban_card_id=$1
                          AND live.status IN ('pending','dispatched','running')))",
    )
    .bind(card)
    .bind(latest_dispatch)
    .fetch_one(&mut **tx)
    .await?;
    if accepted {
        pipeline.transitions.push(TransitionConfig {
            from: from.into(),
            to: to.into(),
            transition_type: TransitionType::Free,
            gates: vec![],
        });
    }
    Ok(())
}
