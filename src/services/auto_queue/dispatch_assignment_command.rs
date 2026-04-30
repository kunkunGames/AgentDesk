use super::*;

pub(super) async fn apply_dispatch_agent_assignments_with_pg(
    pool: &sqlx::PgPool,
    cards_by_issue: &mut HashMap<i64, ResolvedDispatchCard>,
    agent_id: Option<&str>,
    auto_assign_agent: bool,
) -> Result<(), String> {
    let target_agent = agent_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);

    for issue_number in cards_by_issue.keys().copied().collect::<Vec<_>>() {
        let Some(card) = cards_by_issue.get_mut(&issue_number) else {
            continue;
        };
        let current_agent = card
            .assigned_agent_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string);

        match (target_agent.as_deref(), current_agent.as_deref()) {
            (Some(target), Some(current)) if current != target => {
                let repo_hint = card
                    .repo_id
                    .as_deref()
                    .map(|repo| format!(" in repo {repo}"))
                    .unwrap_or_default();
                return Err(format!(
                    "issue #{issue_number}{repo_hint} is assigned to {current}, not {target}"
                ));
            }
            (Some(target), None) if auto_assign_agent => {
                let updated = sqlx::query(
                    "UPDATE kanban_cards
                     SET assigned_agent_id = $1,
                         updated_at = NOW()
                     WHERE id = $2
                       AND (assigned_agent_id IS NULL OR BTRIM(assigned_agent_id) = '')",
                )
                .bind(target)
                .bind(&card.card_id)
                .execute(pool)
                .await
                .map_err(|err| format!("{err}"))?;

                if updated.rows_affected() == 0 {
                    let actual = sqlx::query_scalar::<_, Option<String>>(
                        "SELECT assigned_agent_id
                         FROM kanban_cards
                         WHERE id = $1",
                    )
                    .bind(&card.card_id)
                    .fetch_optional(pool)
                    .await
                    .map_err(|err| format!("{err}"))?
                    .flatten()
                    .map(|value| value.trim().to_string())
                    .filter(|value| !value.is_empty());

                    match actual.as_deref() {
                        Some(actual) if actual == target => {}
                        Some(actual) => {
                            let repo_hint = card
                                .repo_id
                                .as_deref()
                                .map(|repo| format!(" in repo {repo}"))
                                .unwrap_or_default();
                            return Err(format!(
                                "issue #{issue_number}{repo_hint} is assigned to {actual}, not {target}"
                            ));
                        }
                        None => {
                            return Err(format!(
                                "issue #{issue_number} has no assigned agent; provide auto_assign_agent=true or assign it first"
                            ));
                        }
                    }
                }

                card.assigned_agent_id = Some(target.to_string());
            }
            (Some(_), None) => {
                return Err(format!(
                    "issue #{issue_number} has no assigned agent; provide auto_assign_agent=true or assign it first"
                ));
            }
            (None, None) => {
                return Err(format!(
                    "issue #{issue_number} has no assigned agent; provide agent_id or assign it first"
                ));
            }
            _ => {}
        }
    }

    Ok(())
}
