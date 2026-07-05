import re

with open('src/services/auto_queue/route_generate.rs', 'r') as f:
    content = f.read()

new_content = content.replace('''    let mut active_dispatch_skips: Vec<serde_json::Value> = Vec::new();
    {
        let mut retained = Vec::with_capacity(cards.len());
        for card in cards.into_iter() {
            match active_dispatch_id_for_card_pg(pool, &card.card_id).await {
                Ok(Some(existing_dispatch_id)) => {
                    if let Some(issue_number) = card.github_issue_number {
                        active_dispatch_skips.push(json!({
                            "issue_number": issue_number,
                            "existing_dispatch_id": existing_dispatch_id,
                        }));
                    }
                    crate::auto_queue_log!(
                        info,
                        "generate_skip_active_dispatch_pg_1444",
                        AutoQueueLogContext::new()
                            .card(card.card_id.as_str())
                            .agent(card.agent_id.as_str())
                            .dispatch(&existing_dispatch_id),
                        "⏭ GENERATE: card {} already has active dispatch {}, skipping",
                        card.card_id,
                        existing_dispatch_id
                    );
                }
                Ok(None) => retained.push(card),
                Err(error) => {
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(json!({
                            "error": format!(
                                "active-dispatch lookup failed for card {}: {error}",
                                card.card_id
                            ),
                        })),
                    );
                }
            }
        }
        cards = retained;
    }''', '''    let mut active_dispatch_skips: Vec<serde_json::Value> = Vec::new();
    {
        let card_ids: Vec<String> = cards.iter().map(|c| c.card_id.clone()).collect();
        let active_dispatches = match active_dispatch_ids_for_cards_pg(pool, &card_ids).await {
            Ok(map) => map,
            Err(error) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({
                        "error": format!(
                            "active-dispatch batch lookup failed: {error}"
                        ),
                    })),
                );
            }
        };

        let mut retained = Vec::with_capacity(cards.len());
        for card in cards.into_iter() {
            match active_dispatches.get(&card.card_id) {
                Some(existing_dispatch_id) => {
                    if let Some(issue_number) = card.github_issue_number {
                        active_dispatch_skips.push(json!({
                            "issue_number": issue_number,
                            "existing_dispatch_id": existing_dispatch_id,
                        }));
                    }
                    crate::auto_queue_log!(
                        info,
                        "generate_skip_active_dispatch_pg_1444",
                        AutoQueueLogContext::new()
                            .card(card.card_id.as_str())
                            .agent(card.agent_id.as_str())
                            .dispatch(existing_dispatch_id),
                        "⏭ GENERATE: card {} already has active dispatch {}, skipping",
                        card.card_id,
                        existing_dispatch_id
                    );
                }
                None => retained.push(card),
            }
        }
        cards = retained;
    }''')

new_content = new_content.replace('''pub(crate) async fn active_dispatch_id_for_card_pg(
    pool: &sqlx::PgPool,
    card_id: &str,
) -> Result<Option<String>, sqlx::Error> {
    sqlx::query_scalar::<_, String>(
        "SELECT id
         FROM task_dispatches
         WHERE kanban_card_id = $1
           AND status IN ('pending', 'dispatched')
         ORDER BY created_at DESC
         LIMIT 1",
    )
    .bind(card_id)
    .fetch_optional(pool)
    .await
}''', '''pub(crate) async fn active_dispatch_ids_for_cards_pg(
    pool: &sqlx::PgPool,
    card_ids: &[String],
) -> Result<std::collections::HashMap<String, String>, sqlx::Error> {
    let rows = sqlx::query_as::<_, (String, String)>(
        "SELECT DISTINCT ON (kanban_card_id) kanban_card_id, id
         FROM task_dispatches
         WHERE kanban_card_id = ANY($1)
           AND status IN ('pending', 'dispatched')
         ORDER BY kanban_card_id, created_at DESC"
    )
    .bind(card_ids)
    .fetch_all(pool)
    .await?;

    let mut map = std::collections::HashMap::with_capacity(rows.len());
    for (card_id, dispatch_id) in rows {
        map.insert(card_id, dispatch_id);
    }
    Ok(map)
}''')

with open('src/services/auto_queue/route_generate.rs', 'w') as f:
    f.write(new_content)
