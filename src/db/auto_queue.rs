use rusqlite::{Connection, types::ToSql};

#[derive(Debug, Clone, Default)]
pub struct GenerateCardFilter {
    pub repo: Option<String>,
    pub agent_id: Option<String>,
    pub issue_numbers: Option<Vec<i64>>,
}

#[derive(Debug, Clone)]
pub struct BacklogCardRecord {
    pub card_id: String,
    pub repo_id: Option<String>,
    pub assigned_agent_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct GenerateCandidateRecord {
    pub card_id: String,
    pub agent_id: String,
    pub priority: String,
    pub description: Option<String>,
    pub metadata: Option<String>,
    pub github_issue_number: Option<i64>,
}

pub fn list_backlog_cards(
    conn: &Connection,
    filter: &GenerateCardFilter,
) -> rusqlite::Result<Vec<BacklogCardRecord>> {
    let mut conditions = Vec::new();
    let mut params: Vec<Box<dyn ToSql>> = Vec::new();
    append_card_filters("kc", filter, &mut conditions, &mut params);
    conditions.push("kc.status = 'backlog'".to_string());

    let sql = format!(
        "SELECT kc.id, kc.repo_id, kc.assigned_agent_id
         FROM kanban_cards kc
         WHERE {}",
        conditions.join(" AND ")
    );
    let param_refs: Vec<&dyn ToSql> = params.iter().map(|value| value.as_ref()).collect();
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(param_refs.as_slice(), |row| {
        Ok(BacklogCardRecord {
            card_id: row.get(0)?,
            repo_id: row.get(1)?,
            assigned_agent_id: row.get(2)?,
        })
    })?;
    rows.collect()
}

pub fn list_generate_candidates(
    conn: &Connection,
    filter: &GenerateCardFilter,
    enqueueable_states: &[String],
) -> rusqlite::Result<Vec<GenerateCandidateRecord>> {
    let mut conditions = Vec::new();
    let mut params: Vec<Box<dyn ToSql>> = Vec::new();

    let state_start = params.len() + 1;
    let state_placeholders = enqueueable_states
        .iter()
        .enumerate()
        .map(|(idx, _)| format!("?{}", state_start + idx))
        .collect::<Vec<_>>()
        .join(",");
    for state in enqueueable_states {
        params.push(Box::new(state.clone()));
    }
    conditions.push(format!("kc.status IN ({state_placeholders})"));
    append_card_filters("kc", filter, &mut conditions, &mut params);

    let sql = format!(
        "SELECT kc.id, kc.assigned_agent_id, kc.priority, kc.description, kc.metadata, kc.github_issue_number
         FROM kanban_cards kc
         WHERE {}
         ORDER BY
           CASE kc.priority
             WHEN 'urgent' THEN 0
             WHEN 'high' THEN 1
             WHEN 'medium' THEN 2
             WHEN 'low' THEN 3
             ELSE 4
           END,
           kc.created_at ASC",
        conditions.join(" AND ")
    );
    let param_refs: Vec<&dyn ToSql> = params.iter().map(|value| value.as_ref()).collect();
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(param_refs.as_slice(), |row| {
        Ok(GenerateCandidateRecord {
            card_id: row.get::<_, String>(0)?,
            agent_id: row.get::<_, Option<String>>(1)?.unwrap_or_default(),
            priority: row
                .get::<_, Option<String>>(2)?
                .unwrap_or_else(|| "medium".to_string()),
            description: row.get::<_, Option<String>>(3)?,
            metadata: row.get::<_, Option<String>>(4)?,
            github_issue_number: row.get::<_, Option<i64>>(5)?,
        })
    })?;
    rows.collect()
}

pub fn count_cards_by_status(
    conn: &Connection,
    repo: Option<&str>,
    agent_id: Option<&str>,
    status: &str,
) -> rusqlite::Result<i64> {
    let mut sql = "SELECT COUNT(*) FROM kanban_cards WHERE status = ?1".to_string();
    let mut params: Vec<Box<dyn ToSql>> = vec![Box::new(status.to_string())];

    if let Some(repo) = repo.filter(|value| !value.is_empty()) {
        params.push(Box::new(repo.to_string()));
        sql.push_str(&format!(" AND repo_id = ?{}", params.len()));
    }
    if let Some(agent_id) = agent_id.filter(|value| !value.is_empty()) {
        params.push(Box::new(agent_id.to_string()));
        sql.push_str(&format!(" AND assigned_agent_id = ?{}", params.len()));
    }

    let param_refs: Vec<&dyn ToSql> = params.iter().map(|value| value.as_ref()).collect();
    conn.query_row(&sql, param_refs.as_slice(), |row| row.get(0))
}

fn append_card_filters(
    alias: &str,
    filter: &GenerateCardFilter,
    conditions: &mut Vec<String>,
    params: &mut Vec<Box<dyn ToSql>>,
) {
    let prefix = if alias.is_empty() {
        String::new()
    } else {
        format!("{alias}.")
    };

    if let Some(repo) = filter.repo.as_ref() {
        params.push(Box::new(repo.clone()));
        conditions.push(format!("{}repo_id = ?{}", prefix, params.len()));
    }
    if let Some(agent_id) = filter.agent_id.as_ref() {
        params.push(Box::new(agent_id.clone()));
        conditions.push(format!("{}assigned_agent_id = ?{}", prefix, params.len()));
    }
    if let Some(issue_numbers) = filter
        .issue_numbers
        .as_ref()
        .filter(|nums| !nums.is_empty())
    {
        let start = params.len() + 1;
        let placeholders = issue_numbers
            .iter()
            .enumerate()
            .map(|(idx, _)| format!("?{}", start + idx))
            .collect::<Vec<_>>()
            .join(",");
        for issue_number in issue_numbers {
            params.push(Box::new(*issue_number));
        }
        conditions.push(format!("{}github_issue_number IN ({placeholders})", prefix));
    }
}
