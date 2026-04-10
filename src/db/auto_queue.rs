use rusqlite::{Connection, OptionalExtension, types::ToSql};

#[derive(Debug, Clone, Default)]
pub struct GenerateCardFilter {
    pub repo: Option<String>,
    pub agent_id: Option<String>,
    pub issue_numbers: Option<Vec<i64>>,
}

#[derive(Debug, Clone, Default)]
pub struct StatusFilter {
    pub repo: Option<String>,
    pub agent_id: Option<String>,
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

#[derive(Debug, Clone)]
pub struct AutoQueueRunRecord {
    pub id: String,
    pub repo: Option<String>,
    pub agent_id: Option<String>,
    pub status: String,
    pub timeout_minutes: i64,
    pub ai_model: Option<String>,
    pub ai_rationale: Option<String>,
    pub created_at: i64,
    pub completed_at: Option<i64>,
    pub max_concurrent_threads: i64,
    pub thread_group_count: i64,
}

#[derive(Debug, Clone)]
pub struct StatusEntryRecord {
    pub id: String,
    pub agent_id: String,
    pub card_id: String,
    pub priority_rank: i64,
    pub reason: Option<String>,
    pub status: String,
    pub created_at: i64,
    pub dispatched_at: Option<i64>,
    pub completed_at: Option<i64>,
    pub card_title: Option<String>,
    pub github_issue_number: Option<i64>,
    pub github_repo: Option<String>,
    pub thread_group: i64,
    pub slot_index: Option<i64>,
    pub batch_phase: i64,
    pub channel_thread_map: Option<String>,
    pub active_thread_id: Option<String>,
}

pub fn find_latest_run_id(
    conn: &Connection,
    filter: &StatusFilter,
) -> rusqlite::Result<Option<String>> {
    let mut run_filter = "1=1".to_string();
    let mut params: Vec<Box<dyn ToSql>> = Vec::new();

    if let Some(repo) = filter.repo.as_ref() {
        params.push(Box::new(repo.clone()));
        run_filter.push_str(&format!(
            " AND (repo = ?{} OR repo IS NULL OR repo = '')",
            params.len()
        ));
    }
    if let Some(agent_id) = filter.agent_id.as_ref() {
        params.push(Box::new(agent_id.clone()));
        run_filter.push_str(&format!(
            " AND (agent_id = ?{} OR agent_id IS NULL OR agent_id = '')",
            params.len()
        ));
    }

    let param_refs: Vec<&dyn ToSql> = params.iter().map(|value| value.as_ref()).collect();
    conn.query_row(
        &format!(
            "SELECT id FROM auto_queue_runs WHERE {run_filter} ORDER BY created_at DESC LIMIT 1"
        ),
        param_refs.as_slice(),
        |row| row.get(0),
    )
    .optional()
}

pub fn get_run(conn: &Connection, run_id: &str) -> rusqlite::Result<Option<AutoQueueRunRecord>> {
    conn.query_row(
        "SELECT id, repo, agent_id, status, timeout_minutes,
                ai_model, ai_rationale,
                CAST(strftime('%s', created_at) AS INTEGER) * 1000,
                CASE WHEN completed_at IS NOT NULL THEN CAST(strftime('%s', completed_at) AS INTEGER) * 1000 END,
                COALESCE(max_concurrent_threads, 1),
                COALESCE(thread_group_count, 1)
         FROM auto_queue_runs
         WHERE id = ?1",
        [run_id],
        |row| {
            Ok(AutoQueueRunRecord {
                id: row.get(0)?,
                repo: row.get(1)?,
                agent_id: row.get(2)?,
                status: row.get(3)?,
                timeout_minutes: row.get(4)?,
                ai_model: row.get(5)?,
                ai_rationale: row.get(6)?,
                created_at: row.get::<_, Option<i64>>(7)?.unwrap_or(0),
                completed_at: row.get(8)?,
                max_concurrent_threads: row.get(9)?,
                thread_group_count: row.get(10)?,
            })
        },
    )
    .optional()
}

pub fn list_status_entries(
    conn: &Connection,
    run_id: &str,
    filter: &StatusFilter,
) -> rusqlite::Result<Vec<StatusEntryRecord>> {
    let mut sql = String::from(
        "SELECT e.id, e.agent_id, e.kanban_card_id, e.priority_rank, e.reason, e.status,
                CAST(strftime('%s', e.created_at) AS INTEGER) * 1000,
                CASE WHEN e.dispatched_at IS NOT NULL THEN CAST(strftime('%s', e.dispatched_at) AS INTEGER) * 1000 END,
                CASE WHEN e.completed_at IS NOT NULL THEN CAST(strftime('%s', e.completed_at) AS INTEGER) * 1000 END,
                kc.title, kc.github_issue_number, kc.github_issue_url,
                COALESCE(e.thread_group, 0), e.slot_index, COALESCE(e.batch_phase, 0),
                kc.channel_thread_map, kc.active_thread_id
         FROM auto_queue_entries e
         LEFT JOIN kanban_cards kc ON e.kanban_card_id = kc.id
         WHERE e.run_id = ?1",
    );
    let mut params: Vec<Box<dyn ToSql>> = vec![Box::new(run_id.to_string())];

    if let Some(agent_id) = filter.agent_id.as_ref().filter(|value| !value.is_empty()) {
        params.push(Box::new(agent_id.clone()));
        sql.push_str(&format!(" AND e.agent_id = ?{}", params.len()));
    }
    if let Some(repo) = filter.repo.as_ref().filter(|value| !value.is_empty()) {
        params.push(Box::new(repo.clone()));
        sql.push_str(&format!(" AND kc.repo_id = ?{}", params.len()));
    }

    sql.push_str(" ORDER BY e.priority_rank ASC");

    let param_refs: Vec<&dyn ToSql> = params.iter().map(|value| value.as_ref()).collect();
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(param_refs.as_slice(), |row| {
        Ok(StatusEntryRecord {
            id: row.get(0)?,
            agent_id: row.get(1)?,
            card_id: row.get(2)?,
            priority_rank: row.get(3)?,
            reason: row.get(4)?,
            status: row.get(5)?,
            created_at: row.get::<_, Option<i64>>(6)?.unwrap_or(0),
            dispatched_at: row.get(7)?,
            completed_at: row.get(8)?,
            card_title: row.get(9)?,
            github_issue_number: row.get(10)?,
            github_repo: row.get(11)?,
            thread_group: row.get(12)?,
            slot_index: row.get(13)?,
            batch_phase: row.get(14)?,
            channel_thread_map: row.get(15)?,
            active_thread_id: row.get(16)?,
        })
    })?;
    rows.collect()
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
