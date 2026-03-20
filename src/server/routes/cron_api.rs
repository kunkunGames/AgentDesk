use axum::{Json, extract::State, http::StatusCode};
use serde_json::json;

use super::AppState;

/// GET /api/cron-jobs
/// Returns policy-based cron jobs (onTick handlers) as the cron job list.
pub async fn list_cron_jobs(
    State(state): State<AppState>,
) -> (StatusCode, Json<serde_json::Value>) {
    let policies = state.engine.list_policies();

    let jobs: Vec<serde_json::Value> = policies
        .iter()
        .filter(|p| p.hooks.iter().any(|h| h == "onTick"))
        .map(|p| {
            let description = match p.name.as_str() {
                "timeouts" => "타임아웃 감지 — requested/in_progress 스테일 카드 자동 처리",
                "auto-queue" => "자동 큐 진행 — 큐 엔트리 순차 디스패치",
                "triage-rules" => "자동 분류 — GitHub 이슈 라벨 기반 에이전트 할당",
                _ => "",
            };
            json!({
                "id": format!("policy:{}", p.name),
                "name": format!("policy/{} → onTick", p.name),
                "enabled": true,
                "schedule": {
                    "type": "interval",
                    "interval_seconds": 60,
                    "display": "every 60s",
                },
                "state": {
                    "status": "active",
                },
                "description_ko": description,
            })
        })
        .collect();

    (StatusCode::OK, Json(json!({ "jobs": jobs })))
}
