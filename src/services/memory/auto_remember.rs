use std::collections::HashSet;
use std::time::Duration;

use regex::Regex;
use reqwest::Url;
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::db::session_transcripts::{SessionTranscriptEvent, SessionTranscriptEventKind};
use crate::services::discord::settings::{MemoryBackendKind, ResolvedMemorySettings};

use super::auto_remember_store::{
    AutoRememberAuditEntry, AutoRememberMemoryStatus, AutoRememberStage, AutoRememberStore,
};
use super::{
    MementoBackend, MementoRememberRequest, TokenUsage, backend_is_active,
    resolve_memento_workspace,
};

const AUTO_REMEMBER_SOURCE: &str = "agentdesk:auto-remember";
const AUTO_REMEMBER_AGENT_ID: &str = "default";
const AUTO_REMEMBER_LOCAL_REWRITE_SCHEMA_VERSION: &str = "1";
const AUTO_REMEMBER_LOCAL_REWRITE_TIMEOUT_MS: u64 = 8_000;
const AUTO_REMEMBER_LOCAL_REWRITE_BASE_URL_ENV: &str = "AGENTDESK_AUTO_REMEMBER_LOCAL_BASE_URL";
const AUTO_REMEMBER_LOCAL_REWRITE_MODEL_ENV: &str = "AGENTDESK_AUTO_REMEMBER_LOCAL_MODEL";
const DEFAULT_AUTO_REMEMBER_LOCAL_REWRITE_BASE_URL: &str =
    "http://127.0.0.1:1234/v1/chat/completions";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum AutoRememberSignalKind {
    ConfirmedErrorRootCause,
    TechnicalDecision,
    ConfigChange,
}

impl AutoRememberSignalKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::ConfirmedErrorRootCause => "confirmed_error_root_cause",
            Self::TechnicalDecision => "technical_decision",
            Self::ConfigChange => "config_change",
        }
    }

    fn topic(self) -> &'static str {
        match self {
            Self::ConfirmedErrorRootCause => "error-root-cause",
            Self::TechnicalDecision => "technical-decision",
            Self::ConfigChange => "config-change",
        }
    }

    fn kind(self) -> &'static str {
        match self {
            Self::ConfirmedErrorRootCause => "error",
            Self::TechnicalDecision => "decision",
            Self::ConfigChange => "fact",
        }
    }

    fn importance(self) -> f64 {
        match self {
            Self::ConfirmedErrorRootCause => 0.75,
            Self::TechnicalDecision => 0.70,
            Self::ConfigChange => 0.65,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct NormalizedToolEvidence {
    summary: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CandidateUnit {
    text: String,
    supporting_evidence: Vec<String>,
    normalized_tool_evidence: Vec<NormalizedToolEvidence>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct AutoRememberRawCandidate {
    signal_kind: AutoRememberSignalKind,
    raw_content: String,
    supporting_evidence: Vec<String>,
    entity_key: Option<String>,
    workspace: String,
    candidate_hash: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct AutoRememberValidatedCandidate {
    signal_kind: AutoRememberSignalKind,
    content: String,
    keywords: Vec<String>,
    entity_key: Option<String>,
    workspace: String,
    candidate_hash: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AutoRememberDecision {
    StoreDirectly,
    RewriteNeeded,
    Skip(AutoRememberSkipReason),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AutoRememberSkipReason {
    Uncertain,
    AmbiguousSignal,
    LowPrecision,
    MissingEntityKey,
    MissingWorkspace,
    ValidatorRejected,
    RewriteUnavailable,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct AutoRememberRewriteOutput {
    content: String,
    keyword_suggestions: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct LocalRewriteRuntimeConfig {
    base_url: String,
    model: String,
    timeout: Duration,
}

#[derive(Debug, Deserialize)]
struct LocalRewriteChatResponse {
    choices: Vec<LocalRewriteChoice>,
}

#[derive(Debug, Deserialize)]
struct LocalRewriteChoice {
    message: LocalRewriteMessage,
}

#[derive(Debug, Deserialize)]
struct LocalRewriteMessage {
    content: String,
}

#[derive(Debug, Deserialize)]
struct LocalRewritePayload {
    schema_version: String,
    content: String,
    #[serde(default)]
    keywords: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AutoRememberTurnRequest {
    pub(crate) turn_id: String,
    pub(crate) role_id: String,
    pub(crate) channel_id: u64,
    pub(crate) user_text: String,
    pub(crate) assistant_text: String,
    pub(crate) transcript_events: Vec<SessionTranscriptEvent>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct AutoRememberExecutionResult {
    pub(crate) token_usage: TokenUsage,
    pub(crate) remembered_count: usize,
    pub(crate) duplicate_count: usize,
    pub(crate) warnings: Vec<String>,
}

pub(crate) async fn run_auto_remember(
    settings: &ResolvedMemorySettings,
    request: AutoRememberTurnRequest,
) -> AutoRememberExecutionResult {
    if !settings.auto_remember_enabled || settings.backend != MemoryBackendKind::Memento {
        return AutoRememberExecutionResult::default();
    }
    if !backend_is_active(MemoryBackendKind::Memento) {
        return AutoRememberExecutionResult {
            warnings: vec!["memento backend inactive; skipping auto-remember".to_string()],
            ..AutoRememberExecutionResult::default()
        };
    }

    let candidates = extract_candidates(&request);
    if candidates.is_empty() {
        return AutoRememberExecutionResult::default();
    }

    let store = match AutoRememberStore::open() {
        Ok(store) => store,
        Err(error) => {
            return AutoRememberExecutionResult {
                warnings: vec![error],
                ..AutoRememberExecutionResult::default()
            };
        }
    };
    let backend = MementoBackend::new(settings.clone());

    let mut seen_hashes = HashSet::new();
    let mut result = AutoRememberExecutionResult::default();

    for raw_candidate in candidates {
        if !seen_hashes.insert(raw_candidate.candidate_hash.clone()) {
            result.duplicate_count += 1;
            let _ = store.upsert_audit(AutoRememberAuditEntry {
                turn_id: &request.turn_id,
                candidate_hash: &raw_candidate.candidate_hash,
                signal_kind: raw_candidate.signal_kind.as_str(),
                workspace: &raw_candidate.workspace,
                stage: AutoRememberStage::Dedupe,
                status: AutoRememberMemoryStatus::DuplicateSkip,
                retry_count: 0,
                error: Some("duplicate candidate in same turn"),
            });
            continue;
        }

        match store.lookup_record(&raw_candidate.workspace, &raw_candidate.candidate_hash) {
            Ok(Some(record)) if record.status.suppresses_repeat() => {
                result.duplicate_count += 1;
                let _ = store.upsert_audit(AutoRememberAuditEntry {
                    turn_id: &request.turn_id,
                    candidate_hash: &raw_candidate.candidate_hash,
                    signal_kind: raw_candidate.signal_kind.as_str(),
                    workspace: &raw_candidate.workspace,
                    stage: AutoRememberStage::Dedupe,
                    status: AutoRememberMemoryStatus::DuplicateSkip,
                    retry_count: record.retry_count,
                    error: Some("candidate already processed"),
                });
                continue;
            }
            Ok(_) => {}
            Err(error) => {
                result.warnings.push(error);
                continue;
            }
        }

        let decision = pre_validate_candidate(&raw_candidate);
        let candidate = match decision {
            AutoRememberDecision::StoreDirectly => validate_candidate(
                raw_candidate.signal_kind,
                &raw_candidate.raw_content,
                raw_candidate.entity_key.as_deref(),
            )
            .map(|content| build_validated_candidate(&raw_candidate, content, &[])),
            AutoRememberDecision::RewriteNeeded => match rewrite_candidate(&raw_candidate).await {
                Ok(Some(rewritten)) => validate_candidate(
                    raw_candidate.signal_kind,
                    &rewritten.content,
                    raw_candidate.entity_key.as_deref(),
                )
                .map(|content| {
                    build_validated_candidate(
                        &raw_candidate,
                        content,
                        &rewritten.keyword_suggestions,
                    )
                }),
                Ok(None) => None,
                Err(error) => {
                    result.warnings.push(error);
                    None
                }
            },
            AutoRememberDecision::Skip(_) => None,
        };

        let Some(candidate) = candidate else {
            let _ = store.upsert_audit(AutoRememberAuditEntry {
                turn_id: &request.turn_id,
                candidate_hash: &raw_candidate.candidate_hash,
                signal_kind: raw_candidate.signal_kind.as_str(),
                workspace: &raw_candidate.workspace,
                stage: AutoRememberStage::Validate,
                status: AutoRememberMemoryStatus::ValidationSkipped,
                retry_count: 0,
                error: Some(skip_reason_text(decision)),
            });
            continue;
        };

        let supersedes = if candidate.signal_kind == AutoRememberSignalKind::ConfigChange {
            if let Some(entity_key) = candidate.entity_key.as_deref() {
                match backend
                    .lookup_fragment_ids(
                        &candidate.workspace,
                        candidate.signal_kind.topic(),
                        &[format!("config-key:{entity_key}")],
                    )
                    .await
                {
                    Ok((fragment_ids, usage)) => {
                        result.token_usage.saturating_add_assign(usage);
                        fragment_ids
                    }
                    Err(error) => {
                        result.warnings.push(error);
                        Vec::new()
                    }
                }
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        };

        match backend
            .remember(memento_request_from_candidate(&candidate, supersedes))
            .await
        {
            Ok(token_usage) => {
                result.token_usage.saturating_add_assign(token_usage);
                result.remembered_count += 1;
                if let Err(error) = store.upsert_audit(AutoRememberAuditEntry {
                    turn_id: &request.turn_id,
                    candidate_hash: &candidate.candidate_hash,
                    signal_kind: candidate.signal_kind.as_str(),
                    workspace: &candidate.workspace,
                    stage: AutoRememberStage::Remember,
                    status: AutoRememberMemoryStatus::Remembered,
                    retry_count: 0,
                    error: None,
                }) {
                    result.warnings.push(error);
                }
            }
            Err(error) => {
                result.warnings.push(error.clone());
                match store.next_failure_status(&candidate.workspace, &candidate.candidate_hash) {
                    Ok((status, retry_count)) => {
                        if let Err(store_error) = store.upsert_audit(AutoRememberAuditEntry {
                            turn_id: &request.turn_id,
                            candidate_hash: &candidate.candidate_hash,
                            signal_kind: candidate.signal_kind.as_str(),
                            workspace: &candidate.workspace,
                            stage: AutoRememberStage::Remember,
                            status,
                            retry_count,
                            error: Some(&error),
                        }) {
                            result.warnings.push(store_error);
                        }
                    }
                    Err(store_error) => result.warnings.push(store_error),
                }
            }
        }
    }

    result
}

fn extract_candidates(request: &AutoRememberTurnRequest) -> Vec<AutoRememberRawCandidate> {
    let workspace_override = std::env::var("MEMENTO_WORKSPACE").ok();
    let workspace = resolve_memento_workspace(
        &request.role_id,
        request.channel_id,
        workspace_override.as_deref(),
    );
    if workspace.trim().is_empty() {
        return Vec::new();
    }

    collect_candidate_units(request)
        .into_iter()
        .filter_map(|unit| candidate_from_unit(&workspace, unit))
        .collect()
}

fn collect_candidate_units(request: &AutoRememberTurnRequest) -> Vec<CandidateUnit> {
    let mut units = Vec::new();
    let mut seen = HashSet::new();

    push_text_units(
        &mut units,
        &mut seen,
        &request.assistant_text,
        SessionTranscriptEventKind::Assistant,
    );
    for event in &request.transcript_events {
        match event.kind {
            SessionTranscriptEventKind::Assistant
            | SessionTranscriptEventKind::ToolResult
            | SessionTranscriptEventKind::Result
            | SessionTranscriptEventKind::Error => {
                if let Some(summary) = event.summary.as_deref() {
                    push_text_units(&mut units, &mut seen, summary, event.kind);
                }
                push_text_units(&mut units, &mut seen, &event.content, event.kind);
            }
            _ => {}
        }
    }

    units
}

fn push_text_units(
    units: &mut Vec<CandidateUnit>,
    seen: &mut HashSet<String>,
    text: &str,
    event_kind: SessionTranscriptEventKind,
) {
    for raw in text.lines() {
        let normalized = normalize_whitespace(raw);
        if normalized.is_empty() {
            continue;
        }
        if !seen.insert(normalized.clone()) {
            continue;
        }
        let mut normalized_tool_evidence = Vec::new();
        if matches!(
            event_kind,
            SessionTranscriptEventKind::ToolResult
                | SessionTranscriptEventKind::Result
                | SessionTranscriptEventKind::Error
        ) && looks_like_tool_evidence(&normalized)
        {
            normalized_tool_evidence.push(NormalizedToolEvidence {
                summary: normalized.clone(),
            });
        }
        units.push(CandidateUnit {
            text: normalized.clone(),
            supporting_evidence: vec![normalized],
            normalized_tool_evidence,
        });
    }
}

fn candidate_from_unit(workspace: &str, unit: CandidateUnit) -> Option<AutoRememberRawCandidate> {
    if contains_uncertainty(&unit.text) {
        return None;
    }

    let mut matches = Vec::new();
    if is_confirmed_error_root_cause(&unit.text) {
        matches.push(AutoRememberSignalKind::ConfirmedErrorRootCause);
    }
    if is_technical_decision(&unit.text) {
        matches.push(AutoRememberSignalKind::TechnicalDecision);
    }
    if is_config_change(&unit.text) {
        matches.push(AutoRememberSignalKind::ConfigChange);
    }
    if matches.len() != 1 {
        return None;
    }

    let signal_kind = matches[0];
    if !passes_precision_gate(signal_kind, &unit.text) {
        return None;
    }

    let mut supporting_evidence = unit.supporting_evidence;
    supporting_evidence.extend(
        unit.normalized_tool_evidence
            .into_iter()
            .map(|evidence| evidence.summary),
    );

    let entity_key = if signal_kind == AutoRememberSignalKind::ConfigChange {
        normalized_config_key(&unit.text)
    } else {
        None
    };
    let candidate_hash = hash_candidate(workspace, signal_kind, &unit.text);

    Some(AutoRememberRawCandidate {
        signal_kind,
        raw_content: unit.text,
        supporting_evidence,
        entity_key,
        workspace: workspace.to_string(),
        candidate_hash,
    })
}

fn pre_validate_candidate(candidate: &AutoRememberRawCandidate) -> AutoRememberDecision {
    if candidate.workspace.trim().is_empty() {
        return AutoRememberDecision::Skip(AutoRememberSkipReason::MissingWorkspace);
    }
    if contains_uncertainty(&candidate.raw_content) {
        return AutoRememberDecision::Skip(AutoRememberSkipReason::Uncertain);
    }

    match candidate.signal_kind {
        AutoRememberSignalKind::ConfigChange => {
            if candidate.entity_key.is_none() {
                return AutoRememberDecision::Skip(AutoRememberSkipReason::MissingEntityKey);
            }
            if is_self_contained_config_change(
                &candidate.raw_content,
                candidate.entity_key.as_deref(),
            ) {
                AutoRememberDecision::StoreDirectly
            } else if can_rewrite_config_change(candidate) {
                AutoRememberDecision::RewriteNeeded
            } else {
                AutoRememberDecision::Skip(AutoRememberSkipReason::RewriteUnavailable)
            }
        }
        AutoRememberSignalKind::ConfirmedErrorRootCause => {
            if is_self_contained(&candidate.raw_content)
                && is_atomic_sentence(&candidate.raw_content)
            {
                AutoRememberDecision::StoreDirectly
            } else if can_rewrite_root_cause(candidate) {
                AutoRememberDecision::RewriteNeeded
            } else {
                AutoRememberDecision::Skip(AutoRememberSkipReason::ValidatorRejected)
            }
        }
        AutoRememberSignalKind::TechnicalDecision => {
            if is_self_contained(&candidate.raw_content)
                && is_atomic_sentence(&candidate.raw_content)
            {
                AutoRememberDecision::StoreDirectly
            } else if can_rewrite_decision(candidate) {
                AutoRememberDecision::RewriteNeeded
            } else {
                AutoRememberDecision::Skip(AutoRememberSkipReason::ValidatorRejected)
            }
        }
    }
}

fn build_validated_candidate(
    raw: &AutoRememberRawCandidate,
    content: String,
    keyword_suggestions: &[String],
) -> AutoRememberValidatedCandidate {
    AutoRememberValidatedCandidate {
        signal_kind: raw.signal_kind,
        keywords: build_keywords(
            raw.signal_kind,
            raw.entity_key.as_deref(),
            &content,
            keyword_suggestions,
        ),
        entity_key: raw.entity_key.clone(),
        workspace: raw.workspace.clone(),
        candidate_hash: raw.candidate_hash.clone(),
        content,
    }
}

fn local_rewrite_runtime_config() -> Result<LocalRewriteRuntimeConfig, String> {
    let base_url = std::env::var(AUTO_REMEMBER_LOCAL_REWRITE_BASE_URL_ENV)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_AUTO_REMEMBER_LOCAL_REWRITE_BASE_URL.to_string());
    let parsed_url = Url::parse(&base_url)
        .map_err(|error| format!("auto-remember local rewrite base URL is invalid: {error}"))?;
    if !is_loopback_url(&parsed_url) {
        return Err("auto-remember local rewrite base URL must stay on loopback".to_string());
    }

    let model = std::env::var(AUTO_REMEMBER_LOCAL_REWRITE_MODEL_ENV)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| std::env::var("LOCAL_LLM_MODEL").ok())
        .or_else(|| std::env::var("LMSTUDIO_MODEL").ok())
        .map(|value| normalize_whitespace(&value))
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            format!(
                "auto-remember local rewrite model is not configured; set {}",
                AUTO_REMEMBER_LOCAL_REWRITE_MODEL_ENV
            )
        })?;

    Ok(LocalRewriteRuntimeConfig {
        base_url,
        model,
        timeout: Duration::from_millis(AUTO_REMEMBER_LOCAL_REWRITE_TIMEOUT_MS),
    })
}

fn is_loopback_url(url: &Url) -> bool {
    matches!(
        url.host_str(),
        Some("127.0.0.1") | Some("localhost") | Some("::1")
    )
}

fn build_local_rewrite_prompt(candidate: &AutoRememberRawCandidate) -> String {
    let mut rules = vec![
        "- Use only the provided evidence.".to_string(),
        "- Return exactly one self-contained sentence in English.".to_string(),
        "- Do not use pronouns or deictic phrases like this, that, it, they, 해당, 이번."
            .to_string(),
        "- Do not add uncertainty or speculate beyond the evidence.".to_string(),
        "- Keep the sentence atomic and concrete.".to_string(),
        format!(
            "- Return JSON only with keys schema_version, content, keywords. schema_version must be {}.",
            AUTO_REMEMBER_LOCAL_REWRITE_SCHEMA_VERSION
        ),
    ];
    if let Some(entity_key) = candidate.entity_key.as_deref() {
        rules.push(format!(
            "- The sentence must include the exact config key `{entity_key}`."
        ));
    }

    let evidence = candidate
        .supporting_evidence
        .iter()
        .map(|line| format!("- {line}"))
        .collect::<Vec<_>>()
        .join("\n");

    format!(
        "Signal kind: {}\nRaw candidate: {}\nRules:\n{}\nEvidence:\n{}\nJSON:",
        candidate.signal_kind.as_str(),
        candidate.raw_content,
        rules.join("\n"),
        evidence,
    )
}

fn parse_local_rewrite_output(raw: &str) -> Result<AutoRememberRewriteOutput, String> {
    let payload = parse_local_rewrite_payload(raw)?;
    if payload.schema_version != AUTO_REMEMBER_LOCAL_REWRITE_SCHEMA_VERSION {
        return Err(format!(
            "auto-remember local rewrite schema mismatch: expected {}, got {}",
            AUTO_REMEMBER_LOCAL_REWRITE_SCHEMA_VERSION, payload.schema_version
        ));
    }

    let content = normalize_whitespace(&payload.content);
    if content.is_empty() {
        return Err("auto-remember local rewrite returned empty content".to_string());
    }

    Ok(AutoRememberRewriteOutput {
        content,
        keyword_suggestions: payload
            .keywords
            .into_iter()
            .map(|keyword| normalize_keyword_suggestion(&keyword))
            .filter(|keyword| !keyword.is_empty())
            .collect(),
    })
}

fn parse_local_rewrite_payload(raw: &str) -> Result<LocalRewritePayload, String> {
    let trimmed = raw.trim();
    if let Ok(payload) = serde_json::from_str::<LocalRewritePayload>(trimmed) {
        return Ok(payload);
    }

    let stripped = trimmed
        .strip_prefix("```json")
        .or_else(|| trimmed.strip_prefix("```"))
        .map(str::trim)
        .and_then(|value| value.strip_suffix("```"))
        .map(str::trim)
        .unwrap_or(trimmed);
    if let Ok(payload) = serde_json::from_str::<LocalRewritePayload>(stripped) {
        return Ok(payload);
    }

    let start = stripped
        .find('{')
        .ok_or_else(|| "auto-remember local rewrite did not return JSON".to_string())?;
    let end = stripped
        .rfind('}')
        .ok_or_else(|| "auto-remember local rewrite did not return JSON".to_string())?;
    serde_json::from_str::<LocalRewritePayload>(&stripped[start..=end]).map_err(|error| {
        format!("auto-remember local rewrite JSON parse failed: {error}; body={stripped}")
    })
}

async fn rewrite_candidate(
    candidate: &AutoRememberRawCandidate,
) -> Result<Option<AutoRememberRewriteOutput>, String> {
    let config = local_rewrite_runtime_config()?;
    let prompt = build_local_rewrite_prompt(candidate);
    let client = reqwest::Client::builder()
        .timeout(config.timeout)
        .build()
        .map_err(|error| format!("auto-remember local rewrite client build failed: {error}"))?;

    let response = client
        .post(&config.base_url)
        .json(&json!({
            "model": config.model,
            "temperature": 0,
            "response_format": { "type": "json_object" },
            "messages": [
                {
                    "role": "system",
                    "content": "You only rewrite a candidate memory sentence into one self-contained sentence. Use only the provided evidence. Return JSON only."
                },
                {
                    "role": "user",
                    "content": prompt,
                }
            ]
        }))
        .send()
        .await
        .map_err(|error| format!("auto-remember local rewrite request failed: {error}"))?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!(
            "auto-remember local rewrite failed with {status}: {body}"
        ));
    }

    let payload: LocalRewriteChatResponse = response
        .json()
        .await
        .map_err(|error| format!("auto-remember local rewrite response decode failed: {error}"))?;
    let raw_content = payload
        .choices
        .into_iter()
        .next()
        .map(|choice| choice.message.content)
        .ok_or_else(|| "auto-remember local rewrite returned no choices".to_string())?;
    let rewrite = parse_local_rewrite_output(&raw_content)?;
    Ok(Some(rewrite))
}

fn normalize_keyword_suggestion(value: &str) -> String {
    let token_re = Regex::new(r"[A-Za-z0-9_.:/-]{3,}").unwrap();
    token_re
        .find(&normalize_whitespace(value))
        .map(|matched| matched.as_str().to_ascii_lowercase())
        .unwrap_or_default()
}

fn validate_candidate(
    signal_kind: AutoRememberSignalKind,
    content: &str,
    entity_key: Option<&str>,
) -> Option<String> {
    let content = normalize_whitespace(content);
    if content.is_empty()
        || contains_uncertainty(&content)
        || contains_deixis(&content)
        || !is_atomic_sentence(&content)
        || !passes_precision_gate(signal_kind, &content)
    {
        return None;
    }
    if signal_kind == AutoRememberSignalKind::ConfigChange {
        let key = entity_key?;
        if !content
            .to_ascii_lowercase()
            .contains(&key.to_ascii_lowercase())
        {
            return None;
        }
    }
    Some(ensure_period(&content))
}

fn build_keywords(
    signal_kind: AutoRememberSignalKind,
    entity_key: Option<&str>,
    content: &str,
    keyword_suggestions: &[String],
) -> Vec<String> {
    let mut keywords = Vec::new();
    let mut seen = HashSet::new();

    push_keyword(&mut keywords, &mut seen, signal_kind.as_str().to_string());
    if let Some(entity_key) = entity_key {
        push_keyword(&mut keywords, &mut seen, format!("config-key:{entity_key}"));
        push_keyword(&mut keywords, &mut seen, entity_key.to_string());
    }

    let token_re = Regex::new(r"[A-Za-z0-9_.:/-]{4,}").unwrap();
    for matched in token_re.find_iter(content) {
        push_keyword(
            &mut keywords,
            &mut seen,
            matched.as_str().to_ascii_lowercase(),
        );
        if keywords.len() >= 6 {
            break;
        }
    }

    for suggestion in keyword_suggestions {
        push_keyword(
            &mut keywords,
            &mut seen,
            normalize_keyword_suggestion(suggestion),
        );
        if keywords.len() >= 8 {
            break;
        }
    }

    keywords
}

fn push_keyword(keywords: &mut Vec<String>, seen: &mut HashSet<String>, value: String) {
    let value = normalize_whitespace(&value);
    if value.is_empty() {
        return;
    }
    if seen.insert(value.clone()) {
        keywords.push(value);
    }
}

fn memento_request_from_candidate(
    candidate: &AutoRememberValidatedCandidate,
    supersedes: Vec<String>,
) -> MementoRememberRequest {
    MementoRememberRequest {
        content: candidate.content.clone(),
        topic: candidate.signal_kind.topic().to_string(),
        kind: candidate.signal_kind.kind().to_string(),
        keywords: candidate.keywords.clone(),
        importance: Some(candidate.signal_kind.importance()),
        source: Some(AUTO_REMEMBER_SOURCE.to_string()),
        workspace: Some(candidate.workspace.clone()),
        agent_id: Some(AUTO_REMEMBER_AGENT_ID.to_string()),
        assertion_status: Some("inferred".to_string()),
        supersedes,
        ..MementoRememberRequest::default()
    }
}

fn skip_reason_text(decision: AutoRememberDecision) -> &'static str {
    match decision {
        AutoRememberDecision::StoreDirectly => "store_directly",
        AutoRememberDecision::RewriteNeeded => "rewrite_needed_but_invalid",
        AutoRememberDecision::Skip(reason) => match reason {
            AutoRememberSkipReason::Uncertain => "uncertain_candidate",
            AutoRememberSkipReason::AmbiguousSignal => "ambiguous_signal",
            AutoRememberSkipReason::LowPrecision => "low_precision_candidate",
            AutoRememberSkipReason::MissingEntityKey => "missing_entity_key",
            AutoRememberSkipReason::MissingWorkspace => "missing_workspace",
            AutoRememberSkipReason::ValidatorRejected => "validator_rejected",
            AutoRememberSkipReason::RewriteUnavailable => "rewrite_unavailable",
        },
    }
}

fn contains_uncertainty(unit: &str) -> bool {
    let lower = unit.to_ascii_lowercase();
    [
        "추정",
        "추측",
        "아마도",
        "가능성이",
        "가능성",
        "일 수",
        "일수",
        "might",
        "maybe",
        "probably",
        "likely",
        "guess",
        "hypothesis",
        "브레인스토밍",
        "가정",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
}

fn contains_deixis(unit: &str) -> bool {
    let lower = unit.to_ascii_lowercase();
    let english = Regex::new(r"(?i)\b(this|that|these|those|it|they|them|he|she)\b").unwrap();
    english.is_match(unit)
        || [
            "그것",
            "이것",
            "저것",
            "이 에러",
            "그 에러",
            "이 구조",
            "그 구조",
            "저 구조",
            "이 설정",
            "그 설정",
            "이 변경",
            "그 변경",
            "이 값",
            "그 값",
            "이 문제",
            "그 문제",
            "이번",
            "위에서",
            "앞서",
            "해당",
        ]
        .iter()
        .any(|needle| lower.contains(needle))
}

fn is_confirmed_error_root_cause(unit: &str) -> bool {
    let lower = unit.to_ascii_lowercase();
    [
        "원인은",
        "실패 원인은",
        "because",
        "due to",
        "caused by",
        "root cause",
        "때문에 발생",
        "로 인해 발생",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
}

fn is_technical_decision(unit: &str) -> bool {
    let lower = unit.to_ascii_lowercase();
    let has_decision_verb = [
        "결정",
        "채택",
        "사용하기로",
        "고정",
        "표준화",
        "하기로 한다",
        "decided to",
        "we use",
        "we will use",
        "adopt",
        "standardize",
        "policy",
    ]
    .iter()
    .any(|needle| lower.contains(needle));
    let has_technical_target = [
        "backend",
        "sqlite",
        "memento",
        "mem0",
        "agentdesk",
        "구조",
        "아키텍처",
        "패턴",
        "전략",
        "contract",
        "schema",
        "coordinator",
    ]
    .iter()
    .any(|needle| lower.contains(needle));

    has_decision_verb && has_technical_target
}

fn is_config_change(unit: &str) -> bool {
    let lower = unit.to_ascii_lowercase();
    let has_change_verb = [
        "변경",
        "바꿨",
        "수정",
        "set ",
        "set to",
        "updated",
        "changed",
        "configured",
        "enabled",
        "disabled",
        "switched",
    ]
    .iter()
    .any(|needle| lower.contains(needle));
    let has_transition =
        parse_config_change_from_to(unit).is_some() || unit.contains("->") || unit.contains("=>");
    let has_assignment_change = parse_config_assignment_value(unit).is_some() && has_change_verb;

    normalized_config_key(unit).is_some() && (has_transition || has_assignment_change)
}

fn passes_precision_gate(signal_kind: AutoRememberSignalKind, unit: &str) -> bool {
    match signal_kind {
        AutoRememberSignalKind::ConfirmedErrorRootCause => {
            is_confirmed_error_root_cause(unit) && !contains_deixis(unit)
        }
        AutoRememberSignalKind::TechnicalDecision => is_technical_decision(unit),
        AutoRememberSignalKind::ConfigChange => {
            normalized_config_key(unit).is_some()
                && (parse_config_change_from_to(unit).is_some()
                    || unit.contains("->")
                    || unit.contains("=>")
                    || (parse_config_assignment_value(unit).is_some()
                        && [
                            "변경",
                            "바꿨",
                            "수정",
                            "set ",
                            "set to",
                            "updated",
                            "changed",
                            "configured",
                            "enabled",
                            "disabled",
                            "switched",
                        ]
                        .iter()
                        .any(|needle| unit.to_ascii_lowercase().contains(needle))))
        }
    }
}

fn is_self_contained(unit: &str) -> bool {
    !contains_deixis(unit) && !contains_uncertainty(unit)
}

fn is_atomic_sentence(unit: &str) -> bool {
    !(unit.contains('\n')
        || unit.contains(';')
        || unit.contains(" 그리고 ")
        || unit.contains(" and ")
        || unit.contains(" / "))
}

fn is_self_contained_config_change(unit: &str, entity_key: Option<&str>) -> bool {
    let Some(entity_key) = entity_key else {
        return false;
    };
    is_self_contained(unit)
        && is_atomic_sentence(unit)
        && unit
            .to_ascii_lowercase()
            .contains(&entity_key.to_ascii_lowercase())
}

fn can_rewrite_config_change(candidate: &AutoRememberRawCandidate) -> bool {
    candidate.entity_key.is_some()
        && (parse_config_change_from_to(&candidate.raw_content).is_some()
            || parse_config_assignment_value(&candidate.raw_content).is_some())
}

fn can_rewrite_root_cause(candidate: &AutoRememberRawCandidate) -> bool {
    passes_precision_gate(candidate.signal_kind, &candidate.raw_content)
        && !contains_deixis(&candidate.raw_content)
}

fn can_rewrite_decision(candidate: &AutoRememberRawCandidate) -> bool {
    passes_precision_gate(candidate.signal_kind, &candidate.raw_content)
        && !contains_deixis(&candidate.raw_content)
}

fn normalized_config_key(unit: &str) -> Option<String> {
    let explicit_key_re =
        Regex::new(r"(?i)([A-Za-z][A-Za-z0-9_.:-]*[._:-][A-Za-z0-9_.:-]+)").unwrap();
    explicit_key_re
        .captures(unit)
        .and_then(|captures| captures.get(1).map(|m| m.as_str().to_ascii_lowercase()))
}

fn parse_config_change_from_to(unit: &str) -> Option<(String, String)> {
    let english_re = Regex::new(
        r"(?i)([A-Za-z][A-Za-z0-9_.:-]*[._:-][A-Za-z0-9_.:-]+).*?\bfrom\b\s+([^\s,.;]+)\s+\bto\b\s+([^\s,.;]+)",
    )
    .unwrap();
    if let Some(captures) = english_re.captures(unit) {
        return Some((
            captures.get(2)?.as_str().to_string(),
            captures.get(3)?.as_str().to_string(),
        ));
    }

    let korean_re = Regex::new(
        r"(?i)([A-Za-z][A-Za-z0-9_.:-]*[._:-][A-Za-z0-9_.:-]+)(?:를|을|은|는|이|가)?\s+([^\s,.;]+)에서\s+([^\s,.;]+)로",
    )
    .unwrap();
    korean_re.captures(unit).and_then(|captures| {
        Some((
            captures.get(2)?.as_str().to_string(),
            captures.get(3)?.as_str().to_string(),
        ))
    })
}

fn parse_config_assignment_value(unit: &str) -> Option<String> {
    let assignment_re =
        Regex::new(r"(?i)([A-Za-z][A-Za-z0-9_.:-]*[._:-][A-Za-z0-9_.:-]+)\s*=\s*([^\s,.;]+)")
            .unwrap();
    assignment_re
        .captures(unit)
        .and_then(|captures| captures.get(2).map(|m| m.as_str().to_string()))
}

fn looks_like_tool_evidence(unit: &str) -> bool {
    unit.len() <= 200
        && (unit.contains("->")
            || unit.contains("=>")
            || unit.contains(".rs:")
            || unit.contains(".md:")
            || unit.contains('=')
            || unit.contains(" changed ")
            || unit.contains(" updated "))
}

fn clean_value(value: &str) -> String {
    value
        .trim()
        .trim_matches(|ch: char| matches!(ch, '.' | ',' | ';' | ':' | ')' | '(' | '"' | '\''))
        .to_string()
}

fn hash_candidate(workspace: &str, signal_kind: AutoRememberSignalKind, content: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(workspace.as_bytes());
    hasher.update(b"\n");
    hasher.update(signal_kind.as_str().as_bytes());
    hasher.update(b"\n");
    hasher.update(content.as_bytes());
    hex::encode(hasher.finalize())
}

fn ensure_period(value: &str) -> String {
    let value = value.trim();
    if value.ends_with('.') {
        value.to_string()
    } else {
        format!("{value}.")
    }
}

fn normalize_whitespace(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tempfile::TempDir;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;
    use crate::services::discord::runtime_store::lock_test_env;
    use crate::services::discord::settings::ResolvedMemorySettings;

    fn remember_request_from_candidate(
        signal_kind: AutoRememberSignalKind,
        content: &str,
        entity_key: Option<&str>,
    ) -> MementoRememberRequest {
        let candidate = AutoRememberValidatedCandidate {
            signal_kind,
            content: content.to_string(),
            keywords: build_keywords(signal_kind, entity_key, content, &[]),
            entity_key: entity_key.map(ToOwned::to_owned),
            workspace: "agentdesk-default".to_string(),
            candidate_hash: "hash".to_string(),
        };
        memento_request_from_candidate(&candidate, vec!["frag-1".to_string()])
    }

    async fn spawn_local_rewrite_server(
        response_body: &str,
    ) -> (
        String,
        tokio::sync::oneshot::Receiver<Vec<String>>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let body = response_body.to_string();
        let (requests_tx, requests_rx) = tokio::sync::oneshot::channel();
        let handle = tokio::spawn(async move {
            let Ok((mut stream, _)) = listener.accept().await else {
                let _ = requests_tx.send(Vec::new());
                return;
            };
            let mut buf = [0u8; 32768];
            let n = stream.read(&mut buf).await.unwrap_or(0);
            let request = String::from_utf8_lossy(&buf[..n]).to_string();
            let raw_response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = stream.write_all(raw_response.as_bytes()).await;
            let _ = stream.shutdown().await;
            let _ = requests_tx.send(vec![request]);
        });
        (
            format!("http://{addr}/v1/chat/completions"),
            requests_rx,
            handle,
        )
    }

    #[test]
    fn extracts_confirmed_error_root_cause_from_assistant_line() {
        let request = AutoRememberTurnRequest {
            turn_id: "turn-1".to_string(),
            role_id: "project-agentdesk".to_string(),
            channel_id: 42,
            user_text: "왜 실패했어?".to_string(),
            assistant_text: "실패 원인은 MCP 세션 ID 누락 때문이다.".to_string(),
            transcript_events: Vec::new(),
        };

        let candidates = extract_candidates(&request);

        assert_eq!(candidates.len(), 1);
        assert_eq!(
            candidates[0].signal_kind,
            AutoRememberSignalKind::ConfirmedErrorRootCause
        );
    }

    #[test]
    fn skips_uncertain_lines() {
        let request = AutoRememberTurnRequest {
            turn_id: "turn-1".to_string(),
            role_id: "project-agentdesk".to_string(),
            channel_id: 42,
            user_text: String::new(),
            assistant_text: "원인은 아마도 MCP 세션 ID 누락일 수 있다.".to_string(),
            transcript_events: Vec::new(),
        };

        assert!(extract_candidates(&request).is_empty());
    }

    #[test]
    fn config_change_requires_explicit_key_and_transition() {
        let ambiguous = AutoRememberTurnRequest {
            turn_id: "turn-1".to_string(),
            role_id: "project-agentdesk".to_string(),
            channel_id: 42,
            user_text: String::new(),
            assistant_text: "메모리 설정을 변경했다.".to_string(),
            transcript_events: Vec::new(),
        };
        let explicit = AutoRememberTurnRequest {
            turn_id: "turn-2".to_string(),
            role_id: "project-agentdesk".to_string(),
            channel_id: 42,
            user_text: String::new(),
            assistant_text: "memory.backend를 file에서 memento로 변경했다.".to_string(),
            transcript_events: Vec::new(),
        };

        assert!(extract_candidates(&ambiguous).is_empty());
        let candidates = extract_candidates(&explicit);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].entity_key.as_deref(), Some("memory.backend"));
        assert_eq!(
            candidates[0].signal_kind,
            AutoRememberSignalKind::ConfigChange
        );
    }

    #[tokio::test]
    async fn rewrite_config_change_uses_local_llm_json_contract() {
        let _guard = lock_test_env();
        let response = serde_json::json!({
            "choices": [
                {
                    "message": {
                        "content": serde_json::json!({
                            "schema_version": AUTO_REMEMBER_LOCAL_REWRITE_SCHEMA_VERSION,
                            "content": "Config key memory.backend changed from file to memento.",
                            "keywords": ["memory.backend", "memento"]
                        }).to_string()
                    }
                }
            ]
        })
        .to_string();
        let (base_url, requests_rx, handle) = spawn_local_rewrite_server(&response).await;
        unsafe {
            std::env::set_var(AUTO_REMEMBER_LOCAL_REWRITE_BASE_URL_ENV, &base_url);
            std::env::set_var(AUTO_REMEMBER_LOCAL_REWRITE_MODEL_ENV, "qwen3-local");
        }

        let candidate = AutoRememberRawCandidate {
            signal_kind: AutoRememberSignalKind::ConfigChange,
            raw_content: "memory.backend를 file에서 memento로 변경했다".to_string(),
            supporting_evidence: vec!["memory.backend를 file에서 memento로 변경했다".to_string()],
            entity_key: Some("memory.backend".to_string()),
            workspace: "agentdesk-default".to_string(),
            candidate_hash: "hash".to_string(),
        };

        let rewritten = rewrite_candidate(&candidate)
            .await
            .expect("rewrite should succeed")
            .expect("rewrite output should exist");
        assert_eq!(
            rewritten.content,
            "Config key memory.backend changed from file to memento."
        );
        assert!(
            validate_candidate(
                AutoRememberSignalKind::ConfigChange,
                &rewritten.content,
                Some("memory.backend")
            )
            .is_some()
        );
        assert!(
            rewritten
                .keyword_suggestions
                .iter()
                .any(|keyword| keyword == "memory.backend")
        );

        let requests = tokio::time::timeout(Duration::from_secs(1), requests_rx)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(requests.len(), 1);
        assert!(requests[0].contains("\"model\":\"qwen3-local\""));
        assert!(requests[0].contains("\"response_format\":{\"type\":\"json_object\"}"));
        assert!(requests[0].contains("memory.backend"));

        handle.await.unwrap();
        unsafe {
            std::env::remove_var(AUTO_REMEMBER_LOCAL_REWRITE_BASE_URL_ENV);
            std::env::remove_var(AUTO_REMEMBER_LOCAL_REWRITE_MODEL_ENV);
        }
    }

    #[test]
    fn local_rewrite_requires_loopback_base_url() {
        let _guard = lock_test_env();
        unsafe {
            std::env::set_var(
                AUTO_REMEMBER_LOCAL_REWRITE_BASE_URL_ENV,
                "https://example.com/v1/chat/completions",
            );
            std::env::set_var(AUTO_REMEMBER_LOCAL_REWRITE_MODEL_ENV, "qwen3-local");
        }
        let error = local_rewrite_runtime_config().unwrap_err();
        assert!(error.contains("loopback"));
        unsafe {
            std::env::remove_var(AUTO_REMEMBER_LOCAL_REWRITE_BASE_URL_ENV);
            std::env::remove_var(AUTO_REMEMBER_LOCAL_REWRITE_MODEL_ENV);
        }
    }

    #[test]
    fn candidate_mapping_uses_exact_contract() {
        let root_cause = remember_request_from_candidate(
            AutoRememberSignalKind::ConfirmedErrorRootCause,
            "MCP session ID missing caused the failure.",
            None,
        );
        assert_eq!(root_cause.topic, "error-root-cause");
        assert_eq!(root_cause.kind, "error");
        assert_eq!(root_cause.importance, Some(0.75));
        assert_eq!(root_cause.assertion_status.as_deref(), Some("inferred"));

        let decision = remember_request_from_candidate(
            AutoRememberSignalKind::TechnicalDecision,
            "SQLite sidecar is the standard audit store.",
            None,
        );
        assert_eq!(decision.topic, "technical-decision");
        assert_eq!(decision.kind, "decision");
        assert_eq!(decision.importance, Some(0.70));

        let change = remember_request_from_candidate(
            AutoRememberSignalKind::ConfigChange,
            "Config key memory.backend changed from file to memento.",
            Some("memory.backend"),
        );
        assert_eq!(change.topic, "config-change");
        assert_eq!(change.kind, "fact");
        assert_eq!(change.importance, Some(0.65));
        assert_eq!(change.source.as_deref(), Some(AUTO_REMEMBER_SOURCE));
        assert_eq!(change.agent_id.as_deref(), Some(AUTO_REMEMBER_AGENT_ID));
        assert!(
            change
                .keywords
                .iter()
                .any(|keyword| keyword == "config-key:memory.backend")
        );
        assert_eq!(change.supersedes, vec!["frag-1".to_string()]);
    }

    #[test]
    fn validation_rejects_deictic_sentence() {
        assert!(
            validate_candidate(
                AutoRememberSignalKind::TechnicalDecision,
                "이 구조로 가기로 결정했다.",
                None
            )
            .is_none()
        );
    }

    #[tokio::test]
    async fn disabled_auto_remember_does_not_create_sidecar_file() {
        let _guard = lock_test_env();
        let temp = TempDir::new().unwrap();
        let previous = std::env::var_os("AGENTDESK_ROOT_DIR");
        unsafe { std::env::set_var("AGENTDESK_ROOT_DIR", temp.path()) };

        let settings = ResolvedMemorySettings::default();
        let request = AutoRememberTurnRequest {
            turn_id: "turn-1".to_string(),
            role_id: "project-agentdesk".to_string(),
            channel_id: 42,
            user_text: String::new(),
            assistant_text: "실패 원인은 MCP 세션 ID 누락 때문이다.".to_string(),
            transcript_events: Vec::new(),
        };

        let result = run_auto_remember(&settings, request).await;
        assert_eq!(result, AutoRememberExecutionResult::default());
        assert!(
            !temp
                .path()
                .join("data")
                .join("memory-auto-remember.sqlite")
                .exists()
        );

        match previous {
            Some(value) => unsafe { std::env::set_var("AGENTDESK_ROOT_DIR", value) },
            None => unsafe { std::env::remove_var("AGENTDESK_ROOT_DIR") },
        }
    }

    #[tokio::test]
    async fn non_memento_backend_does_not_create_sidecar_file() {
        let _guard = lock_test_env();
        let temp = TempDir::new().unwrap();
        let previous = std::env::var_os("AGENTDESK_ROOT_DIR");
        unsafe { std::env::set_var("AGENTDESK_ROOT_DIR", temp.path()) };

        let settings = ResolvedMemorySettings {
            backend: MemoryBackendKind::Mem0,
            auto_remember_enabled: true,
            ..ResolvedMemorySettings::default()
        };
        let request = AutoRememberTurnRequest {
            turn_id: "turn-1".to_string(),
            role_id: "project-agentdesk".to_string(),
            channel_id: 42,
            user_text: String::new(),
            assistant_text: "실패 원인은 MCP 세션 ID 누락 때문이다.".to_string(),
            transcript_events: Vec::new(),
        };

        let result = run_auto_remember(&settings, request).await;
        assert_eq!(result, AutoRememberExecutionResult::default());
        assert!(
            !temp
                .path()
                .join("data")
                .join("memory-auto-remember.sqlite")
                .exists()
        );

        match previous {
            Some(value) => unsafe { std::env::set_var("AGENTDESK_ROOT_DIR", value) },
            None => unsafe { std::env::remove_var("AGENTDESK_ROOT_DIR") },
        }
    }

    #[test]
    fn retry_limit_constant_matches_spec() {
        assert_eq!(
            crate::services::memory::auto_remember_store::AUTO_REMEMBER_MAX_RETRIES,
            3
        );
    }
}
