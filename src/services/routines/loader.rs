use anyhow::{Result, anyhow};
use rquickjs::{Context, Function, Runtime};
use serde::Serialize;
use serde_json::{Map, Number, Value};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::engine::loader::compute_policy_version;

#[derive(Debug)]
pub struct LoadedRoutineScript {
    pub name: String,
    pub script_ref: String,
    pub file: PathBuf,
    pub script_version: String,
    source: String,
}

impl Clone for LoadedRoutineScript {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            script_ref: self.script_ref.clone(),
            file: self.file.clone(),
            script_version: self.script_version.clone(),
            source: self.source.clone(),
        }
    }
}

pub type RoutineScriptStore = Arc<Mutex<HashMap<String, LoadedRoutineScript>>>;

pub const MAX_OBSERVATIONS_PER_TICK: usize = 100;
pub const MAX_OBSERVATION_PAYLOAD_BYTES: usize = 65536;
pub const MAX_AUTOMATION_INVENTORY_ITEMS: usize = 100;
pub const MAX_AUTOMATION_INVENTORY_PAYLOAD_BYTES: usize = 32768;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ObservationLimits {
    pub max_observations_per_tick: usize,
    pub max_observation_payload_bytes: usize,
    pub max_automation_inventory_items: usize,
    pub max_automation_inventory_payload_bytes: usize,
}

impl Default for ObservationLimits {
    fn default() -> Self {
        Self {
            max_observations_per_tick: MAX_OBSERVATIONS_PER_TICK,
            max_observation_payload_bytes: MAX_OBSERVATION_PAYLOAD_BYTES,
            max_automation_inventory_items: MAX_AUTOMATION_INVENTORY_ITEMS,
            max_automation_inventory_payload_bytes: MAX_AUTOMATION_INVENTORY_PAYLOAD_BYTES,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct RoutineTickContext {
    pub routine: RoutineTickRoutine,
    pub run: RoutineTickRun,
    pub agent: Option<RoutineTickAgent>,
    pub checkpoint: Option<Value>,
    pub now: chrono::DateTime<chrono::Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observations: Option<Vec<Value>>,
    #[serde(rename = "automationInventory", skip_serializing_if = "Option::is_none")]
    pub automation_inventory: Option<Vec<Value>>,
    pub limits: ObservationLimits,
}

#[derive(Debug, Clone, Serialize)]
pub struct RoutineTickRoutine {
    pub id: String,
    pub agent_id: Option<String>,
    pub script_ref: String,
    pub name: String,
    pub execution_strategy: String,
    pub fresh_context_guaranteed: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct RoutineTickRun {
    pub id: String,
    pub lease_expires_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RoutineTickAgent {
    pub id: String,
    pub status: String,
    pub is_idle: bool,
    pub current_task_id: Option<String>,
    pub current_thread_channel_id: Option<String>,
}

/// Isolated QuickJS loader for `agentdesk.routines.register({ name, tick })`.
///
/// This intentionally does not use the PolicyEngine store or
/// `agentdesk.registerPolicy()` namespace. Failed loads return an error before
/// mutating the store, so callers keep the last-known-good registry.
pub struct RoutineScriptLoader {
    scripts: RoutineScriptStore,
}

impl RoutineScriptLoader {
    pub fn new() -> Result<Self> {
        Ok(Self {
            scripts: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    #[cfg(test)]
    pub fn load_script(&self, root: &Path, path: &Path) -> Result<String> {
        let script = load_single_routine_script(root, path)?;
        tracing::debug!(
            routine_script = %script.script_ref,
            name = %script.name,
            file = %script.file.display(),
            version = %script.script_version,
            "loaded routine script"
        );
        let script_ref = script.script_ref.clone();
        self.scripts
            .lock()
            .map_err(|_| anyhow!("routine script store lock poisoned"))?
            .insert(script_ref.clone(), script);
        Ok(script_ref)
    }

    pub fn load_dir(&self, root: &Path) -> Result<usize> {
        if !root.exists() {
            tracing::warn!("Routines directory does not exist: {}", root.display());
            let pruned = self.prune_missing_scripts(&HashSet::new())?;
            if pruned > 0 {
                tracing::info!(count = pruned, "pruned missing routine scripts");
            }
            return Ok(0);
        }

        let mut entries = Vec::new();
        collect_routine_script_paths(root, &mut entries)?;
        entries.sort();

        let mut loaded = 0;
        let mut seen_refs = HashSet::new();
        let mut loaded_scripts = Vec::new();
        for path in entries {
            let seen_ref = script_ref(root, &path);
            seen_refs.insert(seen_ref);
            match load_single_routine_script(root, &path) {
                Ok(script) => {
                    let script_ref = script.script_ref.clone();
                    loaded += 1;
                    tracing::info!(routine_script = %script_ref, "loaded routine script");
                    loaded_scripts.push(script);
                }
                Err(e) => {
                    tracing::error!(
                        routine_script = %path.display(),
                        error = %e,
                        "failed to load routine script; keeping last-known-good registry"
                    );
                }
            }
        }

        let pruned = self.apply_dir_reload(loaded_scripts, &seen_refs)?;
        if pruned > 0 {
            tracing::info!(count = pruned, "pruned missing routine scripts");
        }

        Ok(loaded)
    }

    pub fn get_script(&self, script_ref: &str) -> Result<Option<LoadedRoutineScript>> {
        Ok(self
            .scripts
            .lock()
            .map_err(|_| anyhow!("routine script store lock poisoned"))?
            .get(script_ref)
            .cloned())
    }

    pub fn execute_tick(
        &self,
        script_ref: &str,
        tick_context: RoutineTickContext,
    ) -> Result<crate::services::routines::RoutineAction> {
        let Some(script) = self.get_script(script_ref)? else {
            return Err(anyhow!("routine script {script_ref} is not loaded"));
        };
        let action_json = evaluate_tick_action(&script, &tick_context)?;
        crate::services::routines::RoutineAction::validate(action_json)
    }

    #[cfg(test)]
    pub fn has_script(&self, script_ref: &str) -> Result<bool> {
        Ok(self
            .scripts
            .lock()
            .map_err(|_| anyhow!("routine script store lock poisoned"))?
            .contains_key(script_ref))
    }

    #[cfg(test)]
    pub fn script_refs(&self) -> Result<Vec<String>> {
        let mut refs: Vec<String> = self
            .scripts
            .lock()
            .map_err(|_| anyhow!("routine script store lock poisoned"))?
            .keys()
            .cloned()
            .collect();
        refs.sort();
        Ok(refs)
    }

    fn apply_dir_reload(
        &self,
        loaded_scripts: Vec<LoadedRoutineScript>,
        seen_refs: &HashSet<String>,
    ) -> Result<usize> {
        let mut scripts = self
            .scripts
            .lock()
            .map_err(|_| anyhow!("routine script store lock poisoned"))?;
        for script in loaded_scripts {
            scripts.insert(script.script_ref.clone(), script);
        }
        let before = scripts.len();
        scripts.retain(|script_ref, _| seen_refs.contains(script_ref));
        Ok(before.saturating_sub(scripts.len()))
    }

    fn prune_missing_scripts(&self, seen_refs: &HashSet<String>) -> Result<usize> {
        let mut scripts = self
            .scripts
            .lock()
            .map_err(|_| anyhow!("routine script store lock poisoned"))?;
        let before = scripts.len();
        scripts.retain(|script_ref, _| seen_refs.contains(script_ref));
        Ok(before.saturating_sub(scripts.len()))
    }
}

impl Drop for RoutineScriptLoader {
    fn drop(&mut self) {
        if let Ok(mut scripts) = self.scripts.lock() {
            scripts.clear();
        }
    }
}

pub fn load_single_routine_script(root: &Path, path: &Path) -> Result<LoadedRoutineScript> {
    let source = std::fs::read_to_string(path)
        .map_err(|e| anyhow!("read routine script {}: {e}", path.display()))?;
    let fallback_name = path
        .file_stem()
        .unwrap_or_default()
        .to_string_lossy()
        .to_string();
    let script_ref = script_ref(root, path);
    let script_version = compute_policy_version(&source);

    let name = evaluate_routine_script_metadata(&source, &fallback_name, &script_ref, path)?;

    Ok(LoadedRoutineScript {
        name,
        script_ref,
        file: path.to_path_buf(),
        script_version,
        source,
    })
}

fn evaluate_routine_script_metadata(
    source: &str,
    fallback_name: &str,
    script_ref: &str,
    path: &Path,
) -> Result<String> {
    let runtime =
        Runtime::new().map_err(|e| anyhow!("routine QuickJS runtime creation failed: {e}"))?;
    install_interrupt_handler(&runtime, Duration::from_secs(5));
    let context = Context::full(&runtime)
        .map_err(|e| anyhow!("routine QuickJS context creation failed: {e}"))?;

    context.with(|ctx| -> Result<String> {
        let (name, _tick) =
            capture_registered_routine(ctx.clone(), source, fallback_name, script_ref, path)?;
        Ok(name)
    })
}

fn evaluate_tick_action(
    script: &LoadedRoutineScript,
    tick_context: &RoutineTickContext,
) -> Result<Value> {
    let runtime =
        Runtime::new().map_err(|e| anyhow!("routine QuickJS runtime creation failed: {e}"))?;
    install_interrupt_handler(&runtime, Duration::from_secs(5));
    let context = Context::full(&runtime)
        .map_err(|e| anyhow!("routine QuickJS context creation failed: {e}"))?;
    let fallback_name = script
        .file
        .file_stem()
        .unwrap_or_default()
        .to_string_lossy()
        .to_string();

    context.with(|ctx| -> Result<Value> {
        let (_, tick) = capture_registered_routine(
            ctx.clone(),
            &script.source,
            &fallback_name,
            &script.script_ref,
            &script.file,
        )?;
        let context_json = serde_json::to_string(tick_context)
            .map_err(|e| anyhow!("encode routine tick context: {e}"))?;
        let context_literal = serde_json::to_string(&context_json)
            .map_err(|e| anyhow!("encode routine tick context literal: {e}"))?;
        let js_context: rquickjs::Value = ctx
            .eval(format!("JSON.parse({context_literal})"))
            .map_err(|e| anyhow!("build routine tick context: {e}"))?;
        let action_value: rquickjs::Value = tick
            .call((js_context,))
            .map_err(|e| anyhow!("routine script {} tick(ctx) failed: {e}", script.script_ref))?;
        ensure_acyclic_js_value(ctx, action_value.clone())?;
        js_value_to_json(action_value)
    })
}

fn install_interrupt_handler(runtime: &Runtime, timeout: Duration) {
    let started = Instant::now();
    runtime.set_interrupt_handler(Some(Box::new(move || started.elapsed() > timeout)));
}

fn capture_registered_routine<'js>(
    ctx: rquickjs::Ctx<'js>,
    source: &str,
    fallback_name: &str,
    script_ref: &str,
    path: &Path,
) -> Result<(String, Function<'js>)> {
    let globals = ctx.globals();
    let _: rquickjs::Value = ctx
        .eval(
            r#"
            globalThis.agentdesk = globalThis.agentdesk || {};
            agentdesk.routines = {};
            var __routineCapture = { captured: null };
            agentdesk.routines.register = function(obj) {
                __routineCapture.captured = obj;
            };
            "#,
        )
        .map_err(|e| anyhow!("failed to set up routine register capture: {e}"))?;

    let mut eval_opts = rquickjs::context::EvalOptions::default();
    eval_opts.strict = false;
    let eval_result: rquickjs::Result<rquickjs::Value> =
        ctx.eval_with_options(source.as_bytes().to_vec(), eval_opts);
    if let Err(e) = eval_result {
        return Err(anyhow!(
            "JS eval error in routine script {}: {e}",
            path.display()
        ));
    }

    let capture: rquickjs::Object = globals
        .get("__routineCapture")
        .map_err(|e| anyhow!("__routineCapture missing: {e}"))?;
    let captured: rquickjs::Value = capture
        .get("captured")
        .map_err(|e| anyhow!("get routine capture: {e}"))?;

    if captured.is_null() || captured.is_undefined() {
        return Err(anyhow!(
            "routine script {} did not call agentdesk.routines.register()",
            path.display()
        ));
    }

    let routine_obj = captured
        .into_object()
        .ok_or_else(|| anyhow!("agentdesk.routines.register argument is not an object"))?;

    let name: String = routine_obj
        .get::<_, rquickjs::Value>("name")
        .ok()
        .and_then(|v| v.as_string().and_then(|s| s.to_string().ok()))
        .unwrap_or_else(|| fallback_name.to_string());

    let tick_value: rquickjs::Value = routine_obj
        .get("tick")
        .map_err(|e| anyhow!("routine script {script_ref} missing tick(ctx): {e}"))?;
    if tick_value.is_null() || tick_value.is_undefined() {
        return Err(anyhow!("routine script {script_ref} missing tick(ctx)"));
    }
    if !tick_value.is_function() {
        return Err(anyhow!(
            "routine script {script_ref} tick must be a function"
        ));
    }
    let tick = tick_value
        .into_function()
        .ok_or_else(|| anyhow!("routine script {script_ref} tick must be a function"))?;

    Ok((name, tick))
}

fn js_value_to_json(value: rquickjs::Value<'_>) -> Result<Value> {
    if value.is_null() || value.is_undefined() {
        return Ok(Value::Null);
    }
    if let Some(value) = value.as_bool() {
        return Ok(Value::Bool(value));
    }
    if let Some(value) = value.as_int() {
        return Ok(Value::Number(Number::from(value)));
    }
    if let Some(value) = value.as_float() {
        let Some(number) = Number::from_f64(value) else {
            return Err(anyhow!("routine action contains non-finite number"));
        };
        return Ok(Value::Number(number));
    }
    if let Some(value) = value.as_string() {
        return Ok(Value::String(value.to_string().map_err(|e| {
            anyhow!("routine action string conversion failed: {e}")
        })?));
    }
    if value.is_array() {
        let array = value
            .into_array()
            .ok_or_else(|| anyhow!("routine action array conversion failed"))?;
        let mut out = Vec::with_capacity(array.len());
        for index in 0..array.len() {
            let item: rquickjs::Value = array
                .get(index)
                .map_err(|e| anyhow!("routine action array[{index}] conversion failed: {e}"))?;
            out.push(js_value_to_json(item)?);
        }
        return Ok(Value::Array(out));
    }
    if value.is_object() {
        let object = value
            .into_object()
            .ok_or_else(|| anyhow!("routine action object conversion failed"))?;
        let mut out = Map::new();
        for key in object.keys::<String>() {
            let key =
                key.map_err(|e| anyhow!("routine action object key conversion failed: {e}"))?;
            let item: rquickjs::Value = object
                .get(key.as_str())
                .map_err(|e| anyhow!("routine action field {key} conversion failed: {e}"))?;
            out.insert(key, js_value_to_json(item)?);
        }
        return Ok(Value::Object(out));
    }

    Err(anyhow!(
        "routine action returned unsupported JavaScript value"
    ))
}

fn ensure_acyclic_js_value<'js>(
    ctx: rquickjs::Ctx<'js>,
    value: rquickjs::Value<'js>,
) -> Result<()> {
    let checker: rquickjs::Function = ctx
        .eval(
            r#"
            (value) => {
              const seen = new WeakSet();
              const visit = (item) => {
                if (item === null || typeof item !== "object") {
                  return;
                }
                if (seen.has(item)) {
                  throw new Error("routine action contains cyclic object graph");
                }
                seen.add(item);
                if (Array.isArray(item)) {
                  for (const child of item) {
                    visit(child);
                  }
                } else {
                  for (const key of Object.keys(item)) {
                    visit(item[key]);
                  }
                }
                seen.delete(item);
              };
              visit(value);
            }
            "#,
        )
        .map_err(|e| anyhow!("routine action cycle checker init failed: {e}"))?;
    checker
        .call::<_, ()>((value,))
        .map_err(|e| anyhow!("routine action cycle check failed: {e}"))
}

fn script_ref(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

fn collect_routine_script_paths(root: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let path = entry.path();
        if file_type.is_dir() {
            collect_routine_script_paths(&path, out)?;
        } else if file_type.is_file() && path.extension().is_some_and(|ext| ext == "js") {
            out.push(path);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loads_registered_routine_script() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("daily-summary.js");
        std::fs::write(
            &path,
            r#"
            agentdesk.routines.register({
              name: "Daily Summary",
              tick(ctx) {
                return { action: "complete", result: { ok: true } };
              }
            });
            "#,
        )
        .unwrap();

        let loader = RoutineScriptLoader::new().unwrap();
        let script_ref = loader.load_script(dir.path(), &path).unwrap();
        assert_eq!(script_ref, "daily-summary.js");
        assert!(loader.has_script("daily-summary.js").unwrap());
        assert_eq!(loader.script_refs().unwrap(), vec!["daily-summary.js"]);
    }

    #[test]
    fn failed_load_keeps_last_known_good_registry() {
        let dir = tempfile::tempdir().unwrap();
        let good = dir.path().join("good.js");
        let bad = dir.path().join("bad.js");
        std::fs::write(
            &good,
            "agentdesk.routines.register({ name: 'Good', tick() { return { action: 'skip' }; } });",
        )
        .unwrap();
        std::fs::write(&bad, "agentdesk.routines.register({ name: 'Bad' });").unwrap();

        let loader = RoutineScriptLoader::new().unwrap();
        loader.load_script(dir.path(), &good).unwrap();
        let err = loader.load_script(dir.path(), &bad).unwrap_err();

        assert!(err.to_string().contains("missing tick"));
        assert_eq!(loader.script_refs().unwrap(), vec!["good.js"]);
    }

    #[test]
    fn isolates_global_bindings_between_scripts() {
        let dir = tempfile::tempdir().unwrap();
        let first = dir.path().join("first.js");
        let second = dir.path().join("second.js");
        let source = |name: &str| {
            format!(
                "const config = {{ name: '{name}' }}; agentdesk.routines.register({{ name: config.name, tick() {{ return {{ action: 'skip' }}; }} }});"
            )
        };
        std::fs::write(&first, source("First")).unwrap();
        std::fs::write(&second, source("Second")).unwrap();

        let loader = RoutineScriptLoader::new().unwrap();
        assert_eq!(loader.load_dir(dir.path()).unwrap(), 2);
        assert_eq!(
            loader.script_refs().unwrap(),
            vec!["first.js".to_string(), "second.js".to_string()]
        );
    }

    #[test]
    fn load_dir_recurses_into_nested_script_dirs() {
        let dir = tempfile::tempdir().unwrap();
        let nested = dir.path().join("ops").join("daily");
        std::fs::create_dir_all(&nested).unwrap();
        let path = nested.join("summary.js");
        std::fs::write(
            &path,
            "agentdesk.routines.register({ name: 'Nested', tick() { return { action: 'skip' }; } });",
        )
        .unwrap();

        let loader = RoutineScriptLoader::new().unwrap();
        assert_eq!(loader.load_dir(dir.path()).unwrap(), 1);
        assert_eq!(loader.script_refs().unwrap(), vec!["ops/daily/summary.js"]);
        assert!(loader.has_script("ops/daily/summary.js").unwrap());
    }

    #[test]
    fn load_dir_prunes_removed_scripts_and_keeps_failed_seen_script() {
        let dir = tempfile::tempdir().unwrap();
        let removed = dir.path().join("removed.js");
        let retained = dir.path().join("retained.js");
        std::fs::write(
            &removed,
            "agentdesk.routines.register({ name: 'Removed', tick() { return { action: 'skip' }; } });",
        )
        .unwrap();
        std::fs::write(
            &retained,
            "agentdesk.routines.register({ name: 'Retained', tick() { return { action: 'skip' }; } });",
        )
        .unwrap();

        let loader = RoutineScriptLoader::new().unwrap();
        assert_eq!(loader.load_dir(dir.path()).unwrap(), 2);

        std::fs::remove_file(&removed).unwrap();
        std::fs::write(
            &retained,
            "agentdesk.routines.register({ name: 'Broken' });",
        )
        .unwrap();

        assert_eq!(loader.load_dir(dir.path()).unwrap(), 0);
        assert_eq!(loader.script_refs().unwrap(), vec!["retained.js"]);
    }

    #[test]
    fn executes_tick_and_validates_action() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("complete.js");
        std::fs::write(
            &path,
            r#"
            agentdesk.routines.register({
              name: "Complete",
              tick(ctx) {
                return {
                  action: "complete",
                  result: { routineId: ctx.routine.id, runId: ctx.run.id },
                  lastResult: "ok"
                };
              }
            });
            "#,
        )
        .unwrap();

        let loader = RoutineScriptLoader::new().unwrap();
        loader.load_script(dir.path(), &path).unwrap();
        let action = loader
            .execute_tick(
                "complete.js",
                RoutineTickContext {
                    routine: RoutineTickRoutine {
                        id: "routine-1".to_string(),
                        agent_id: None,
                        script_ref: "complete.js".to_string(),
                        name: "Complete".to_string(),
                        execution_strategy: "fresh".to_string(),
                        fresh_context_guaranteed: false,
                    },
                    run: RoutineTickRun {
                        id: "run-1".to_string(),
                        lease_expires_at: chrono::Utc::now(),
                    },
                    agent: None,
                    checkpoint: None,
                    now: chrono::Utc::now(),
                    observations: None,
                    automation_inventory: None,
                    limits: ObservationLimits::default(),
                },
            )
            .unwrap();

        match action {
            crate::services::routines::RoutineAction::Complete {
                result_json,
                last_result,
                ..
            } => {
                assert_eq!(last_result.as_deref(), Some("ok"));
                assert_eq!(
                    result_json.unwrap(),
                    serde_json::json!({"routineId": "routine-1", "runId": "run-1"})
                );
            }
            other => panic!("unexpected action: {other:?}"),
        }
    }

    #[test]
    fn exposes_tick_agent_idle_state_to_js() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("agent-idle.js");
        std::fs::write(
            &path,
            r#"
            agentdesk.routines.register({
              name: "Agent Idle",
              tick(ctx) {
                if (!ctx.agent.is_idle) {
                  return {
                    action: "skip",
                    reason: "agent not idle",
                    result: { isIdle: ctx.agent.is_idle },
                    lastResult: "skipped"
                  };
                }

                return {
                  action: "complete",
                  result: { isIdle: ctx.agent.is_idle },
                  lastResult: "idle"
                };
              }
            });
            "#,
        )
        .unwrap();

        let loader = RoutineScriptLoader::new().unwrap();
        loader.load_script(dir.path(), &path).unwrap();

        let context_for = |is_idle: bool| RoutineTickContext {
            routine: RoutineTickRoutine {
                id: "routine-1".to_string(),
                agent_id: Some("monitoring".to_string()),
                script_ref: "agent-idle.js".to_string(),
                name: "Agent Idle".to_string(),
                execution_strategy: "fresh".to_string(),
                fresh_context_guaranteed: false,
            },
            run: RoutineTickRun {
                id: "run-1".to_string(),
                lease_expires_at: chrono::Utc::now(),
            },
            agent: Some(RoutineTickAgent {
                id: "monitoring".to_string(),
                status: if is_idle { "idle" } else { "working" }.to_string(),
                is_idle,
                current_task_id: None,
                current_thread_channel_id: None,
            }),
            checkpoint: None,
            now: chrono::Utc::now(),
            observations: None,
            automation_inventory: None,
            limits: ObservationLimits::default(),
        };

        let idle_action = loader
            .execute_tick("agent-idle.js", context_for(true))
            .unwrap();
        match idle_action {
            crate::services::routines::RoutineAction::Complete {
                result_json,
                last_result,
                ..
            } => {
                assert_eq!(last_result.as_deref(), Some("idle"));
                assert_eq!(result_json.unwrap(), serde_json::json!({"isIdle": true}));
            }
            other => panic!("unexpected idle action: {other:?}"),
        }

        let working_action = loader
            .execute_tick("agent-idle.js", context_for(false))
            .unwrap();
        match working_action {
            crate::services::routines::RoutineAction::Skip {
                reason,
                result_json,
                last_result,
                ..
            } => {
                assert_eq!(reason.as_deref(), Some("agent not idle"));
                assert_eq!(last_result.as_deref(), Some("skipped"));
                assert_eq!(result_json.unwrap(), serde_json::json!({"isIdle": false}));
            }
            other => panic!("unexpected working action: {other:?}"),
        }
    }

    #[test]
    fn rejects_cyclic_action_result_payloads() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cycle.js");
        std::fs::write(
            &path,
            r#"
            agentdesk.routines.register({
              name: "Cycle",
              tick() {
                const result = { ok: true };
                result.self = result;
                return { action: "complete", result };
              }
            });
            "#,
        )
        .unwrap();

        let loader = RoutineScriptLoader::new().unwrap();
        loader.load_script(dir.path(), &path).unwrap();
        let error = loader
            .execute_tick(
                "cycle.js",
                RoutineTickContext {
                    routine: RoutineTickRoutine {
                        id: "routine-1".to_string(),
                        agent_id: None,
                        script_ref: "cycle.js".to_string(),
                        name: "Cycle".to_string(),
                        execution_strategy: "fresh".to_string(),
                        fresh_context_guaranteed: false,
                    },
                    run: RoutineTickRun {
                        id: "run-1".to_string(),
                        lease_expires_at: chrono::Utc::now(),
                    },
                    agent: None,
                    checkpoint: None,
                    now: chrono::Utc::now(),
                    observations: None,
                    automation_inventory: None,
                    limits: ObservationLimits::default(),
                },
            )
            .unwrap_err();

        let message = error.to_string();
        assert!(
            message.contains("cycle check failed") || message.contains("cyclic object graph"),
            "{message}"
        );
    }

    #[test]
    fn bundled_sample_routines_load_and_validate() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("routines");
        let loader = RoutineScriptLoader::new().unwrap();
        assert_eq!(loader.load_dir(&root).unwrap(), 2);
        assert_eq!(
            loader.script_refs().unwrap(),
            vec![
                "agent-checkpoint-review.js".to_string(),
                "script-summary.js".to_string(),
            ]
        );

        let context_for = |script_ref: &str, name: &str| RoutineTickContext {
            routine: RoutineTickRoutine {
                id: "routine-1".to_string(),
                agent_id: Some("maker".to_string()),
                script_ref: script_ref.to_string(),
                name: name.to_string(),
                execution_strategy: "fresh".to_string(),
                fresh_context_guaranteed: false,
            },
            run: RoutineTickRun {
                id: "run-1".to_string(),
                lease_expires_at: chrono::Utc::now(),
            },
            agent: None,
            checkpoint: None,
            now: chrono::Utc::now(),
            observations: None,
            automation_inventory: None,
            limits: ObservationLimits::default(),
        };

        assert!(matches!(
            loader
                .execute_tick(
                    "script-summary.js",
                    context_for("script-summary.js", "script-only-summary")
                )
                .unwrap(),
            crate::services::routines::RoutineAction::Complete { .. }
        ));
        assert!(matches!(
            loader
                .execute_tick(
                    "agent-checkpoint-review.js",
                    context_for("agent-checkpoint-review.js", "agent-checkpoint-review")
                )
                .unwrap(),
            crate::services::routines::RoutineAction::Agent { .. }
        ));
    }
}
