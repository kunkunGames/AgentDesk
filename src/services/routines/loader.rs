use anyhow::{Result, anyhow};
use rquickjs::{Context, Function, Persistent, Runtime};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::engine::loader::compute_policy_version;

#[derive(Debug)]
pub struct LoadedRoutineScript {
    name: String,
    script_ref: String,
    file: PathBuf,
    script_version: String,
    tick: Persistent<Function<'static>>,
}

// SAFETY: LoadedRoutineScript is stored behind a Mutex and all JS execution is
// serialized through the owning QuickJS Context.
unsafe impl Send for LoadedRoutineScript {}
unsafe impl Sync for LoadedRoutineScript {}

pub type RoutineScriptStore = Arc<Mutex<HashMap<String, LoadedRoutineScript>>>;

/// Isolated QuickJS loader for `agentdesk.routines.register({ name, tick })`.
///
/// This intentionally does not use the PolicyEngine store or
/// `agentdesk.registerPolicy()` namespace. Failed loads return an error before
/// mutating the store, so callers keep the last-known-good registry.
pub struct RoutineScriptLoader {
    scripts: RoutineScriptStore,
    context: Context,
    _runtime: Runtime,
}

impl RoutineScriptLoader {
    pub fn new() -> Result<Self> {
        let runtime =
            Runtime::new().map_err(|e| anyhow!("routine QuickJS runtime creation failed: {e}"))?;
        let context = Context::full(&runtime)
            .map_err(|e| anyhow!("routine QuickJS context creation failed: {e}"))?;
        Ok(Self {
            scripts: Arc::new(Mutex::new(HashMap::new())),
            context,
            _runtime: runtime,
        })
    }

    pub fn load_script(&self, root: &Path, path: &Path) -> Result<String> {
        let script = load_single_routine_script(&self.context, root, path)?;
        tracing::debug!(
            routine_script = %script.script_ref,
            name = %script.name,
            file = %script.file.display(),
            version = %script.script_version,
            "loaded routine script"
        );
        let _ = &script.tick;
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
            return Ok(0);
        }

        let mut entries: Vec<PathBuf> = std::fs::read_dir(root)?
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.path())
            .filter(|path| path.extension().is_some_and(|ext| ext == "js"))
            .collect();
        entries.sort();

        let mut loaded = 0;
        for path in entries {
            match self.load_script(root, &path) {
                Ok(script_ref) => {
                    loaded += 1;
                    tracing::info!(routine_script = %script_ref, "loaded routine script");
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

        Ok(loaded)
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
}

impl Drop for RoutineScriptLoader {
    fn drop(&mut self) {
        if let Ok(mut scripts) = self.scripts.lock() {
            scripts.clear();
        }
    }
}

pub fn load_single_routine_script(
    ctx: &Context,
    root: &Path,
    path: &Path,
) -> Result<LoadedRoutineScript> {
    let source = std::fs::read_to_string(path)
        .map_err(|e| anyhow!("read routine script {}: {e}", path.display()))?;
    let fallback_name = path
        .file_stem()
        .unwrap_or_default()
        .to_string_lossy()
        .to_string();
    let script_ref = script_ref(root, path);
    let script_version = compute_policy_version(&source);

    ctx.with(|ctx| -> Result<LoadedRoutineScript> {
        let globals = ctx.globals();
        let _: rquickjs::Value = ctx
            .eval(
                r#"
                globalThis.agentdesk = globalThis.agentdesk || {};
                agentdesk.routines = agentdesk.routines || {};
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
            .unwrap_or_else(|| fallback_name.clone());

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
        let tick = Persistent::save(&ctx, tick);

        Ok(LoadedRoutineScript {
            name,
            script_ref,
            file: path.to_path_buf(),
            script_version,
            tick,
        })
    })
}

fn script_ref(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
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
}
