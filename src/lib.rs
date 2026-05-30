#![recursion_limit = "256"]
// Non-dead-code clippy debt predates the bin/lib split. Keep the existing
// style/complexity debt explicit here so this change can remove the crate-wide
// dead_code blanket without rewriting unrelated modules.
#![allow(
    clippy::absurd_extreme_comparisons,
    clippy::assertions_on_constants,
    clippy::await_holding_lock,
    clippy::bind_instead_of_map,
    clippy::bool_assert_comparison,
    clippy::clone_on_copy,
    clippy::collapsible_if,
    clippy::collapsible_str_replace,
    clippy::derivable_impls,
    clippy::doc_lazy_continuation,
    clippy::doc_overindented_list_items,
    clippy::double_ended_iterator_last,
    clippy::empty_line_after_doc_comments,
    clippy::err_expect,
    clippy::explicit_auto_deref,
    clippy::explicit_counter_loop,
    clippy::field_reassign_with_default,
    clippy::filter_next,
    clippy::if_same_then_else,
    clippy::items_after_test_module,
    clippy::iter_cloned_collect,
    clippy::large_enum_variant,
    clippy::let_and_return,
    clippy::let_unit_value,
    clippy::let_underscore_future,
    clippy::manual_clamp,
    clippy::manual_contains,
    clippy::manual_inspect,
    clippy::manual_is_multiple_of,
    clippy::manual_map,
    clippy::manual_pattern_char_comparison,
    clippy::manual_range_contains,
    clippy::manual_range_patterns,
    clippy::manual_repeat_n,
    clippy::manual_str_repeat,
    clippy::manual_strip,
    clippy::manual_unwrap_or,
    clippy::manual_unwrap_or_default,
    clippy::map_identity,
    clippy::needless_as_bytes,
    clippy::needless_borrow,
    clippy::needless_borrows_for_generic_args,
    clippy::needless_lifetimes,
    clippy::needless_option_as_deref,
    clippy::needless_range_loop,
    clippy::needless_return,
    clippy::nonminimal_bool,
    clippy::obfuscated_if_else,
    clippy::option_as_ref_deref,
    clippy::overly_complex_bool_expr,
    clippy::print_literal,
    clippy::ptr_arg,
    clippy::question_mark,
    clippy::redundant_closure,
    clippy::redundant_guards,
    clippy::redundant_locals,
    clippy::redundant_pattern_matching,
    clippy::result_large_err,
    clippy::single_char_add_str,
    clippy::single_match,
    clippy::suspicious_open_options,
    clippy::too_many_arguments,
    clippy::trim_split_whitespace,
    clippy::type_complexity,
    clippy::unnecessary_cast,
    clippy::unnecessary_filter_map,
    clippy::unnecessary_lazy_evaluations,
    clippy::unnecessary_map_or,
    clippy::unnecessary_sort_by,
    clippy::unnecessary_to_owned,
    clippy::unnecessary_unwrap,
    clippy::unusual_byte_groupings,
    clippy::useless_conversion,
    clippy::useless_format
)]

mod bootstrap;
// CLI subcommands include operational and migration surfaces that are not all
// exercised by every binary/test target yet.
#[allow(dead_code)]
mod cli;
// Legacy path shims remain available while runtime-layout migrations settle.
#[allow(dead_code)]
pub(crate) mod compat;
// Config helpers are shared by CLI/server/tests, with some provider-onboarding
// helpers only called from rollout flows.
#[allow(dead_code)]
mod config;
pub(crate) mod credential;
// Database repositories intentionally expose cross-route helpers that are only
// wired from selected API/maintenance paths.
#[allow(dead_code)]
mod db;
// Dispatch orchestration spans direct, review, auto-queue, and test-support
// paths that are not all active in each target.
#[allow(dead_code)]
mod dispatch;
// Policy-engine loader/runtime helpers cover hot-reload and test-only entry
// points outside the default server launch path.
#[allow(dead_code)]
mod engine;
// Error-code helpers are kept for API response boundaries that only some
// routes currently surface.
#[allow(dead_code)]
mod error;
// GitHub sync/triage helpers are optional integration surfaces behind runtime
// configuration.
#[allow(dead_code)]
mod github;
// Kanban transition helpers include hook and cleanup entry points selected by
// policy/runtime flows.
#[allow(dead_code)]
pub(crate) mod kanban;
mod launch;
mod logging;
pub(crate) mod manual_intervention;
// Pipeline policy structs include retry/override fields retained for policy
// compatibility across staged rollout paths.
#[allow(dead_code)]
pub(crate) mod pipeline;
pub(crate) mod receipt;
// Reconciliation sweep jobs are invoked by maintenance scheduling, not by every
// compile target.
#[allow(dead_code)]
pub(crate) mod reconcile;
pub(crate) mod runtime;
// Runtime layout exposes migration helpers used by setup and repair commands.
#[allow(dead_code)]
pub(crate) mod runtime_layout;
// Server route modules include API endpoints whose handlers are selected by
// router composition and integration tests.
#[allow(dead_code)]
mod server;
// Service modules contain provider, Discord, maintenance, and observability
// feature surfaces that are enabled by runtime config rather than all targets.
#[allow(dead_code)]
mod services;
// Supervisor test hooks are intentionally retained for dispatch/runtime tests.
#[allow(dead_code)]
pub(crate) mod supervisor;
mod ui;
// Utility detectors are shared opportunistically across provider paths.
#[allow(dead_code)]
mod utils;
// Voice runtime is an optional provider feature; most entry points are wired
// only when voice config is enabled.
#[allow(dead_code)]
pub(crate) mod voice;

#[cfg(test)]
mod high_risk_recovery;
#[cfg(all(test, feature = "legacy-sqlite-tests"))]
mod integration_tests;

// Re-export for crate-level access (used by services::discord::mod.rs)
pub(crate) use cli::agentdesk_runtime_root;

use anyhow::{Context, Result};

/// Target soft `RLIMIT_NOFILE` for the running process. Mirrors the launchd
/// `SoftResourceLimits` value written by `agentdesk init` (see
/// `cli::init::LAUNCHD_NOFILE_SOFT_LIMIT_TARGET`) so the binary keeps the same
/// FD headroom even when started outside launchd.
#[cfg(unix)]
const NOFILE_SOFT_TARGET: libc::rlim_t = 16_384;

/// Returns the soft limit we should raise to, or `None` when the current soft
/// limit already meets the target (we never lower an existing limit).
#[cfg(unix)]
fn desired_nofile_soft_limit(cur: libc::rlim_t, max: libc::rlim_t) -> Option<libc::rlim_t> {
    let desired = if max == libc::RLIM_INFINITY {
        NOFILE_SOFT_TARGET
    } else {
        NOFILE_SOFT_TARGET.min(max)
    };
    (cur < desired).then_some(desired)
}

/// Best-effort raise of this process's soft `RLIMIT_NOFILE` toward
/// [`NOFILE_SOFT_TARGET`], clamped to the hard limit. Non-fatal: any failure is
/// reported to stderr and startup continues.
#[cfg(unix)]
fn raise_nofile_soft_limit() {
    let mut limits = std::mem::MaybeUninit::<libc::rlimit>::uninit();
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, limits.as_mut_ptr()) } != 0 {
        return;
    }
    let mut limits = unsafe { limits.assume_init() };
    let Some(desired) = desired_nofile_soft_limit(limits.rlim_cur, limits.rlim_max) else {
        return;
    };
    limits.rlim_cur = desired;
    if unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &limits) } != 0 {
        eprintln!(
            "warning: failed to raise RLIMIT_NOFILE soft limit to {desired}: {}",
            std::io::Error::last_os_error()
        );
    }
}

#[cfg(not(unix))]
fn raise_nofile_soft_limit() {}

#[cfg(all(test, unix))]
mod nofile_limit_tests {
    use super::{NOFILE_SOFT_TARGET, desired_nofile_soft_limit};

    #[test]
    fn raises_when_below_target_and_clamps_to_hard() {
        assert_eq!(
            desired_nofile_soft_limit(256, 1_000_000),
            Some(NOFILE_SOFT_TARGET)
        );
        assert_eq!(desired_nofile_soft_limit(256, 1_024), Some(1_024));
        assert_eq!(
            desired_nofile_soft_limit(256, libc::RLIM_INFINITY),
            Some(NOFILE_SOFT_TARGET)
        );
    }

    #[test]
    fn never_lowers_existing_limit() {
        assert_eq!(desired_nofile_soft_limit(NOFILE_SOFT_TARGET, 1_000_000), None);
        assert_eq!(desired_nofile_soft_limit(65_536, 1_000_000), None);
    }
}

pub fn run_from_args() -> Result<()> {
    raise_nofile_soft_limit();
    match cli::args::parse() {
        cli::args::ParseOutcome::Command(command) => cli::execute(command),
        cli::args::ParseOutcome::RunServer => {
            let state = bootstrap::initialize().context("Bootstrap failed")?;
            launch::run(state).context("Launch failed")
        }
    }
}
