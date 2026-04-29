/// Routines core runtime — durable store plus script-only runtime pieces.
///
/// This module is intentionally small at this stage. It exposes only
/// `RoutineStore` exposes PG-backed claim/finish/recovery transactions.
/// `RoutineScriptLoader` and `RoutineAction` are the ORDER-P0-002 foundation;
/// the worker tick loop and `/api/routines` route integration land after this
/// isolated loader/validator is verified.
///
/// SQLite users: this module is never instantiated when `pg_pool` is `None`,
/// so there are zero side effects for non-PG deployments.
pub mod action;
pub mod loader;
pub mod store;

pub use action::{RoutineAction, validate_routine_action};
pub use loader::RoutineScriptLoader;
pub use store::RoutineStore;
