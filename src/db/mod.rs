pub mod agents;
pub(crate) mod schema;

use anyhow::Result;
use rusqlite::Connection;
use std::sync::{Arc, Mutex};

use crate::config::Config;

pub type Db = Arc<Mutex<Connection>>;

pub fn init(config: &Config) -> Result<Db> {
    let db_path = config.data.dir.join(&config.data.db_name);
    let conn = Connection::open(&db_path)?;

    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;")?;
    schema::migrate(&conn)?;

    tracing::info!("Database initialized at {}", db_path.display());
    Ok(Arc::new(Mutex::new(conn)))
}
