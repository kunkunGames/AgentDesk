//! Atomic calendar intent and durable request keys; provider I/O never occurs in transactions.
use serde::Serialize;
use serde_json::{Value, json};
use sqlx::{PgPool, Postgres, Row, Transaction};
use uuid::Uuid;

mod execution;
pub(crate) use execution::*;
#[cfg(test)]
mod postgres_tests;

#[derive(Debug, thiserror::Error)]
pub enum CalendarDbError {
    #[error("calendar request conflicts with existing intent")]
    Conflict,
    #[error("managed calendar event not found")]
    NotFound,
    #[error("calendar account binding changed or is duplicated")]
    Binding,
    #[error("calendar storage unavailable")]
    Database(#[from] sqlx::Error),
}

#[derive(Clone, sqlx::FromRow)]
pub struct Binding {
    pub account_id: String,
    pub binding_id: Uuid,
    pub app_id: i64,
    pub user_id: i64,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Receipt {
    pub event_id: Uuid,
    pub revision: i64,
    pub status: &'static str,
}

pub async fn bind_account(
    pool: &PgPool,
    account: &str,
    app: i64,
    user: i64,
) -> Result<Binding, CalendarDbError> {
    let row = sqlx::query_as::<_, Binding>(
        "INSERT INTO kakao_calendar_bindings(account_id,binding_id,app_id,user_id) VALUES($1,$2,$3,$4)
         ON CONFLICT(account_id) DO UPDATE SET checked_at=NOW()
         WHERE kakao_calendar_bindings.app_id=EXCLUDED.app_id AND kakao_calendar_bindings.user_id=EXCLUDED.user_id
         RETURNING account_id,binding_id,app_id,user_id")
        .bind(account).bind(Uuid::new_v4()).bind(app).bind(user).fetch_optional(pool).await;
    match row {
        Ok(Some(binding)) => Ok(binding),
        Ok(None) => Err(CalendarDbError::Binding),
        Err(sqlx::Error::Database(error)) if error.is_unique_violation() => {
            Err(CalendarDbError::Binding)
        }
        Err(error) => Err(error.into()),
    }
}

async fn replay_tx(
    tx: &mut Transaction<'_, Postgres>,
    key: &str,
    fingerprint: &str,
) -> Result<Option<Receipt>, CalendarDbError> {
    // Serialize identical keys, including first insert, without retaining a transaction over I/O.
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 120))")
        .bind(key)
        .execute(&mut **tx)
        .await?;
    let row = sqlx::query(
        "SELECT fingerprint,event_id,revision FROM kakao_calendar_requests WHERE request_key=$1",
    )
    .bind(key)
    .fetch_optional(&mut **tx)
    .await?;
    row.map(|row| {
        if row.get::<String, _>("fingerprint") != fingerprint {
            return Err(CalendarDbError::Conflict);
        }
        Ok(Receipt {
            event_id: row.get("event_id"),
            revision: row.get("revision"),
            status: "accepted",
        })
    })
    .transpose()
}

pub async fn replay(
    pool: &PgPool,
    key: &str,
    fingerprint: &str,
) -> Result<Option<Receipt>, CalendarDbError> {
    let mut tx = pool.begin().await?;
    let receipt = replay_tx(&mut tx, key, fingerprint).await?;
    tx.commit().await?;
    Ok(receipt)
}

async fn record_request(
    tx: &mut Transaction<'_, Postgres>,
    key: &str,
    fingerprint: &str,
    event: Uuid,
    revision: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query("INSERT INTO kakao_calendar_requests(request_key,fingerprint,event_id,revision) VALUES($1,$2,$3,$4)")
        .bind(key).bind(fingerprint).bind(event).bind(revision).execute(&mut **tx).await?;
    Ok(())
}

pub async fn create(
    pool: &PgPool,
    key: &str,
    fingerprint: &str,
    content: &Value,
    bindings: &[Binding],
) -> Result<Receipt, CalendarDbError> {
    let mut tx = pool.begin().await?;
    if let Some(receipt) = replay_tx(&mut tx, key, fingerprint).await? {
        tx.commit().await?;
        return Ok(receipt);
    }
    let event = Uuid::new_v4();
    sqlx::query("INSERT INTO kakao_calendar_events(id,revision,content) VALUES($1,1,$2)")
        .bind(event)
        .bind(content)
        .execute(&mut *tx)
        .await?;
    record_request(&mut tx, key, fingerprint, event, 1).await?;
    for binding in bindings {
        let target = Uuid::new_v4();
        sqlx::query("INSERT INTO kakao_calendar_targets(id,event_id,binding_id) VALUES($1,$2,$3)")
            .bind(target)
            .bind(event)
            .bind(binding.binding_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("INSERT INTO kakao_calendar_operations(id,target_id,request_key,revision,action,snapshot) VALUES($1,$2,$3,1,'create',$4)")
            .bind(Uuid::new_v4()).bind(target).bind(key).bind(content).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    Ok(Receipt {
        event_id: event,
        revision: 1,
        status: "accepted",
    })
}

pub struct Mutation<'a> {
    pub event: Uuid,
    pub key: &'a str,
    pub fingerprint: &'a str,
    pub expected_revision: i64,
    pub content: &'a Value,
    pub delete: bool,
}

pub async fn mutate(pool: &PgPool, mutation: Mutation<'_>) -> Result<Receipt, CalendarDbError> {
    let mut tx = pool.begin().await?;
    if let Some(receipt) = replay_tx(&mut tx, mutation.key, mutation.fingerprint).await? {
        tx.commit().await?;
        return Ok(receipt);
    }
    let row =
        sqlx::query("SELECT revision,deleted FROM kakao_calendar_events WHERE id=$1 FOR UPDATE")
            .bind(mutation.event)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(CalendarDbError::NotFound)?;
    if row.get::<i64, _>("revision") != mutation.expected_revision || row.get::<bool, _>("deleted")
    {
        return Err(CalendarDbError::Conflict);
    }
    let revision = mutation
        .expected_revision
        .checked_add(1)
        .ok_or(CalendarDbError::Conflict)?;
    sqlx::query("UPDATE kakao_calendar_events SET revision=$2,content=$3,deleted=$4,updated_at=NOW() WHERE id=$1")
        .bind(mutation.event).bind(revision).bind(mutation.content).bind(mutation.delete).execute(&mut *tx).await?;
    record_request(
        &mut tx,
        mutation.key,
        mutation.fingerprint,
        mutation.event,
        revision,
    )
    .await?;
    // Never supersede an uncertain or dispatched predecessor. Undispatched work is safe to fold into latest intent.
    sqlx::query(
        "UPDATE kakao_calendar_operations o SET status='superseded',updated_at=NOW()
        FROM kakao_calendar_targets t WHERE o.target_id=t.id AND t.event_id=$1
        AND o.status IN ('queued','blocked','rejected') AND o.dispatched_at IS NULL",
    )
    .bind(mutation.event)
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "INSERT INTO kakao_calendar_operations(id,target_id,request_key,revision,action,snapshot)
        SELECT gen_random_uuid(),id,$2,$3,$4,$5 FROM kakao_calendar_targets WHERE event_id=$1",
    )
    .bind(mutation.event)
    .bind(mutation.key)
    .bind(revision)
    .bind(if mutation.delete { "delete" } else { "update" })
    .bind(mutation.content)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(Receipt {
        event_id: mutation.event,
        revision,
        status: "accepted",
    })
}

pub async fn event_accounts(pool: &PgPool, event: Uuid) -> Result<Vec<String>, CalendarDbError> {
    let accounts = sqlx::query_scalar("SELECT b.account_id FROM kakao_calendar_targets t JOIN kakao_calendar_bindings b USING(binding_id) WHERE t.event_id=$1 ORDER BY b.account_id")
        .bind(event).fetch_all(pool).await?;
    Ok(accounts)
}

pub async fn get(pool: &PgPool, event: Uuid) -> Result<Value, CalendarDbError> {
    let row = sqlx::query("SELECT revision,content,deleted FROM kakao_calendar_events WHERE id=$1")
        .bind(event)
        .fetch_optional(pool)
        .await?
        .ok_or(CalendarDbError::NotFound)?;
    let targets = sqlx::query("SELECT b.account_id,t.applied_revision,t.remote_id,o.status,o.error_code,o.id AS operation_id
        FROM kakao_calendar_targets t JOIN kakao_calendar_bindings b USING(binding_id)
        JOIN LATERAL (SELECT id,status,error_code FROM kakao_calendar_operations WHERE target_id=t.id
          ORDER BY (status IN ('needs_reconcile','dispatching','blocked','rejected')) DESC,revision DESC LIMIT 1) o ON TRUE
        WHERE t.event_id=$1 ORDER BY b.account_id").bind(event).fetch_all(pool).await?;
    let targets: Vec<Value> = targets.into_iter().map(|row| json!({"accountId":row.get::<String,_>("account_id"),"appliedRevision":row.get::<i64,_>("applied_revision"),"remoteKnown":row.get::<Option<String>,_>("remote_id").is_some(),"status":row.get::<String,_>("status"),"errorCode":row.get::<Option<String>,_>("error_code"),"operationId":row.get::<Uuid,_>("operation_id")})).collect();
    let applied = targets
        .iter()
        .filter(|target| target["status"] == "applied")
        .count();
    let status = if targets
        .iter()
        .any(|target| target["status"] == "needs_reconcile")
    {
        "unknown"
    } else if applied == targets.len() {
        "success"
    } else if applied > 0 {
        "partial_success"
    } else if targets
        .iter()
        .all(|target| target["status"] == "blocked" || target["status"] == "rejected")
    {
        "failed"
    } else {
        "accepted"
    };
    Ok(
        json!({"eventId":event,"revision":row.get::<i64,_>("revision"),"content":row.get::<Value,_>("content"),"deleted":row.get::<bool,_>("deleted"),"status":status,"targets":targets}),
    )
}

pub async fn list(
    pool: &PgPool,
    accounts: &[String],
    before: Option<Uuid>,
    limit: i64,
) -> Result<Vec<Uuid>, CalendarDbError> {
    Ok(sqlx::query_scalar("SELECT e.id FROM kakao_calendar_events e WHERE ($1::uuid IS NULL OR e.id < $1)
        AND NOT EXISTS (SELECT 1 FROM kakao_calendar_targets t JOIN kakao_calendar_bindings b USING(binding_id)
        WHERE t.event_id=e.id AND NOT(b.account_id=ANY($2))) ORDER BY e.id DESC LIMIT $3")
        .bind(before).bind(accounts).bind(limit).fetch_all(pool).await?)
}

pub async fn operations(pool: &PgPool, event: Uuid) -> Result<Vec<Value>, CalendarDbError> {
    let rows = sqlx::query("SELECT o.id,o.revision,o.action,o.status,o.error_code,o.dispatched_at,o.attempts,o.recovery_note,b.account_id
        FROM kakao_calendar_operations o JOIN kakao_calendar_targets t ON t.id=o.target_id JOIN kakao_calendar_bindings b USING(binding_id)
        WHERE t.event_id=$1 ORDER BY o.revision DESC,b.account_id LIMIT 200").bind(event).fetch_all(pool).await?;
    Ok(rows.into_iter().map(|r| json!({"operationId":r.get::<Uuid,_>("id"),"revision":r.get::<i64,_>("revision"),"action":r.get::<String,_>("action"),"status":r.get::<String,_>("status"),"accountId":r.get::<String,_>("account_id"),"errorCode":r.get::<Option<String>,_>("error_code"),"dispatchedAt":r.get::<Option<chrono::DateTime<chrono::Utc>>,_>("dispatched_at"),"attempts":r.get::<i32,_>("attempts"),"recoveryNote":r.get::<Option<String>,_>("recovery_note")})).collect())
}
