use super::*;

/// Discord dependencies shared by leader and worker intake paths.
/// Workers pass no cache or live Context; leaders supply both for chained dispatch.
#[derive(Clone, Copy)]
pub(in crate::services::discord) struct IntakeDeps<'a> {
    pub http: &'a Arc<serenity::http::Http>,
    pub cache: Option<&'a Arc<serenity::cache::Cache>>,
    pub ctx_for_chained_dispatch: Option<&'a serenity::Context>,
    pub shared: &'a Arc<SharedData>,
    pub token: &'a str,
}

pub(super) async fn campaign_graph_prompt(
    pool: Option<&sqlx::PgPool>,
    dispatch_id: Option<&str>,
) -> Result<Option<String>, Error> {
    Ok(match (pool, dispatch_id) {
        (Some(pool), Some(dispatch)) => {
            let prompt =
                crate::db::campaigns::graph::store::execution_prompt(pool, dispatch).await?;
            if prompt.is_some() {
                crate::services::campaign_control_input::prepare_input(pool, dispatch).await?;
            }
            prompt
        }
        _ => None,
    })
}

pub(super) async fn discord_context(shared: &SharedData, channel_id: ChannelId) -> String {
    let data = shared.core.lock().await;
    let session = data.sessions.get(&channel_id);
    build_system_discord_context(
        session.and_then(|s| s.channel_name.as_deref()),
        session.and_then(|s| s.category_name.as_deref()),
        channel_id,
        false,
    )
}
