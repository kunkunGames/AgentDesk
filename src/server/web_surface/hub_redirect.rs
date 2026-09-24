//! Browser redirects use an online Hub and an operator-owned origin. Registry
//! advertisements and request headers cannot independently choose a target.

use axum::http::Uri;
use serde_json::Value;
use url::{Host, Url};

use crate::config::{ClusterConfig, ClusterRole};

pub(super) async fn online_hubs(pool: &sqlx::PgPool, ttl: u64) -> Result<Vec<Value>, sqlx::Error> {
    // Browser navigation needs only the Hub identity and origin, not dispatch
    // counts or execution occupancy from the administrative node listing.
    let rows = sqlx::query_as::<_, (String, Value)>(
        "SELECT instance_id, capabilities FROM cluster_nodes
         WHERE effective_role = $1 AND status = 'online'
           AND last_heartbeat_at >= NOW() - ($2::BIGINT * INTERVAL '1 second')
         LIMIT 2",
    )
    .bind(ClusterRole::Hub.as_str())
    .bind(i64::try_from(ttl.max(1)).unwrap_or(i64::MAX))
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(|(id, capabilities)| serde_json::json!({
        "instance_id": id, "effective_role": "hub", "status": "online",
        "api_base_url": crate::services::cluster::session_routing::runner_api_base_url_from_capabilities(&capabilities),
    })).collect())
}

pub(super) fn destination(
    cluster: &ClusterConfig,
    local_instance_id: Option<&str>,
    nodes: &[Value],
    uri: &Uri,
) -> Option<String> {
    let mut hubs = nodes.iter().filter(|node| {
        node["status"].as_str() == Some("online")
            && node["effective_role"]
                .as_str()
                .and_then(|role| role.parse::<ClusterRole>().ok())
                == Some(ClusterRole::Hub)
    });
    let hub = hubs.next()?;
    // An ambiguous election must not send browsers to an arbitrary machine.
    if hubs.next().is_some() {
        return None;
    }
    let instance_id = hub["instance_id"].as_str()?;
    if local_instance_id == Some(instance_id) {
        return None;
    }
    let configured = origin(
        cluster
            .nodes
            .get(instance_id)?
            .trusted_forward_origin
            .as_deref()?,
    )?;
    let advertised = origin(hub["api_base_url"].as_str()?)?;
    if configured != advertised {
        return None;
    }
    // Only explicit browser entry routes are eligible; never forward API calls.
    if !matches!(uri.path(), "/" | "/settings") {
        return None;
    }
    Some(format!(
        "{}{}",
        configured.origin().ascii_serialization(),
        uri.path_and_query()?.as_str()
    ))
}

fn origin(raw: &str) -> Option<Url> {
    let url = Url::parse(raw.trim()).ok()?;
    if !matches!(url.scheme(), "http" | "https")
        || !url.username().is_empty()
        || url.password().is_some()
        || url.path() != "/"
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return None;
    }
    // Loopback would point back at the browser's machine, not the remote Hub.
    let invalid_host = match url.host()? {
        Host::Domain(host) => {
            let host = host.trim_end_matches('.');
            host == "localhost" || host.ends_with(".localhost")
        }
        Host::Ipv4(ip) => {
            ip.is_loopback() || ip.is_unspecified() || ip.is_multicast() || ip.is_link_local()
        }
        Host::Ipv6(ip) => {
            ip.is_loopback()
                || ip.is_unspecified()
                || ip.is_multicast()
                || ip.is_unicast_link_local()
                || ip.to_ipv4_mapped().is_some()
        }
    };
    (!invalid_host).then_some(url)
}
