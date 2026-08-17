//! Shared Kakao default-template construction and public image URL checks.
//!
//! Kakao fetches feed images itself. Keep the URL contract here so scheduled
//! and manual sends share one public-HTTPS rule without growing `kakao.rs`.

use serde_json::json;

use super::kakao::KakaoError;

/// Kakao fetches feed images itself. Restrict the URL to a bounded public HTTPS
/// location so an operator cannot accidentally hand a private network address
/// or a local scheduled attachment blob to the external provider.
pub fn validate_kakao_image_url(image_url: Option<&str>) -> Result<(), KakaoError> {
    let Some(image_url) = image_url else {
        return Ok(());
    };
    if image_url.len() > 2_048 || image_url.trim() != image_url {
        return Err(KakaoError::Validation(
            "image_url must be a public HTTPS URL",
        ));
    }
    let url = reqwest::Url::parse(image_url)
        .map_err(|_| KakaoError::Validation("image_url must be a public HTTPS URL"))?;
    let host_allowed = match url.host() {
        Some(url::Host::Domain(host)) => {
            !host.eq_ignore_ascii_case("localhost") && !is_private_ip_literal(host)
        }
        Some(url::Host::Ipv4(address)) => !is_blocked_ipv4(address),
        Some(url::Host::Ipv6(address)) => !is_blocked_ipv6(address),
        None => false,
    };
    if url.scheme() != "https"
        || url.username() != ""
        || url.password().is_some()
        || url.port().is_some()
        || !host_allowed
    {
        return Err(KakaoError::Validation(
            "image_url must be a public HTTPS URL",
        ));
    }
    Ok(())
}

pub(crate) fn message_template(text: &str, image_url: Option<&str>, landing_url: &str) -> String {
    let link = json!({
        "web_url": landing_url,
        "mobile_web_url": landing_url
    });
    match image_url {
        Some(image_url) => json!({
            "object_type": "feed",
            "content": {
                "title": feed_title(text),
                "description": text,
                "image_url": image_url,
                "link": link
            },
            "button_title": "문서 보기"
        })
        .to_string(),
        None => json!({
            "object_type": "text",
            "text": text,
            "link": link
        })
        .to_string(),
    }
}

fn feed_title(text: &str) -> String {
    text.lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .unwrap_or("예약 메시지")
        .chars()
        .take(50)
        .collect()
}

fn is_private_ip_literal(host: &str) -> bool {
    let host = host
        .strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
        .unwrap_or(host);
    let Ok(address) = host.parse::<std::net::IpAddr>() else {
        return false;
    };
    match address {
        std::net::IpAddr::V4(address) => is_blocked_ipv4(address),
        std::net::IpAddr::V6(address) => is_blocked_ipv6(address),
    }
}

fn is_blocked_ipv6(address: std::net::Ipv6Addr) -> bool {
    if let Some(mapped) = address.to_ipv4_mapped() {
        return is_blocked_ipv4(mapped);
    }
    let segments = address.segments();
    address.is_loopback()
        || address.is_unspecified()
        || address.is_unique_local()
        || address.is_unicast_link_local()
        || address.is_multicast()
        || (segments[0] == 0x2001 && segments[1] == 0x0db8)
}

fn is_blocked_ipv4(address: std::net::Ipv4Addr) -> bool {
    let octets = address.octets();
    address.is_private()
        || address.is_loopback()
        || address.is_link_local()
        || address.is_unspecified()
        || address.is_broadcast()
        || matches!(
            octets,
            [192, 0, 2, _] | [198, 51, 100, _] | [203, 0, 113, _]
        )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn feed_template_is_used_only_for_a_validated_image_url() {
        let feed: serde_json::Value = serde_json::from_str(&message_template(
            "소복이 D-7 알림",
            Some("https://example.com/thumbnail.jpg"),
            "https://universe.vr11.net/Docs/",
        ))
        .unwrap();
        assert_eq!(feed["object_type"], "feed");
        assert_eq!(
            feed["content"]["image_url"],
            "https://example.com/thumbnail.jpg"
        );
        assert_eq!(
            feed["content"]["link"]["web_url"],
            "https://universe.vr11.net/Docs/"
        );

        let text: serde_json::Value = serde_json::from_str(&message_template(
            "소복이 D-7 알림",
            None,
            "https://universe.vr11.net/Docs/",
        ))
        .unwrap();
        assert_eq!(text["object_type"], "text");
        assert!(text.get("content").is_none());
    }

    #[test]
    fn image_url_validation_rejects_private_and_credentialed_locations() {
        for invalid in [
            "https://127.0.0.1/image.jpg",
            "https://[::1]/image.jpg",
            "https://[::ffff:127.0.0.1]/image.jpg",
            "https://user@example.com/image.jpg",
            "https://example.com:8443/image.jpg",
        ] {
            assert!(
                validate_kakao_image_url(Some(invalid)).is_err(),
                "{invalid}"
            );
        }
        assert!(validate_kakao_image_url(Some("https://cdn.example.com/image.jpg")).is_ok());
        assert!(validate_kakao_image_url(None).is_ok());
    }
}
