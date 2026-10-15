//! Rate-limit response header parsing, including absolute and relative reset times.

pub(super) fn parse_header_i64(headers: &reqwest::header::HeaderMap, name: &str) -> Option<i64> {
    headers.get(name)?.to_str().ok()?.parse().ok()
}

/// Parse reset timestamp from a rate-limit header into unix epoch seconds.
///
/// Anthropic returns RFC3339 timestamps. OpenAI commonly returns relative
/// durations such as `1s` or `6m0s`, so accept both forms.
pub(super) fn parse_header_reset(headers: &reqwest::header::HeaderMap, name: &str) -> i64 {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| parse_rate_limit_reset_value(s, chrono::Utc::now().timestamp()))
        .unwrap_or(0)
}

pub(super) fn parse_rate_limit_reset_value(value: &str, now: i64) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|dt| dt.timestamp())
        .or_else(|| parse_rate_limit_duration_seconds(value).map(|seconds| now + seconds))
}

pub(super) fn parse_rate_limit_duration_seconds(value: &str) -> Option<i64> {
    let input = value.trim();
    if input.is_empty() {
        return None;
    }
    if let Ok(seconds) = input.parse::<i64>() {
        return Some(seconds.max(0));
    }

    let mut total = 0.0_f64;
    let mut number = String::new();
    let mut consumed = false;
    let chars = input.chars().collect::<Vec<_>>();
    let mut index = 0;
    while index < chars.len() {
        let ch = chars[index];
        if ch.is_ascii_digit() || ch == '.' {
            number.push(ch);
            index += 1;
            continue;
        }
        if number.is_empty() {
            return None;
        }
        let amount = number.parse::<f64>().ok()?;
        number.clear();
        let multiplier = match ch {
            'd' => 86_400.0,
            'h' => 3_600.0,
            'm' => {
                if chars.get(index + 1) == Some(&'s') {
                    index += 1;
                    0.001
                } else {
                    60.0
                }
            }
            's' => 1.0,
            _ => return None,
        };
        total += amount * multiplier;
        consumed = true;
        index += 1;
    }

    if !number.is_empty() || !consumed {
        return None;
    }
    Some(total.ceil().max(0.0) as i64)
}
