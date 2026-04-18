use crate::services::provider::ProviderKind;

use super::super::{Context, Error, check_auth};
use super::config::{EffectiveModelSnapshot, effective_model_snapshot};

fn describe_effective_model(snapshot: &EffectiveModelSnapshot) -> String {
    if snapshot.effective.eq_ignore_ascii_case("default") {
        format!("provider default (`{}`)", snapshot.default_source)
    } else {
        format!("`{}` (`{}`)", snapshot.effective, snapshot.source)
    }
}

fn build_fast_unavailable_notice(
    provider: &ProviderKind,
    snapshot: &EffectiveModelSnapshot,
) -> Result<String, &'static str> {
    let current_model = describe_effective_model(snapshot);
    match provider {
        ProviderKind::Claude => Ok(format!(
            "`/fast`는 현재 이 채널에서 지원되지 않습니다. Claude Code에는 AgentDesk가 사용할 수 있는 provider-native fast toggle 경로가 없습니다.\n현재 effective model은 {}이며 바꾸지 않았습니다. 모델 변경은 `/model`만 지원합니다.",
            current_model
        )),
        ProviderKind::Codex => Ok(format!(
            "`/fast`는 현재 이 채널에서 지원되지 않습니다. Codex에는 interactive TUI의 built-in `/fast`가 있지만 AgentDesk는 `codex exec --json` 경로를 사용하므로 provider-native fast mode를 전달할 수 없습니다.\n현재 effective model은 {}이며 바꾸지 않았습니다. 모델 변경은 `/model`만 지원합니다.",
            current_model
        )),
        _ => Err("/fast is only available in Claude and Codex channels."),
    }
}

/// /fast — Explain fast-mode availability without changing the model
#[poise::command(slash_command, rename = "fast")]
pub(in crate::services::discord) async fn cmd_fast(ctx: Context<'_>) -> Result<(), Error> {
    let user_id = ctx.author().id;
    let user_name = &ctx.author().name;
    if !check_auth(user_id, user_name, &ctx.data().shared, &ctx.data().token).await {
        return Ok(());
    }

    let ts = chrono::Local::now().format("%H:%M:%S");
    tracing::info!("  [{ts}] ◀ [{user_name}] /fast");

    let channel_id = ctx.channel_id();
    let snapshot = effective_model_snapshot(&ctx.data().shared, channel_id).await;
    let notice = match build_fast_unavailable_notice(&ctx.data().provider, &snapshot) {
        Ok(notice) => notice,
        Err(message) => message.to_string(),
    };
    ctx.say(notice).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{build_fast_unavailable_notice, describe_effective_model};
    use crate::services::discord::commands::config::EffectiveModelSnapshot;
    use crate::services::provider::ProviderKind;

    #[test]
    fn describe_effective_model_prefers_runtime_value() {
        let snapshot = EffectiveModelSnapshot {
            override_model: Some("gpt-5.4".to_string()),
            dispatch_role_model: None,
            role_model: None,
            effective: "gpt-5.4".to_string(),
            source: "runtime override",
            default_model: "default".to_string(),
            default_source: "provider default",
        };
        assert_eq!(
            describe_effective_model(&snapshot),
            "`gpt-5.4` (`runtime override`)"
        );
    }

    #[test]
    fn describe_effective_model_falls_back_to_provider_default() {
        let snapshot = EffectiveModelSnapshot {
            override_model: None,
            dispatch_role_model: None,
            role_model: None,
            effective: "default".to_string(),
            source: "provider default",
            default_model: "default".to_string(),
            default_source: "provider default",
        };
        assert_eq!(
            describe_effective_model(&snapshot),
            "provider default (`provider default`)"
        );
    }

    #[test]
    fn codex_notice_explains_exec_path_limitation() {
        let snapshot = EffectiveModelSnapshot {
            override_model: None,
            dispatch_role_model: Some("gpt-5.4".to_string()),
            role_model: None,
            effective: "gpt-5.4".to_string(),
            source: "dispatch-role override",
            default_model: "gpt-5.4".to_string(),
            default_source: "dispatch-role override",
        };
        let notice =
            build_fast_unavailable_notice(&ProviderKind::Codex, &snapshot).expect("notice");
        assert!(notice.contains("codex exec --json"));
        assert!(notice.contains("`gpt-5.4`"));
        assert!(notice.contains("`/model`"));
    }

    #[test]
    fn claude_notice_explains_provider_native_limitation() {
        let snapshot = EffectiveModelSnapshot {
            override_model: None,
            dispatch_role_model: None,
            role_model: None,
            effective: "default".to_string(),
            source: "provider default",
            default_model: "default".to_string(),
            default_source: "provider default",
        };
        let notice =
            build_fast_unavailable_notice(&ProviderKind::Claude, &snapshot).expect("notice");
        assert!(notice.contains("provider-native fast toggle"));
        assert!(notice.contains("모델 변경은 `/model`만"));
    }

    #[test]
    fn non_claude_codex_providers_are_rejected() {
        let snapshot = EffectiveModelSnapshot {
            override_model: None,
            dispatch_role_model: None,
            role_model: None,
            effective: "default".to_string(),
            source: "provider default",
            default_model: "default".to_string(),
            default_source: "provider default",
        };
        let error = build_fast_unavailable_notice(&ProviderKind::Gemini, &snapshot).unwrap_err();
        assert_eq!(
            error,
            "/fast is only available in Claude and Codex channels."
        );
    }
}
