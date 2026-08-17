pub mod agy;
pub mod grok;

use std::sync::mpsc::Sender;

use crate::services::agent_protocol::StreamMessage;

use super::request::ProviderTurnRequest;

pub fn execute_stream_json_dialect(
    dialect: crate::services::provider::StreamJsonDialectId,
    request: ProviderTurnRequest,
    sender: Sender<StreamMessage>,
) -> Result<(), String> {
    match dialect {
        crate::services::provider::StreamJsonDialectId::Gemini => {
            let materialized = match request.tool_policy.effective_for_legacy_provider() {
                super::policy::ToolPolicy::ProviderDefault => None,
                super::policy::ToolPolicy::ReadOnly => Some(vec!["Read".to_string()]),
                super::policy::ToolPolicy::AllowListed(tools) => Some(
                    tools
                        .iter()
                        .map(|tool| tool.as_str().to_string())
                        .collect::<Vec<_>>(),
                ),
            };
            crate::services::gemini::execute_command_streaming(
                &request.prompt,
                request.session.as_ref().map(|token| token.as_str()),
                &request.working_directory.to_string_lossy(),
                sender,
                request.system_prompt.as_deref(),
                materialized.as_deref(),
                request.cancel,
                request.remote_profile.as_ref(),
                None,
                None,
                Some(request.provider),
                request.model.as_deref(),
                None,
            )
        }
        crate::services::provider::StreamJsonDialectId::Grok => grok::execute(request, sender),
        crate::services::provider::StreamJsonDialectId::Agy => agy::execute(request, sender),
    }
}
