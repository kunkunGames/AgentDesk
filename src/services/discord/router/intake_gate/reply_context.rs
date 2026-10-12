//! Build reply context from canonical Discord messages and attachments.
use super::{serenity, truncate_str};

pub(super) async fn build_reply_context(
    ctx: &serenity::Context,
    channel_id: serenity::ChannelId,
    new_message: &serenity::Message,
) -> Option<String> {
    let ref_msg = new_message.referenced_message.as_ref()?;
    let ref_author = &ref_msg.author.name;
    let ref_content = ref_msg.content.trim();
    let ref_text = if ref_content.is_empty() {
        let attachments = ref_msg
            .attachments
            .iter()
            .map(AttachmentReplyItem::from)
            .collect::<Vec<_>>();
        format_attachment_reply_context(ref_author, ref_msg.id.get(), &attachments)
    } else {
        let truncated = truncate_str(ref_content, 500);
        format!(
            "[Reply context]\nAuthor: {}\nContent: {}",
            ref_author, truncated
        )
    };

    let mut context_parts = Vec::new();
    if let Ok(preceding) = channel_id
        .messages(
            &ctx.http,
            serenity::builder::GetMessages::new()
                .before(ref_msg.id)
                .limit(4),
        )
        .await
    {
        let mut msgs: Vec<_> = preceding
            .iter()
            .filter(|m| !m.content.trim().is_empty())
            .collect();
        msgs.reverse();
        let mut budget: usize = 1000;
        for m in msgs
            .iter()
            .rev()
            .take(4)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
        {
            let entry = format!("{}: {}", m.author.name, truncate_str(m.content.trim(), 300));
            if entry.len() > budget {
                break;
            }
            budget -= entry.len();
            context_parts.push(entry);
        }
    }

    if context_parts.is_empty() {
        Some(ref_text)
    } else {
        let preceding_ctx = context_parts.join("\n");
        Some(format!(
            "[Reply context — preceding conversation]\n{}\n\n{}",
            preceding_ctx, ref_text
        ))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct AttachmentReplyItem {
    pub(super) filename: String,
    pub(super) size: u32,
    pub(super) description: Option<String>,
}

impl From<&serenity::Attachment> for AttachmentReplyItem {
    fn from(attachment: &serenity::Attachment) -> Self {
        Self {
            filename: attachment.filename.clone(),
            size: attachment.size,
            description: attachment.description.clone(),
        }
    }
}

pub(super) fn format_attachment_reply_context(
    ref_author: &str,
    ref_message_id: u64,
    attachments: &[AttachmentReplyItem],
) -> String {
    if attachments.is_empty() {
        return format!("[Reply to {}'s message (no text content)]", ref_author);
    }

    let mut lines = vec![
        "[Reply context]".to_string(),
        format!("Author: {ref_author}"),
        format!("Canonical Discord message id: {ref_message_id}"),
        "Content: [message has attachments but no text]".to_string(),
        "Attachments:".to_string(),
    ];
    for (index, attachment) in attachments.iter().take(10).enumerate() {
        let description = attachment.description.as_deref().unwrap_or("").trim();
        let mut line = format!(
            "{}. {} ({} bytes)",
            index + 1,
            attachment.filename,
            attachment.size
        );
        if !description.is_empty() {
            line.push_str(&format!(" — {}", truncate_str(description, 160)));
        }
        lines.push(line);
    }
    if attachments.len() > 10 {
        lines.push(format!("... {} more attachment(s)", attachments.len() - 10));
    }
    lines.join("\n")
}
