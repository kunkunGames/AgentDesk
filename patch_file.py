import re

with open('src/services/discord/commands/text_commands.rs', 'r') as f:
    content = f.read()

content = re.sub(
    r'crate::services::discord::outbound::message_outbox::send_message\(&data\.shared, channel_id, CreateMessage::new\(\)\.add_file\(attachment\)\)',
    r'channel_id.send_message(&ctx.http, CreateMessage::new().add_file(attachment))',
    content
)

content = re.sub(
    r'crate::services::discord::outbound::message_outbox::send_message\(&data\.shared, channel_id, CreateMessage::new\(\)\.content\(format!\("Running skill: `/{skill}`"\)\)\)',
    r'channel_id.send_message(&ctx.http, CreateMessage::new().content(format!("Running skill: `/{skill}`")))',
    content
)

with open('src/services/discord/commands/text_commands.rs', 'w') as f:
    f.write(content)
