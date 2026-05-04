# DomainKeeper Journal

- DomainKeeper PR titles must follow specific formats, such as `🧾 DomainKeeper: clarify <settings boundary> contract`.
- Configuration domain boundaries (runtime-config, dashboard settings, bot-settings) and their precedence are documented as the source-of-truth in `docs/config-domains.md`, `docs/source-of-truth.md`, and `docs/adr-settings-precedence.md`.
- In AgentDesk's configuration, `server_port` is strictly considered read-only system metadata. Dashboard settings enforce full-replace semantics (not patch merging), while runtime config separates baseline configuration from live overrides.
