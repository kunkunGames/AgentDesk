What changed:
Produced a no-change report because rustls-webpki @ 0.102.8 cannot be safely updated.

Why:
`rustls-webpki 0.102.8` has multiple high-severity vulnerability reports (`RUSTSEC-2026-0098`, `RUSTSEC-2026-0049`, `RUSTSEC-2026-0104`, `RUSTSEC-2026-0099`).
However, it is depended on by `rustls v0.22.4`, which is required by `tokio-tungstenite v0.21.0`, which is required by `serenity v0.12.5`, which is a direct dependency.
Trying to upgrade `rustls-webpki` using `cargo update -p rustls-webpki@0.102.8 --precise 0.103.13` fails because `rustls v0.22.4` requires `rustls-webpki = "^0.102.1"`.

The safe change is unclear since we cannot easily bump the `rustls-webpki` dependency under `rustls v0.22.4` without either a patch from upstream `rustls v0.22` or bumping `serenity` and its whole ecosystem.

WorkFingerprint:
- Agent: Supply-Lite
- Boundary: package.json files, Cargo.toml files, toolchain and CI setup
- Primary files: Cargo.lock
- Invariants protected: Only make conservative dependency updates.
- Verification plan: N/A - no changes made.
- Related PRs: None (though there may be an overlapping open PR for this already if someone has been trying to bump serenity).

Duplicate/overlap check:
Checked open PRs (e.g., `origin/jules/supply-lite/rustls-webpki-update-15941581891331716903`) but ultimately decided to back off due to unclear path.

Verification:
- `cargo tree --invert rustls-webpki@0.102.8` shows the dependency chain `agentdesk -> serenity 0.12 -> tokio-tungstenite 0.21.0 -> rustls 0.22.4 -> rustls-webpki 0.102.8`.
- No code was changed.

Skipped checks:
- No changes made, so no cargo check, cargo test, or generate docs commands were run.

Risk:
- The `rustls-webpki 0.102.8` vulnerabilities remain.

Rollback notes:
- N/A
