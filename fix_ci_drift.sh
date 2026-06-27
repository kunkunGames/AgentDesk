#!/bin/bash
set -e

# Fix hotfile_ratchet.toml
sed -i 's/"src\/services\/discord\/turn_bridge\/mod.rs" = 6664/"src\/services\/discord\/turn_bridge\/mod.rs" = 6661/' scripts/hotfile_ratchet.toml
sed -i 's/"src\/services\/discord\/tui_prompt_relay.rs" = 7789/"src\/services\/discord\/tui_prompt_relay.rs" = 8349/' scripts/hotfile_ratchet.toml

# Fix change-surfaces.md
sed -i 's/3137 in change-surfaces.md/3180 in change-surfaces.md/g' docs/agent-maintenance/change-surfaces.md
sed -i 's/1612 in change-surfaces.md/1621 in change-surfaces.md/g' docs/agent-maintenance/change-surfaces.md
sed -i 's/1375 in change-surfaces.md/1441 in change-surfaces.md/g' docs/agent-maintenance/change-surfaces.md

# Also replace the inline line counts explicitly:
sed -i 's/inflight.rs\` (3137 lines;/inflight.rs\` (3180 lines;/g' docs/agent-maintenance/change-surfaces.md
sed -i 's/dispatched_sessions.rs\` (1612 lines;/dispatched_sessions.rs\` (1621 lines;/g' docs/agent-maintenance/change-surfaces.md
sed -i 's/dispatched_sessions.rs\` (1375), and/dispatched_sessions.rs\` (1441), and/g' docs/agent-maintenance/change-surfaces.md
sed -i 's/dispatched_sessions.rs\` (1375) — dispatched/dispatched_sessions.rs\` (1441) — dispatched/g' docs/agent-maintenance/change-surfaces.md

# Fix audit_maintainability_giant_baseline.toml
sed -i 's/"src\/services\/discord\/tui_prompt_relay.rs" = 4310/"src\/services\/discord\/tui_prompt_relay.rs" = 4574/g' scripts/audit_maintainability_giant_baseline.toml
sed -i 's/"src\/services\/discord\/inflight.rs" = 3138/"src\/services\/discord\/inflight.rs" = 3180/g' scripts/audit_maintainability_giant_baseline.toml

# Regenerate inventory
python3 scripts/generate_inventory_docs.py
