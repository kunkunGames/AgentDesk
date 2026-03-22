#!/usr/bin/env bash
set -euo pipefail

ADK_DEV="$HOME/.adk/dev"
ADK_REL="$HOME/.adk/release"
PLIST_REL="com.agentdesk.release"

echo "═══ ADK Promote Dev → Release ═══"

# Safety check: review must be passed (unless --skip-review is passed)
if [[ "${1:-}" != "--skip-review" ]]; then
    # Check if the latest commit has a review-passed marker (may be in dev or release runtime)
    LAST_COMMIT=$(cd "$HOME/AgentDesk" && git rev-parse HEAD 2>/dev/null)
    REVIEW_MARKER_DEV="$ADK_DEV/runtime/review_passed/$LAST_COMMIT"
    REVIEW_MARKER_REL="$ADK_REL/runtime/review_passed/$LAST_COMMIT"
    if [ ! -f "$REVIEW_MARKER_DEV" ] && [ ! -f "$REVIEW_MARKER_REL" ]; then
        echo "✗ Review not passed for commit $LAST_COMMIT — aborting promotion"
        echo "  Run counter-review first, or use --skip-review to override"
        exit 1
    fi
    echo "▸ Review passed for $LAST_COMMIT"
fi

# Safety check: dev must be healthy
DEV_PORT="${AGENTDESK_DEV_PORT:-8791}"
if ! curl -s --max-time 5 "http://127.0.0.1:${DEV_PORT}/api/health" | grep -q '"status":"healthy"'; then
    echo "✗ Dev is not healthy — aborting promotion"
    exit 1
fi

echo "▸ Dev is healthy — proceeding"

# Ensure release dir exists
mkdir -p "$ADK_REL"/{bin,config,data,logs}

# Stop release
echo "▸ Stopping release..."
launchctl bootout "gui/$(id -u)/$PLIST_REL" 2>/dev/null || true
sleep 2

# Copy binary from dev
echo "▸ Copying binary from dev..."
cp "$ADK_DEV/bin/agentdesk" "$ADK_REL/bin/agentdesk"
chmod +x "$ADK_REL/bin/agentdesk"

# Copy dashboard from dev
echo "▸ Copying dashboard from dev..."
mkdir -p "$ADK_REL/dashboard"
rm -rf "$ADK_REL/dashboard/dist"
cp -r "$ADK_DEV/dashboard/dist" "$ADK_REL/dashboard/dist"

# Initialize release database if it doesn't exist (never overwrite release data)
if [ ! -f "$ADK_REL/data/agentdesk.sqlite" ]; then
    echo "▸ Initializing release database from dev..."
    cp "$ADK_DEV/data/agentdesk.sqlite" "$ADK_REL/data/agentdesk.sqlite"
else
    echo "▸ Release database exists — preserving release data (skip copy)"
fi

# Start release
echo "▸ Starting release..."
xattr -d com.apple.quarantine "$HOME/Library/LaunchAgents/$PLIST_REL.plist" 2>/dev/null || true
launchctl bootstrap "gui/$(id -u)" "$HOME/Library/LaunchAgents/$PLIST_REL.plist"
sleep 3

# Health check
REL_PORT="${AGENTDESK_REL_PORT:-8791}"
if curl -s --max-time 5 "http://127.0.0.1:${REL_PORT}/api/health" | grep -q '"status":"healthy"'; then
    echo "✓ Release is healthy on :${REL_PORT}"
else
    echo "✗ Release health check failed — check logs: $ADK_REL/logs/"
    exit 1
fi

echo "═══ Promotion Complete ═══"
