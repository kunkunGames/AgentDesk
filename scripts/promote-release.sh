#!/usr/bin/env bash
set -euo pipefail

ADK_DEV="$HOME/.adk/dev"
ADK_REL="$HOME/.adk/release"
PLIST_REL="com.agentdesk.release"

echo "═══ ADK Promote Dev → Release ═══"

# Safety check: dev must be healthy
if ! curl -s --max-time 5 http://127.0.0.1:8799/api/health | grep -q '"ok":true'; then
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
launchctl bootstrap "gui/$(id -u)" "$HOME/Library/LaunchAgents/$PLIST_REL.plist"
sleep 3

# Health check
if curl -s --max-time 5 http://127.0.0.1:8791/api/health | grep -q '"ok":true'; then
    echo "✓ Release is healthy on :8791"
else
    echo "✗ Release health check failed — check logs: $ADK_REL/logs/"
    exit 1
fi

echo "═══ Promotion Complete ═══"
