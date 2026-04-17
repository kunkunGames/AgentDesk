#!/bin/bash
# ──────────────────────────────────────────────────────────────────────────────
# deploy.sh — Build, install, and restart AgentDesk
#
# Steps:
#   1. Build release binary (+ dashboard)
#   2. Copy binary to ~/.adk/release/bin/
#   3. Install/update launchd plist (macOS) or systemd unit (Linux)
#   4. Restart service
#   5. Smoke test (health check)
#
# Usage:
#   ./scripts/deploy.sh [--skip-dashboard] [--skip-build] \
#     [--codesign-mode=auto|developer-id|adhoc|skip] \
#     [--codesign-identity="Developer ID Application: ..."]
# ──────────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# shellcheck source=_defaults.sh
. "$SCRIPT_DIR/_defaults.sh"

AD_HOME="${AGENTDESK_HOME:-$HOME/.adk/release}"
BIN_DIR="$AD_HOME/bin"
LIBEXEC_DIR="$AD_HOME/libexec"
WRAPPER_BIN="$BIN_DIR/agentdesk"
REAL_BIN="$LIBEXEC_DIR/agentdesk"
HEALTH_PORT="${AGENTDESK_SERVER_PORT:-$ADK_DEFAULT_PORT}"
LABEL="com.agentdesk.release"
OS=$(uname -s | tr '[:upper:]' '[:lower:]')

SKIP_BUILD=false
SKIP_DASHBOARD=false
CODESIGN_MODE="${AGENTDESK_CODESIGN_MODE:-auto}"
CODESIGN_IDENTITY="${AGENTDESK_CODESIGN_IDENTITY:-Developer ID Application: Wonchang Oh (A7LJY7HNGA)}"
CODESIGN_IDENTIFIER="${AGENTDESK_CODESIGN_IDENTIFIER:-com.itismyfield.agentdesk}"
RAW_CODESIGN_MODE="$CODESIGN_MODE"

for arg in "$@"; do
  case "$arg" in
    --skip-build)     SKIP_BUILD=true ;;
    --skip-dashboard) SKIP_DASHBOARD=true ;;
    --codesign-mode=*) CODESIGN_MODE="${arg#*=}"; RAW_CODESIGN_MODE="$CODESIGN_MODE" ;;
    --codesign-identity=*) CODESIGN_IDENTITY="${arg#*=}" ;;
  esac
done

info()  { printf "\033[1;34m[deploy]\033[0m %s\n" "$*"; }
ok()    { printf "\033[1;32m[deploy]\033[0m %s\n" "$*"; }
fail()  { printf "\033[1;31m[deploy]\033[0m %s\n" "$*"; exit 1; }

normalize_codesign_mode() {
  local raw_mode="${1:-}"
  raw_mode="$(printf '%s' "$raw_mode" | tr '[:upper:]' '[:lower:]')"
  case "$raw_mode" in
    auto|"")
      printf 'auto\n'
      ;;
    developer-id|developer_id|developerid|developer)
      printf 'developer-id\n'
      ;;
    adhoc|ad-hoc|ad_hoc)
      printf 'adhoc\n'
      ;;
    skip|none|preserve|existing)
      printf 'skip\n'
      ;;
    *)
      return 1
      ;;
  esac
}

codesign_identity_available() {
  local identity="$1"
  if [ "$OS" != "darwin" ] || [ -z "$identity" ]; then
    return 1
  fi

  security find-identity -v -p codesigning 2>/dev/null | grep -F -- "$identity" >/dev/null
}

resolve_macos_codesign_mode() {
  case "$CODESIGN_MODE" in
    developer-id|adhoc|skip)
      printf '%s\n' "$CODESIGN_MODE"
      ;;
    auto)
      if [ "$CODESIGN_IDENTITY" = "-" ]; then
        printf 'adhoc\n'
      elif codesign_identity_available "$CODESIGN_IDENTITY"; then
        printf 'developer-id\n'
      else
        printf 'adhoc\n'
      fi
      ;;
    *)
      return 1
      ;;
  esac
}

codesign_real_binary_if_needed() {
  local resolved_mode="$1"

  if [ "$OS" != "darwin" ]; then
    return 0
  fi

  case "$resolved_mode" in
    developer-id)
      [ -n "$CODESIGN_IDENTITY" ] || fail "Developer ID signing requested but no identity was provided"
      codesign_identity_available "$CODESIGN_IDENTITY" \
        || fail "Developer ID identity not found in keychain: $CODESIGN_IDENTITY"
      info "Signing $REAL_BIN with Developer ID identity"
      codesign \
        -s "$CODESIGN_IDENTITY" \
        --options runtime \
        --identifier "$CODESIGN_IDENTIFIER" \
        --force \
        "$REAL_BIN"
      codesign -v "$REAL_BIN" 2>/dev/null \
        || fail "Developer ID codesign verification failed — aborting"
      ;;
    adhoc)
      info "Signing $REAL_BIN with ad-hoc identity"
      codesign \
        -s - \
        --identifier "$CODESIGN_IDENTIFIER" \
        --force \
        "$REAL_BIN"
      codesign -v "$REAL_BIN" 2>/dev/null \
        || fail "Ad-hoc codesign verification failed — aborting"
      ;;
    skip)
      info "Skipping re-sign for $REAL_BIN; preserving existing signature state"
      ;;
    *)
      fail "Unsupported resolved codesign mode: $resolved_mode"
      ;;
  esac
}

if ! CODESIGN_MODE="$(normalize_codesign_mode "$CODESIGN_MODE")"; then
  fail "Unsupported --codesign-mode: $RAW_CODESIGN_MODE"
fi

if [ "$CODESIGN_IDENTITY" = "-" ] && [ "$CODESIGN_MODE" = "developer-id" ]; then
  fail "Developer ID mode cannot use '-' identity; use --codesign-mode=adhoc instead"
fi

BACKUP_WRAPPER=""
BACKUP_REAL=""

cleanup_backup() {
  if [ -n "${BACKUP_WRAPPER:-}" ] && [ -f "$BACKUP_WRAPPER" ]; then
    rm -f "$BACKUP_WRAPPER"
  fi
  if [ -n "${BACKUP_REAL:-}" ] && [ -f "$BACKUP_REAL" ]; then
    rm -f "$BACKUP_REAL"
  fi
}

trap cleanup_backup EXIT

print_recent_macos_binary_logs() {
  if [ "$OS" != "darwin" ]; then
    return
  fi

  local log_cmd="/usr/bin/log"
  if [ ! -x "$log_cmd" ]; then
    return
  fi

  echo "  Recent macOS policy logs for $BIN_DIR/agentdesk:"
  "$log_cmd" show --last 2m --style compact \
    --predicate "eventMessage CONTAINS[c] \"$WRAPPER_BIN\" OR process == \"agentdesk\"" \
    2>/dev/null | tail -n 20 || true
}

write_wrapper_script() {
  local tmp_wrapper
  tmp_wrapper="$(mktemp "$WRAPPER_BIN.new.XXXXXX")"
  cat > "$tmp_wrapper" <<EOF
#!/bin/bash
exec "$REAL_BIN" "\$@"
EOF
  chmod +x "$tmp_wrapper"
  mv -f "$tmp_wrapper" "$WRAPPER_BIN"
}

install_file_atomically() {
  local src="$1"
  local dest="$2"
  local mode="${3:-755}"
  local tmp_dest

  tmp_dest="$(mktemp "$dest.new.XXXXXX")"
  cp "$src" "$tmp_dest"
  chmod "$mode" "$tmp_dest"
  mv -f "$tmp_dest" "$dest"
}

restore_previous_install() {
  if [ -n "${BACKUP_WRAPPER:-}" ] && [ -f "$BACKUP_WRAPPER" ]; then
    install_file_atomically "$BACKUP_WRAPPER" "$WRAPPER_BIN" 755
  else
    rm -f "$WRAPPER_BIN"
  fi

  if [ -n "${BACKUP_REAL:-}" ] && [ -f "$BACKUP_REAL" ]; then
    install_file_atomically "$BACKUP_REAL" "$REAL_BIN" 755
  else
    rm -f "$REAL_BIN"
  fi
}

run_installed_binary_self_check() {
  local stdout_file stderr_file version_line exit_code
  stdout_file="$(mktemp)"
  stderr_file="$(mktemp)"

  if "$WRAPPER_BIN" --version >"$stdout_file" 2>"$stderr_file"; then
    version_line="$(head -n 1 "$stdout_file" | tr -d '\r')"
    rm -f "$stdout_file" "$stderr_file"
    if [ -n "$version_line" ]; then
      ok "Installed binary self-check passed: $version_line"
    else
      ok "Installed binary self-check passed: --version executed successfully"
    fi
    return 0
  else
    exit_code=$?
  fi

  echo "  Installed binary self-check failed (exit $exit_code)"
  if [ -s "$stdout_file" ]; then
    echo "  stdout:"
    sed 's/^/    /' "$stdout_file"
  fi
  if [ -s "$stderr_file" ]; then
    echo "  stderr:"
    sed 's/^/    /' "$stderr_file"
  fi
  rm -f "$stdout_file" "$stderr_file"
  print_recent_macos_binary_logs

  restore_previous_install
  ok "Restored previous install after failed self-check"

  fail "Installed binary self-check failed for $WRAPPER_BIN"
}

# ── Step 1: Build ─────────────────────────────────────────────────────────────
if [ "$SKIP_BUILD" = true ]; then
  info "Build skipped (--skip-build)"
  if [ ! -f "$PROJECT_DIR/target/release/agentdesk" ]; then
    fail "No existing binary at target/release/agentdesk — cannot skip build"
  fi
else
  info "Building release..."
  BUILD_ARGS=()
  if [ "$SKIP_DASHBOARD" = true ]; then
    BUILD_ARGS+=("--skip-dashboard")
  fi
  if [ ${#BUILD_ARGS[@]} -gt 0 ]; then
    "$SCRIPT_DIR/build-release.sh" "${BUILD_ARGS[@]}"
  else
    "$SCRIPT_DIR/build-release.sh"
  fi
fi

# ── Step 2: Copy binary ──────────────────────────────────────────────────────
info "Installing binary..."
mkdir -p "$BIN_DIR"
if [ "$OS" = "darwin" ]; then
  # Previous installs may have been immutable; unlock before backup/replace.
  chflags nouchg "$WRAPPER_BIN" "$REAL_BIN" 2>/dev/null || true
fi
mkdir -p "$LIBEXEC_DIR"
if [ -e "$WRAPPER_BIN" ]; then
  BACKUP_WRAPPER="$(mktemp "$BIN_DIR/agentdesk.wrapper.backup.XXXXXX")"
  cp "$WRAPPER_BIN" "$BACKUP_WRAPPER"
fi
if [ -e "$REAL_BIN" ]; then
  BACKUP_REAL="$(mktemp "$LIBEXEC_DIR/agentdesk.real.backup.XXXXXX")"
  cp "$REAL_BIN" "$BACKUP_REAL"
fi
install_file_atomically "$PROJECT_DIR/target/release/agentdesk" "$REAL_BIN" 755
if [ "$OS" = "darwin" ]; then
  RESOLVED_CODESIGN_MODE="$(resolve_macos_codesign_mode)" \
    || fail "Could not resolve macOS codesign mode from: $CODESIGN_MODE"
  info "Resolved macOS codesign mode: $RESOLVED_CODESIGN_MODE"
  codesign_real_binary_if_needed "$RESOLVED_CODESIGN_MODE"
fi
write_wrapper_script
ok "Binary wrapper: $WRAPPER_BIN -> $REAL_BIN"
run_installed_binary_self_check
rm -f "$BIN_DIR/agentdesk-real"

# Build and copy dashboard dist
if [ -d "$PROJECT_DIR/dashboard" ]; then
  echo "▸ Building dashboard..."
  (cd "$PROJECT_DIR/dashboard" && npm run build --silent)
fi
if [ -d "$PROJECT_DIR/dashboard/dist" ]; then
  mkdir -p "$AD_HOME/dashboard"
  rsync -a --delete "$PROJECT_DIR/dashboard/dist/" "$AD_HOME/dashboard/dist/"
  ok "Dashboard: $AD_HOME/dashboard/dist/"
fi

if [ -d "$PROJECT_DIR/policies" ]; then
  mkdir -p "$AD_HOME/policies"
  rsync -a --delete "$PROJECT_DIR/policies/" "$AD_HOME/policies/"
  ok "Policies: $AD_HOME/policies/"
fi

if [ -d "$PROJECT_DIR/skills" ]; then
  mkdir -p "$AD_HOME/skills"
  rsync -a --delete "$PROJECT_DIR/skills/" "$AD_HOME/skills/"
  ok "Managed skills: $AD_HOME/skills/"
fi

# ── Step 3: Install/update service ────────────────────────────────────────────
install_launchd() {
  local PLIST_SRC="$SCRIPT_DIR/com.agentdesk.release.plist"
  local PLIST_DST="$HOME/Library/LaunchAgents/com.agentdesk.release.plist"
  local LAUNCHD_ENV_FILE="$AD_HOME/config/launchd.env"

  # Migrate: remove legacy com.agentdesk plist if present
  local LEGACY_PLIST="$HOME/Library/LaunchAgents/com.agentdesk.plist"
  if [ -f "$LEGACY_PLIST" ]; then
    launchctl bootout "gui/$(id -u)/com.agentdesk" 2>/dev/null || true
    rm -f "$LEGACY_PLIST"
    info "Removed legacy plist: $LEGACY_PLIST"
  fi

  if [ ! -f "$PLIST_SRC" ]; then
    fail "Plist template not found: $PLIST_SRC"
  fi

  mkdir -p "$HOME/Library/LaunchAgents"
  mkdir -p "$AD_HOME/logs"

  # Replace placeholders with actual paths
  sed \
    -e "s|AGENTDESK_BIN|$BIN_DIR/agentdesk|g" \
    -e "s|AGENTDESK_HOME|$AD_HOME|g" \
    "$PLIST_SRC" > "$PLIST_DST"

  if [ -f "$LAUNCHD_ENV_FILE" ]; then
    sync_launchd_plist_environment_from_file "$PLIST_DST" "$LAUNCHD_ENV_FILE"
    ok "Applied local launchd env: $LAUNCHD_ENV_FILE"
  fi

  ok "Plist installed: $PLIST_DST"
}

install_systemd() {
  local UNIT_SRC="$SCRIPT_DIR/agentdesk-dcserver.service"
  local UNIT_DIR="$HOME/.config/systemd/user"
  local UNIT_DST="$UNIT_DIR/agentdesk-dcserver.service"

  if [ ! -f "$UNIT_SRC" ]; then
    fail "Systemd unit template not found: $UNIT_SRC"
  fi

  # Migrate: disable and remove legacy agentdesk.service if present
  local LEGACY_UNIT="$UNIT_DIR/agentdesk.service"
  if [ -f "$LEGACY_UNIT" ]; then
    systemctl --user disable --now agentdesk.service 2>/dev/null || true
    rm -f "$LEGACY_UNIT"
    info "Removed legacy unit: $LEGACY_UNIT"
  fi

  mkdir -p "$UNIT_DIR"
  mkdir -p "$AD_HOME/logs"
  cp "$UNIT_SRC" "$UNIT_DST"

  systemctl --user daemon-reload
  systemctl --user enable agentdesk-dcserver.service

  ok "Systemd unit installed: $UNIT_DST"
}

case "$OS" in
  darwin) install_launchd ;;
  linux)  install_systemd ;;
  *)      info "Unknown OS ($OS) — skipping service install" ;;
esac

# ── Step 4: Restart service ───────────────────────────────────────────────────
info "Restarting service..."

restart_launchd() {
  local PLIST="$HOME/Library/LaunchAgents/com.agentdesk.release.plist"
  local attempt max_attempts=5
  if [ ! -f "$PLIST" ]; then
    info "Plist not installed — skipping restart"
    return
  fi

  # Unload (ignore errors if not loaded)
  launchctl bootout "gui/$(id -u)/$LABEL" 2>/dev/null || true
  sleep 1

  # Load with retry because launchd can briefly report
  # "operation already in progress" immediately after bootout.
  for attempt in $(seq 1 "$max_attempts"); do
    if launchctl bootstrap "gui/$(id -u)" "$PLIST" >/dev/null 2>&1; then
      _kickstart_launchd_job_if_needed "$LABEL" || true
      ok "Service restarted via launchd"
      return
    fi

    info "  launchd bootstrap attempt $attempt/$max_attempts failed — retrying"
    sleep 1
  done

  # Surface the real launchctl error on the final attempt.
  launchctl bootstrap "gui/$(id -u)" "$PLIST"
  ok "Service restarted via launchd"
}

restart_systemd() {
  systemctl --user restart agentdesk-dcserver.service
  ok "Service restarted via systemd"
}

case "$OS" in
  darwin) restart_launchd ;;
  linux)  restart_systemd ;;
  *)      info "Restart manually" ;;
esac

# ── Step 5: Smoke test ────────────────────────────────────────────────────────
info "Waiting for health check (port $HEALTH_PORT)..."
if wait_for_http_service_health "$LABEL" "$HEALTH_PORT" 10 2 0 1; then
  ok "Health check passed on :$HEALTH_PORT/api/health"
else
  fail "Health check failed after waiting for :$HEALTH_PORT/api/health. Check logs:"
  echo "  $AD_HOME/logs/dcserver.stdout.log"
  echo "  $AD_HOME/logs/dcserver.stderr.log"
fi

echo ""
ok "Deploy complete."
