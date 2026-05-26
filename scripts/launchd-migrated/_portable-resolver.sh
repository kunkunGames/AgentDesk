#!/bin/bash
# Shared path resolver for migrated launchd helper scripts.
#
# These scripts are packaged as release helpers, so they must not bake in an
# operator's home directory. Existing operators can preserve old paths by
# exporting the env vars below from launchd.env or their routine environment.

agentdesk_source_portable_resolver() {
  if [[ -z "${HOME:-}" ]]; then
    HOME="$(cd ~ && pwd)"
    export HOME
  fi

  export AGENTDESK_ROOT_DIR="${AGENTDESK_ROOT_DIR:-$HOME/.adk/release}"
  export AGENTDESK_WORKSPACE_ROOT="${AGENTDESK_WORKSPACE_ROOT:-$AGENTDESK_ROOT_DIR/workspaces}"
  export AGENTDESK_MIGRATED_ENTRYPOINT_DIR="${AGENTDESK_MIGRATED_ENTRYPOINT_DIR:-$AGENTDESK_ROOT_DIR/scripts/launchd-migrated}"
  export AGENTDESK_OPERATOR_WORKDIR="${AGENTDESK_OPERATOR_WORKDIR:-$HOME}"
  export AGENTDESK_MIGRATED_AGENTFACTORY_WORKDIR="${AGENTDESK_MIGRATED_AGENTFACTORY_WORKDIR:-$AGENTDESK_WORKSPACE_ROOT/agentfactory}"

  export OBSIDIAN_VAULT_ROOT="${OBSIDIAN_VAULT_ROOT:-$HOME/ObsidianVault}"
  export OBSIDIAN_REMOTE_VAULT_ROOT="${OBSIDIAN_REMOTE_VAULT_ROOT:-$OBSIDIAN_VAULT_ROOT/RemoteVault}"
  export AGENTDESK_OBSIDIAN_AGENTS_SRC="${AGENTDESK_OBSIDIAN_AGENTS_SRC:-$OBSIDIAN_REMOTE_VAULT_ROOT/adk-config/agents}"
  export AGENTDESK_OBSIDIAN_SKILL_ROOT="${AGENTDESK_OBSIDIAN_SKILL_ROOT:-$OBSIDIAN_REMOTE_VAULT_ROOT/99_Skills}"
  export AGENTDESK_AGENT_FEEDBACK_INBOX="${AGENTDESK_AGENT_FEEDBACK_INBOX:-$OBSIDIAN_REMOTE_VAULT_ROOT/agents/ch-pmd/inbox}"

  local bin_dir="${AGENTDESK_OPERATOR_BIN_DIR:-$HOME/bin}"
  case ":$PATH:" in
    *":$bin_dir:"*) ;;
    *) export PATH="$bin_dir:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:$PATH" ;;
  esac

  export LANG="${LANG:-ko_KR.UTF-8}"
  export LC_ALL="${LC_ALL:-ko_KR.UTF-8}"

  # shellcheck source=/dev/null
  if [[ "${AGENTDESK_SOURCE_ZPROFILE:-1}" != "0" && -f "$HOME/.zprofile" ]]; then
    source "$HOME/.zprofile"
  fi
}

agentdesk_obsidian_skill_path() {
  printf '%s/%s/SKILL.md' "$AGENTDESK_OBSIDIAN_SKILL_ROOT" "$1"
}

agentdesk_optional_file_or_skip() {
  local label="$1"
  local path="$2"
  if [[ ! -f "$path" ]]; then
    echo "optional connector skipped: $label file not found: $path" >&2
    exit 0
  fi
}

agentdesk_optional_dir_or_skip() {
  local label="$1"
  local path="$2"
  if [[ ! -d "$path" ]]; then
    echo "optional connector skipped: $label directory not found: $path" >&2
    exit 0
  fi
}
