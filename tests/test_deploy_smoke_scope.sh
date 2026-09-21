#!/usr/bin/env bash
# Exercise the production coordinator with fake probes; never deploy or send.
# Extracted production functions consume these fixture globals through eval.
# shellcheck disable=SC2034
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
eval "$(awk '/^_run_post_deploy_functional_smoke\(\) \{$/ {copy=1} copy {print} copy && /^}$/ {exit}' "$root/scripts/deploy-release.sh")"
ADK_REL="$scratch/runtime"
POST_DEPLOY_SMOKE_STAMP=fixture
POST_DEPLOY_SMOKE_EVIDENCE="$scratch/evidence"
REL_PORT=1
relay_calls=0
durable_calls=0
api_calls=0
probe_fail=0
_post_deploy_smoke_wedge_reset() { :; }
_post_deploy_smoke_note() { printf '%s\n' "$*" >> "$POST_DEPLOY_SMOKE_EVIDENCE"; }
_post_deploy_smoke_wait_for_startup_recovery() { :; }
_post_deploy_smoke_check_wedges() { :; }
_post_deploy_smoke_check_fail_closed_warn_rate() { :; }
_post_deploy_smoke_probe_apis() {
    api_calls=$((api_calls + 1))
    POST_DEPLOY_SMOKE_READY=true
    return "$probe_fail"
}
_post_deploy_smoke_check_relay_round_trip() { relay_calls=$((relay_calls + 1)); }
_post_deploy_smoke_check_durable_record() { durable_calls=$((durable_calls + 1)); }
POST_DEPLOY_SMOKE_SCOPE=api
_run_post_deploy_functional_smoke
test "$api_calls:$relay_calls:$durable_calls" = 1:0:0
test "$POST_DEPLOY_SMOKE_DURABLE_COVERAGE" = 'not evaluated: operator selected API smoke scope'
probe_fail=1
if _run_post_deploy_functional_smoke; then
    echo 'API failure was hidden' >&2
    exit 1
fi
test "$api_calls:$relay_calls:$durable_calls" = 2:0:0
probe_fail=0
POST_DEPLOY_SMOKE_SCOPE=full
_run_post_deploy_functional_smoke
test "$api_calls:$relay_calls:$durable_calls" = 3:1:1
unset POST_DEPLOY_SMOKE_SCOPE
_run_post_deploy_functional_smoke
test "$api_calls:$relay_calls:$durable_calls" = 4:2:2
echo 'PASS: API scope preserves API failures and full/default retain external probes'
