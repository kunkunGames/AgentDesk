#!/usr/bin/env bash
# Smoke test for #1447 — agentdesk-restart silent-fail regression.
#
# Verifies:
#   1. scripts/_defaults.sh defines all required restart-drain helpers.
#   2. assert_restart_helpers_loaded returns 0 when helpers are present.
#   3. assert_restart_helpers_loaded returns 1 when a helper is missing.
#   4. A representative caller pattern (`if ! helper_call; then exit 1; fi`)
#      propagates exit 1 — never silently exits 0 — when the helper:
#        a) fails (non-zero return)
#        b) is undefined (command not found)
#
# This test is self-contained; it does not call launchctl or any real service.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DEFAULTS_SH="$REPO_ROOT/scripts/_defaults.sh"

PASS=0
FAIL=0
FAIL_NAMES=()

pass() {
  echo "  PASS: $1"
  PASS=$((PASS + 1))
}

fail() {
  echo "  FAIL: $1" >&2
  FAIL=$((FAIL + 1))
  FAIL_NAMES+=("$1")
}

assert_eq() {
  local label="$1" expected="$2" actual="$3"
  if [ "$expected" = "$actual" ]; then
    pass "$label (= $expected)"
  else
    fail "$label (expected=$expected actual=$actual)"
  fi
}

echo "== Test 1: _defaults.sh defines required helpers =="
[ -f "$DEFAULTS_SH" ] || { echo "FATAL: $DEFAULTS_SH missing"; exit 2; }

# shellcheck source=/dev/null
. "$DEFAULTS_SH"

for fn in \
  request_restart_drain_mode_or_fail \
  wait_for_live_turns_to_drain_or_fail \
  clear_restart_drain_mode \
  assert_restart_helpers_loaded; do
  if declare -F "$fn" >/dev/null 2>&1; then
    pass "function defined: $fn"
  else
    fail "function defined: $fn"
  fi
done

echo "== Test 2: assert_restart_helpers_loaded passes when helpers present =="
if assert_restart_helpers_loaded >/dev/null 2>&1; then
  pass "assert_restart_helpers_loaded returns 0"
else
  fail "assert_restart_helpers_loaded returns 0"
fi

echo "== Test 3: assert_restart_helpers_loaded fails when a helper is missing =="
# Run in a subshell so we can unset a function locally.
set +e
(
  set -e
  . "$DEFAULTS_SH"
  unset -f request_restart_drain_mode_or_fail
  assert_restart_helpers_loaded >/dev/null 2>&1
)
rc=$?
set -e
assert_eq "assert returns 1 with one helper missing" "1" "$rc"

echo "== Test 4a: caller exits 1 when helper returns non-zero =="
set +e
bash -c '
  set -euo pipefail
  failing_helper() { return 1; }
  if ! failing_helper; then
    exit 1
  fi
  exit 0
'
rc=$?
set -e
assert_eq "if ! failing_helper; then exit 1" "1" "$rc"

echo "== Test 4b: caller exits 1 when helper is undefined (command not found) =="
set +e
bash -c '
  set -euo pipefail
  if ! request_restart_drain_mode_or_fail dev label 0 /tmp src 2>/dev/null; then
    exit 1
  fi
  exit 0
'
rc=$?
set -e
assert_eq "if ! undefined_function; then exit 1" "1" "$rc"

echo "== Test 4c: caller using assert preflight exits 1 cleanly when helper missing =="
# This is the recommended pattern that protects against the silent-fail bug.
set +e
bash -c '
  set -euo pipefail
  . "'"$DEFAULTS_SH"'"
  unset -f wait_for_live_turns_to_drain_or_fail
  if ! assert_restart_helpers_loaded 2>/dev/null; then
    exit 1
  fi
  exit 0
'
rc=$?
set -e
assert_eq "preflight assert blocks restart with EXIT 1" "1" "$rc"

echo "== Test 5: clear_restart_drain_mode removes marker file =="
TMPDIR_TEST=$(mktemp -d)
trap 'rm -rf "$TMPDIR_TEST"' EXIT
touch "$TMPDIR_TEST/restart_pending"
. "$DEFAULTS_SH"
clear_restart_drain_mode "$TMPDIR_TEST" >/dev/null 2>&1 || true
if [ ! -e "$TMPDIR_TEST/restart_pending" ]; then
  pass "marker removed"
else
  fail "marker removed"
fi

echo
echo "==== Results ===="
echo "  PASS: $PASS"
echo "  FAIL: $FAIL"
if [ "$FAIL" -gt 0 ]; then
  printf '  failed: %s\n' "${FAIL_NAMES[@]}" >&2
  exit 1
fi
exit 0
