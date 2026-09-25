#!/usr/bin/env bash
# Run the production verification script with offline npm/build prerequisites.
set -euo pipefail

repo="$(cd "$(dirname "$0")/.." && pwd)"
tmp_parent="$(cd "${TMPDIR:-/tmp}" && pwd -P)"
sandbox="$(mktemp -d "$tmp_parent/agentdesk-dashboard-audit.XXXXXX")"
cleanup() {
  case "$sandbox" in
    "$tmp_parent"/agentdesk-dashboard-audit.*) rm -rf -- "$sandbox" ;;
    *) echo "unexpected test directory: $sandbox" >&2; return 1 ;;
  esac
}
trap cleanup EXIT
mkdir -p "$sandbox/scripts" "$sandbox/dashboard" "$sandbox/bin"
cp "$repo/scripts/verify-dashboard.sh" "$sandbox/scripts/verify-dashboard.sh"
printf '#!/usr/bin/env bash\nexit 0\n' > "$sandbox/scripts/check-dashboard-toolchain.sh"
cp "$sandbox/scripts/check-dashboard-toolchain.sh" "$sandbox/scripts/install-dashboard-dependencies.sh"
cat > "$sandbox/bin/npm" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >> "$AUDIT_TEST_LOG"
if [ "${1:-}" = audit ]; then exit "$AUDIT_TEST_STATUS"; fi
case "$*" in
  'run build' | test) exit 0 ;;
  *) echo "unexpected npm command: $*" >&2; exit 99 ;;
esac
SH
chmod +x "$sandbox/bin/npm"

run_case() {
  local label="$1" audit_status="$2" waiver="$3" expected="$4"
  local status=0 log="$sandbox/$label.calls" output="$sandbox/$label.output"
  : > "$log"
  PATH="$sandbox/bin:$PATH" AUDIT_TEST_LOG="$log" AUDIT_TEST_STATUS="$audit_status" \
    DASHBOARD_AUDIT_WAIVER="$waiver" bash "$sandbox/scripts/verify-dashboard.sh" > "$output" 2>&1 || status=$?
  if [ "$status" -ne "$expected" ]; then
    cat "$output" >&2
    echo "$label: expected exit $expected, got $status" >&2
    exit 1
  fi
  if [ "$expected" -eq 0 ]; then
    printf 'audit --audit-level=high\nrun build\ntest\n' > "$sandbox/expected.calls"
  else
    printf 'audit --audit-level=high\n' > "$sandbox/expected.calls"
  fi
  diff -u "$sandbox/expected.calls" "$log"
  case "$label" in
    stale) grep -q 'Remove the stale waiver' "$output" ;;
    accepted) grep -q 'findings WAIVED' "$output" ;;
  esac
  echo "PASS $label"
}

run_case clean 0 '' 0
run_case stale 0 'previous advisory' 1
run_case rejected 1 '' 1
run_case accepted 1 'unfixed advisory with an operator-approved exception' 0
