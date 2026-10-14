#!/usr/bin/env bash
# Verify only commits already reachable from the protected main snapshot.
set -euo pipefail

fail() { printf '::error::%s\n' "$1" >&2; exit 1; }
main_ref=refs/remotes/origin/main
main_sha=$(git rev-parse --verify "$main_ref^{commit}") \
  || fail 'Protected origin/main snapshot is unavailable.'
requested_ref=${REQUESTED_REF:-$main_sha}
[[ "$requested_ref" != -* ]] || fail 'Requested revision must not be an option.'
candidate=$(git rev-parse --verify --end-of-options "$requested_ref^{commit}" 2>/dev/null) \
  || candidate=$(git rev-parse --verify --end-of-options "refs/remotes/origin/$requested_ref^{commit}" 2>/dev/null) \
  || fail 'Requested revision does not resolve to a commit.'
[[ "$candidate" =~ ^[0-9a-f]{40}$ ]] || fail 'Requested revision is not a full commit identity.'
git merge-base --is-ancestor "$candidate" "$main_sha" \
  || fail 'Requested revision is outside protected main history.'

# No requested source executes before the ancestry check succeeds.
git checkout --detach "$candidate"
printf 'Trusted main revision: %s\n' "$candidate"
