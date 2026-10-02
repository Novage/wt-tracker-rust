#!/usr/bin/env bash
# Fails when code changed but docs/SPEC.md did not (see AGENTS.md).
#
# Compares against a base ref (default: main), including uncommitted and untracked files:
#   scripts/check-spec.sh [base-ref]
# Set SPEC_UNCHANGED_OK=1 to accept a change that needs no spec update.
set -euo pipefail
cd "$(dirname "$0")/.."

base="${1:-main}"
spec="docs/SPEC.md"
code_pattern='^(crates/|bench/js/|bench/compare\.ts|bench/run\.sh|difftest/|loadtest/|Cargo\.toml)'

changed=$(
  {
    if git rev-parse --verify --quiet HEAD >/dev/null; then
      if git rev-parse --verify --quiet "$base" >/dev/null; then
        git diff --name-only "$base"...HEAD
      fi
      git diff --name-only HEAD
    else
      # No commits yet: everything staged or modified counts as changed.
      git diff --name-only --cached
      git diff --name-only
    fi
    git ls-files --others --exclude-standard
  } | sort -u
)

code_changes=$(grep -E "$code_pattern" <<<"$changed" || true)
if [[ -z "$code_changes" ]]; then
  echo "check-spec: no code changes"
  exit 0
fi

if grep -qx "$spec" <<<"$changed"; then
  echo "check-spec: ok ($spec updated)"
  exit 0
fi

if [[ "${SPEC_UNCHANGED_OK:-}" == "1" ]]; then
  echo "check-spec: code changed without a spec update (accepted via SPEC_UNCHANGED_OK)"
  exit 0
fi

echo "check-spec: code changed but $spec did not:" >&2
sed 's/^/  /' <<<"$code_changes" >&2
echo "Update $spec (see AGENTS.md), or rerun with SPEC_UNCHANGED_OK=1 if no spec change is needed." >&2
exit 1
