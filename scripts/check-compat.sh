#!/usr/bin/env bash
# Compatibility check: a change to a contract path (.github/compat-paths.txt)
# must be recorded in docs/compat-log.md, or explicitly waived with a line
# "Compat: none (<reason>)" in the PR body (env PR_BODY) or a commit message
# in the range. Run by CI on pull requests; runnable locally:
#   scripts/check-compat.sh origin/main HEAD
set -euo pipefail

base="${1:-origin/main}"
head="${2:-HEAD}"
root="$(git rev-parse --show-toplevel)"
paths_file="$root/.github/compat-paths.txt"
log="docs/compat-log.md"

patterns="$(grep -vE '^\s*(#|$)' "$paths_file")"
changed="$(git diff --name-only "$base...$head")"
hits="$(printf '%s\n' "$changed" | grep -E -f <(printf '%s\n' "$patterns") || true)"

if [ -z "$hits" ]; then
    echo "compat: no contract paths changed"
    exit 0
fi
if printf '%s\n' "$changed" | grep -qx "$log"; then
    echo "compat: contract paths changed and $log updated"
    printf '  %s\n' $hits
    exit 0
fi
waiver_re='^[[:space:]]*Compat:[[:space:]]*none'
if printf '%s\n' "${PR_BODY:-}" | grep -qiE "$waiver_re" ||
   git log --format=%B "$base..$head" | grep -qiE "$waiver_re"; then
    echo "compat: contract paths changed, waived with 'Compat: none'"
    printf '  %s\n' $hits
    exit 0
fi
echo "compat: these changes can affect existing publishers, apps or configs:" >&2
printf '  %s\n' $hits >&2
echo "Add an entry to $log, or state 'Compat: none (<reason>)' in the PR body or a commit message." >&2
exit 1
