#!/usr/bin/env bash
# Merge a pull request only when its required checks have passed.
#   scripts/merge-pr.sh <pr-number>
# INTENT: the repo is private on a free plan, so GitHub cannot mark checks as
# required. This script is the gate instead: use it rather than the merge
# button or a bare `gh pr merge`.
set -euo pipefail

pr="${1:?usage: scripts/merge-pr.sh <pr-number>}"
required=(compat-log build-2604-gcc16)

checks="$(gh pr checks "$pr" --json name,state --jq '.[] | "\(.name) \(.state)"')"
fail=0
for name in "${required[@]}"; do
    # A check can run twice (push and pull_request); every run must pass.
    state="$(printf '%s\n' "$checks" | awk -v n="$name" '
        $1 == n { seen = 1; if ($2 != "SUCCESS") bad = $2 }
        END { print (!seen ? "missing" : (bad ? bad : "SUCCESS")) }')"
    if [ "$state" != "SUCCESS" ]; then
        echo "merge-pr: check '$name' is $state on PR #$pr" >&2
        fail=1
    fi
done
if [ "$fail" -ne 0 ]; then
    echo "merge-pr: not merging" >&2
    exit 1
fi

gh pr merge "$pr" --rebase --delete-branch
