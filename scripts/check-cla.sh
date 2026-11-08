#!/usr/bin/env bash
# Check that a pull request's author has signed the CLA: a row for their
# GitHub handle in CLA-signatures.md, as the PR's head has it.
#   scripts/check-cla.sh <github-login>
# INTENT: the row is the signature (CLA.md, "How to sign"), so the check reads
# the file from the PR itself; a first-time contributor signs in the same PR.
set -euo pipefail

login="${1:?usage: scripts/check-cla.sh <github-login>}"
# The maintainer and bots do not sign.
case "$login" in
    satyan-g|dependabot\[bot\]|github-actions\[bot\]) echo "cla: $login is exempt"; exit 0 ;;
esac
if grep -i -q -E "^\|[[:space:]]*@?${login}[[:space:]]*\|" CLA-signatures.md; then
    echo "cla: $login has signed"
    exit 0
fi
echo "cla: $login has not signed the CLA. Add your row to CLA-signatures.md in this pull request (see CLA.md, \"How to sign\")." >&2
exit 1
