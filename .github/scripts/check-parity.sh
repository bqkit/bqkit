#!/usr/bin/env bash
# Checks that every commit of the previous major branch is on this one: either the same patch, or a
# commit carrying its "(cherry picked from commit <sha>)" line, as written by git cherry-pick -x.
set -euo pipefail

head="${1:?usage: check-parity.sh <head ref> <previous major ref>}"
previous="${2:?usage: check-parity.sh <head ref> <previous major ref>}"

picked=$(git log --format=%B "$head" | sed -n 's/^(cherry picked from commit \([0-9a-f]\{40\}\))$/\1/p')
missing=$(git cherry "$head" "$previous" | sed -n 's/^+ //p' | grep -vxF -f <(printf '%s\n' "$picked") || true)

if [ -n "$missing" ]; then
  echo "Commits of $previous missing from $head (cherry-pick them with git cherry-pick -x):" >&2
  git log --no-walk=unsorted --format='  %h %s' $missing >&2
  exit 1
fi
echo "Every commit of $previous is on $head"
