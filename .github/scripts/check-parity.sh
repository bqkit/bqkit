#!/usr/bin/env bash
# Checks that every commit of the previous major branch is on this one: either the same patch, or a
# commit carrying its "(cherry picked from commit <sha>)" line, as written by git cherry-pick -x.
# A commit of the previous major that was itself cherry-picked from this one counts as present.
# A commit that does not apply to this major is marked as handled by an empty commit carrying the line:
#   git commit --allow-empty -m "Skip <subject> on N.x" -m "(cherry picked from commit <sha>)"
set -euo pipefail

head="${1:?usage: check-parity.sh <head ref> <previous major ref>}"
previous="${2:?usage: check-parity.sh <head ref> <previous major ref>}"

for ref in "$head" "$previous"; do
  git rev-parse --verify --quiet "$ref^{commit}" >/dev/null || { echo "Unknown ref: $ref" >&2; exit 1; }
done

picked_from() { sed -n 's/^(cherry picked from commit \([0-9a-f]\{40\}\))$/\1/p'; }

picked=$(git log --format=%B "$head" | picked_from)
missing=$(git cherry "$head" "$previous" | sed -n 's/^+ //p' | grep -vxF -f <(printf '%s\n' "$picked") || true)
missing=$(for commit in $missing; do
  source=$(git log -1 --format=%B "$commit" | picked_from | tail -n 1)
  [ -n "$source" ] && git merge-base --is-ancestor "$source" "$head" 2>/dev/null || echo "$commit"
done)

if [ -n "$missing" ]; then
  echo "Commits of $previous missing from $head (cherry-pick them with git cherry-pick -x," \
    "or mark one that does not apply with an empty commit, see the top of this script):" >&2
  git log --no-walk=unsorted --format='  %h %s' $missing >&2
  exit 1
fi
echo "Every commit of $previous is on $head"
