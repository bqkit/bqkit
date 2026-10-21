#!/usr/bin/env bash
# Builds the documentation site: docs/ of each release/N.x branch under N.x/,
# versions.json listing the majors, and a root page redirecting to the latest released one.
# The second argument is a file listing the tags of the published GitHub releases, one per line.
set -euo pipefail

usage="usage: build-pages.sh <output directory> <published release tags file>"
out="${1:?$usage}"
published="${2:?$usage}"
[ -f "$published" ] || { echo "No such file: $published" >&2; exit 1; }
rm -rf "$out"
mkdir -p "$out"

majors=$(git for-each-ref --format='%(refname:lstrip=4)' 'refs/remotes/origin/release/' \
  | { grep -E '^[0-9]+\.x$' || true; } | sort -t. -k1,1n)
[ -n "$majors" ] || { echo "No release/N.x branch found on origin" >&2; exit 1; }

built=""
for major in $majors; do
  git cat-file -e "origin/release/$major:docs" 2>/dev/null \
    || { echo "release/$major has no docs/, skipped" >&2; continue; }
  built="$built $major"
  mkdir -p "$out/$major"
  git archive "origin/release/$major" docs | tar -x -C "$out/$major" --strip-components=1
  page="$out/$major/index.html"
  sed "s/@MAJOR@/$major/g" "$page" > "$page.tmp" && mv "$page.tmp" "$page"
done
majors=$(echo $built | tr ' ' '\n')
[ -n "$majors" ] || { echo "No release/N.x branch has docs/" >&2; exit 1; }

# The root opens the latest major with a published final release (vN.x.y, no pre-release suffix), or
# the oldest major before the first one.
released=$(for major in $majors; do
  grep -qE "^v${major%.x}\.[0-9]+\.[0-9]+$" "$published" && echo "$major"
done || true)
latest=$( [ -n "$released" ] && echo "$released" | tail -n 1 || echo "$majors" | head -n 1)
printf '%s\n' $majors | sed 's/.*/"&"/' | paste -sd, - | sed 's/.*/[&]/' > "$out/versions.json"

cat > "$out/index.html" <<HTML
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>bqkit</title>
<meta http-equiv="refresh" content="0; url=$latest/">
<script>location.replace("$latest/" + location.hash);</script>
</head>
<body><a href="$latest/">bqkit documentation</a></body>
</html>
HTML
touch "$out/.nojekyll"
