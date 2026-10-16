#!/usr/bin/env bash
# Build an immutable event-driven canary release from the candidate work tree.
#
# The active release layout is releases/<64-hex content SHA> with symlinks
# .venv -> shared venv release and data -> /opt/short-telegram-bot-lite/data,
# plus release-manifest.json (schema_version 1).  No committed builder covered
# that layout, so this script packages the candidate deterministically and
# injects the provided config.yaml; it never edits an existing release.
#
# Usage: package_event_driven_release.sh <candidate-dir> <config-src> <note>
set -euo pipefail

ADMIN=/opt/short-telegram-bot-lite-admin
CAND="$1"
CONFIG_SRC="$2"
NOTE="${3:-canary}"
[ -f "$CONFIG_SRC" ] || { echo "config source missing: $CONFIG_SRC" >&2; exit 2; }
VENV_TARGET=$(readlink "$CAND/.venv")
DATA_TARGET=$(readlink "$CAND/data")

STAGE=$(mktemp -d "$ADMIN/work/.pkg-XXXXXX")
trap 'rm -rf "$STAGE"' EXIT

rsync -a --exclude '.venv' --exclude 'data' --exclude '.env' \
  --exclude 'release-manifest.json' --exclude 'artifacts' \
  --exclude 'canary-configs' --exclude '.switch-state' \
  "$CAND/" "$STAGE/tree/"
# Byte-code caches and editor state must never enter release identity.
find "$STAGE/tree" \( -name '__pycache__' -o -name '.pytest_cache' \
  -o -name '.ruff_cache' -o -name '*.pyc' \) -prune -exec rm -rf {} +

# Production linkage is explicit and identical for every canary release.
ln -s "$VENV_TARGET" "$STAGE/tree/.venv"
ln -s "$DATA_TARGET" "$STAGE/tree/data"
cp "$CAND/.env" "$STAGE/tree/.env"
chmod 600 "$STAGE/tree/.env"
# The deployment convention puts the provider mode in .env (env overrides
# config.yaml in load_config); keep it consistent with the packaged config so
# the release cannot silently boot back into REST_ONLY.
MODE=$(grep -E '^canonical_market_data_provider_mode:' "$CONFIG_SRC" | awk '{print $2}' | tr -d '[:space:]')
if [ -n "$MODE" ]; then
  if grep -q '^CANONICAL_MARKET_DATA_PROVIDER_MODE=' "$STAGE/tree/.env"; then
    sed -i "s/^CANONICAL_MARKET_DATA_PROVIDER_MODE=.*/CANONICAL_MARKET_DATA_PROVIDER_MODE=$MODE/" "$STAGE/tree/.env"
  else
    printf 'CANONICAL_MARKET_DATA_PROVIDER_MODE=%s\n' "$MODE" >> "$STAGE/tree/.env"
  fi
fi

# config.yaml is part of the release identity so distinct canary
# configurations always produce distinct immutable directories; .env and
# the manifest never enter the hash.
cp "$CONFIG_SRC" "$STAGE/tree/config.yaml"
chmod 644 "$STAGE/tree/config.yaml"

# Hash from inside the tree with ./-relative paths so any later rehash from
# the release directory reproduces the exact same digest.
SHA=$(cd "$STAGE/tree" && find . -type f \
  ! -name '.env' ! -name 'release-manifest.json' \
  -exec sha256sum {} + | sort -k2 | sha256sum | cut -d' ' -f1)
RELEASE_DIR="$ADMIN/releases/$SHA"
if [ -e "$RELEASE_DIR" ]; then
  echo "refusing to overwrite existing immutable release $RELEASE_DIR" >&2
  exit 1
fi
mv "$STAGE/tree" "$RELEASE_DIR"
"$RELEASE_DIR/.venv/bin/python" "$RELEASE_DIR/scripts/write_release_manifest.py" \
  --code-sha "$SHA" --output "$RELEASE_DIR/release-manifest.json"
chmod 600 "$RELEASE_DIR/release-manifest.json"
echo "RELEASE=$RELEASE_DIR"
echo "SHA=$SHA"
echo "NOTE=$NOTE"
