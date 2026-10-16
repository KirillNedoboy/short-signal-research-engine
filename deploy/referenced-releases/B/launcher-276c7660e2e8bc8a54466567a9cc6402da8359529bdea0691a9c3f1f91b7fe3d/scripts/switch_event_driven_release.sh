#!/usr/bin/env bash
# Atomically switch short-telegram-bot-lite.service to a prepared immutable
# release, restart once, and gate on READY.  The previous current target is
# recorded first; rollback restores it.  Never run both releases at once:
# the instance fence and single delivery/state ownership are authoritative.
#
# Usage:
#   switch_event_driven_release.sh <release-sha>    # switch + verify
#   switch_event_driven_release.sh --rollback <release-sha>
set -euo pipefail

ADMIN=/opt/short-telegram-bot-lite-admin
CURRENT_LINK=$ADMIN/current
SERVICE=short-telegram-bot-lite.service
STATE_DIR=$ADMIN/work/event-driven-live-scan-20260910/.switch-state
mkdir -p "$STATE_DIR"

verify_ready() {
  local want="$1"
  systemctl restart "$SERVICE"
  for _ in $(seq 1 24); do
    sleep 5
    if journalctl -u "$SERVICE" --since "-2 min" --no-pager -q 2>/dev/null | grep -q "Runtime READY"; then
      local path
      path=$(systemctl show "$SERVICE" -p ExecMainStartTimestamp --value)
      echo "READY confirmed (service started $path, release $want)"
      return 0
    fi
    if ! systemctl is-active --quiet "$SERVICE"; then
      return 1
    fi
  done
  return 1
}

case "${1:-}" in
  --rollback)
    SHA="${2:?usage: --rollback <release-sha>}"
    TARGET=$ADMIN/releases/$SHA
    [ -d "$TARGET" ] || { echo "rollback target missing: $TARGET" >&2; exit 1; }
    ln -sfn "$TARGET" "$CURRENT_LINK.tmp" && mv -Tf "$CURRENT_LINK.tmp" "$CURRENT_LINK"
    readlink -f "$CURRENT_LINK"
    verify_ready "$SHA" || { echo "rollback READY failed" >&2; exit 1; }
    echo "rolled back to $SHA"
    ;;
  "")
    echo "usage: switch_event_driven_release.sh <release-sha> | --rollback <release-sha>" >&2
    exit 2
    ;;
  *)
    SHA="$1"
    TARGET=$ADMIN/releases/$SHA
    [ -d "$TARGET" ] || { echo "release missing: $TARGET" >&2; exit 1; }
    PREVIOUS=$(readlink -f "$CURRENT_LINK")
    echo "$PREVIOUS" > "$STATE_DIR/previous-$SHA"
    ln -sfn "$TARGET" "$CURRENT_LINK.tmp" && mv -Tf "$CURRENT_LINK.tmp" "$CURRENT_LINK"
    echo "switched current -> $TARGET (previous: $PREVIOUS)"
    if ! verify_ready "$SHA"; then
      echo "READY gate FAILED; restoring previous release automatically" >&2
      "$0" --rollback "$(basename "$PREVIOUS")"
      exit 1
    fi
    systemctl show "$SERVICE" -p NRestarts -p MemoryCurrent --no-pager
    echo "switch complete: $SHA"
    ;;
esac
