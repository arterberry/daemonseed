#!/usr/bin/env bash
# Uninstall daemonseed: stop the daemon if running and remove the binary.
# Config (~/.config/daemonseed) and the audit log (~/.local/share/daemonseed)
# are left in place unless --purge is passed.
set -euo pipefail

INSTALL_DIR="${INSTALL_DIR:-$HOME/.local/bin}"
BIN="$INSTALL_DIR/daemonseed"

if [ -x "$BIN" ]; then
  echo "==> stopping daemon (if running)"
  "$BIN" stop 2>/dev/null || true
  echo "==> removing $BIN"
  rm -f "$BIN"
else
  echo "daemonseed binary not found at $BIN; nothing to remove"
fi

if [ "${1:-}" = "--purge" ]; then
  echo "==> purging config and data"
  rm -rf "$HOME/.config/daemonseed" "$HOME/.local/share/daemonseed"
  rm -f /tmp/daemonseed.sock /tmp/daemonseed.pid
fi

echo "done."
