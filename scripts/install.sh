#!/usr/bin/env bash
# Install daemonseed: build the binary, place it on PATH, and optionally
# wire up a repo's MCP config.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INSTALL_DIR="${INSTALL_DIR:-$HOME/.local/bin}"
VERSION="${VERSION:-1.0.0}"

echo "==> building daemonseed ${VERSION}"
cd "$REPO_ROOT"
mkdir -p "$INSTALL_DIR"
go build -ldflags="-X main.Version=${VERSION} -X main.Commit=$(git rev-parse --short HEAD 2>/dev/null || echo unknown)" \
  -o "$INSTALL_DIR/daemonseed" ./cmd/daemonseed

echo "==> installed $INSTALL_DIR/daemonseed"
if ! command -v daemonseed >/dev/null 2>&1; then
  echo "    NOTE: $INSTALL_DIR is not on your PATH. Add this to your shell profile:"
  echo "      export PATH=\"$INSTALL_DIR:\$PATH\""
fi

cat <<'EOF'

Next steps:
  1. Start the daemon:                 daemonseed start --background
  2. In your PARENT repo:              cp .mcp.json.parent.example /path/to/parent-repo/.mcp.json
  3. In each CHILD repo:               cp .mcp.json.child.example /path/to/child-repo/.mcp.json
     (give each child a unique --name in its .mcp.json)
  4. Optional slash commands:          daemonseed install-commands --repo-path /path/to/repo --role parent
  5. Watch the bus live:               daemonseed tui
EOF
