---
name: Bug report
about: Something misbehaves (broker, MCP tools, TUI, scheduler, tracing)
labels: bug
---

**What happened**

**What you expected**

**How to reproduce**

1.
2.

**Environment**
- daemonseed version (`daemonseed version`):
- OS (macOS/Linux + version):
- How the instance was connected (parent/child MCP, observer, raw):

**Logs / trace**

If possible, attach relevant lines from:
- `daemonseed logs -n 50` (audit)
- `daemonseed trace -n 50` (session trace — payloads are already truncated)
- daemon stderr (`--log-format text` is easiest to read)
