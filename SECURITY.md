# Security Policy

## Threat model

daemonSeed is **local-only by design**: the broker binds a `0600` Unix
domain socket, and the codebase intentionally contains no TCP/HTTP client or
listener (spec §15). The security boundary is the operating system's file
permissions — only the socket owner's processes can connect. There is no
authentication layer in v1.x because there is no remote surface.

Things that are *in scope* as vulnerabilities:

- Anything that lets a different OS user interact with the daemon
- Broker crashes or hangs triggered by crafted socket input (malformed
  frames, oversized messages, protocol abuse) — the broker must never panic
  on external input
- Role-enforcement bypasses (a child performing parent-only operations)
- The inbox hook executing a parent-supplied slash command that is **not**
  in `commands.allow_from_parent`
- Sensitive payload content leaking into logs when `audit.log_payloads` is
  false (trace snippets are truncated by design, but truncation is not
  redaction — report anything that defeats the intent)

## Reporting

Please report vulnerabilities privately via **GitHub → Security →
Report a vulnerability** (private vulnerability reporting) rather than a
public issue. You should get a response within a week.

## Supported versions

Only the latest release / `main` is supported.
