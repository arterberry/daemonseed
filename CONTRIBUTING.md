# Contributing to daemonSeed

Thanks for your interest! daemonSeed is a small, spec-driven project — the
bar for changes is "does it keep the system simple, local-only, and
accountable."

## Ground rules

1. **The spec is the source of truth.** `spec/daemonSeed_spec.md` describes
   the system. Behavior changes should update the spec in the same PR
   (extensions get a new §20.x section; ambiguity resolutions get a
   documented-decision note). Code that contradicts the spec is a bug in one
   of them.
2. **Local-only, forever.** No TCP listeners, no HTTP clients, no telemetry,
   no network of any kind (spec §15.4). PRs introducing network I/O will be
   declined regardless of how useful the feature is — that's a different
   project.
3. **Tests are not optional.** Every package has tests; new behavior needs
   them. The suite must pass `go test -race -count=1 ./...` with zero
   failures and zero goroutine leaks (the broker tests run under goleak).
   No unconditional sleeps in tests — synchronize with channels, deadlines,
   or condition polling.
4. **No silent failures.** Every error is returned, logged, or both
   (spec §16). `_ = someErr` doesn't pass review.

## Getting started

```bash
git clone https://github.com/arterberry/daemonseed
cd daemonseed
make build     # bin/daemonseed
make test      # go test -race ./...
```

Go 1.25+ (the toolchain directive in go.mod handles the rest). No other
dependencies — the TUI, SQLite trace backend, and everything else are pure
Go.

A quick manual smoke test:

```bash
DAEMONSEED_DAEMON_SOCKET_PATH=/tmp/dev.sock ./bin/daemonseed start --background
./bin/daemonseed status
./bin/daemonseed trace -n 20
./bin/daemonseed stop
```

## Pull requests

- Branch from `main`; PRs are squash-merged, so the PR title becomes the
  commit message — write it like one.
- CI (gofmt, vet, build, `go test -race` on Linux **and** macOS, binary-size
  budget) must be green.
- Keep PRs focused. A feature plus a drive-by refactor is two PRs.
- For anything beyond a small fix, open an issue first so we can agree on
  the shape — especially for new MCP tools or wire-protocol changes
  (envelope/message types are a compatibility surface).

## Where help is welcome

- Durable schedule/task storage via bbolt (spec §20.1 — the scheduler is
  in-memory today)
- Task result storage (`bus_get_result`, spec §20.2)
- Named pub/sub topics (spec §20.4)
- Swapping `internal/cron` for `robfig/cron` (kept in-house only because the
  original build environment was offline)
- TUI polish (filter dialog, schedule panel)
