## What & why

<!-- PRs are squash-merged: the PR title becomes the commit message. -->

## Checklist

- [ ] `make test` passes locally (`go test -race ./...`)
- [ ] New behavior has tests (no unconditional sleeps; goleak-clean)
- [ ] Spec updated if behavior changed (`spec/daemonSeed_spec.md`)
- [ ] No network I/O introduced (local-only, spec §15.4)
