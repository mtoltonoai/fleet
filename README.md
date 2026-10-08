# fleet

A standalone, multi-repo **agent-fleet orchestrator**: general messaging + window management +
orchestration for a fleet of unattended AI agents that work across many git repositories.

Extracted from the cadenza repo (where it grew up embedded in `xtask`). Cadenza is now just one *target
repo* among many — the `perf-agent` working in `s2n-quic` proved the multi-repo thesis.

## Current architecture

The task board stores the board-native roster, charters, assignments, messages and lifecycle intent.
Fleet materializes workspaces, runs managed sessions and delivers event wakes. Repository-specific
build, test and merge policy belongs to each target repository's adapter.

- **Execution:** [main.rs](crates/fleet/src/main.rs) routes `spin-up`, `up-board` and `codex-session`.
  [session_host.rs](crates/fleet/src/session_host.rs) owns one local host per agent, persists the pending
  wake mailbox, runs one turn at a time and checks board lifecycle intent before starting a turn.
  [codex.rs](crates/fleet/src/codex.rs) handles the app-server protocol, cancellation, bounded logs and
  process cleanup. A completed turn records an attempt outcome; the assigned worker verifies and closes
  the business task separately.
- **Roster and worktrees:** [board.rs](crates/fleet/src/board.rs) provides the orchestrator's REST client.
  Board metadata supplies host, repositories, model, effort and interval. `spin-up` uses
  [workspace.rs](crates/fleet/src/workspace.rs) to create shared mirrors under `<root>/mirrors` and
  per-agent worktrees under `<root>/agents/<agent>/<repo>`. Agents coordinate through board MCP tools.
- **Notification and tunnel:** [notify.rs](crates/fleet/src/notify.rs) receives local board webhooks,
  filters events and sends control requests to the session host's private Unix socket. It acknowledges
  delivery after the host accepts the request. Board-issued delivery IDs support deduplication on
  retries. [fleet-tunnel](crates/fleet-tunnel/src/lib.rs) forwards requests over an outbound WebSocket
  connection when the board cannot reach the notifier directly. The board inbox remains the fallback
  for failed webhook delivery.
- **Watchdog:** `watchdog_board` in [main.rs](crates/fleet/src/main.rs) combines the host-filtered roster,
  heartbeat age, declared cadence and actionable task counts. Managed session status fences wakes while
  a turn is busy or its state is unknown. [drift.rs](crates/fleet/src/drift.rs) tracks repeated drift
  signals. Watchdog actions are controlled by their CLI flags and lifecycle checks.
- **Configuration and services:** [config.rs](crates/fleet/src/config.rs) loads the TOML configuration;
  [config.example.toml](config.example.toml) shows common settings. The [nix](nix) directory contains
  daemon and timer definitions. Legacy file-hub commands remain in the binary for existing deployments.

[DESIGN.md](DESIGN.md) preserves the extraction history and board-offload design. Its older rollout
phases and polling examples describe earlier implementations; use the source paths above for the
managed session flow.

## Cadence and event wakes

For a board-native agent, persist the fallback cadence in the agent's `metadata.interval` using the
board's `update_agent` tool. The session host refreshes this value during board synchronization.
Managed sessions accept positive integer seconds, or the suffixes `s`, `m`, `h` and `d`, up to 24 hours:
`90`, `90s`, `2m`, `3h`, `24h` and `1d` are valid. `1d` and `24h` both mean 86400 seconds. Invalid or
out-of-range values produce a diagnostic and retain the existing 30-minute fallback behavior.

Use a short cadence while assigned work is actionable. With no actionable tasks and a drained inbox,
use the charter's rest cadence and let board events wake the session. Event delivery is independent of
the fallback timer. A blocked task should record its dependency on the board; it does not justify
leaving other actionable assignments idle.

## Local development and validation

Start from a clean worktree or identify existing changes with `git status --short`. Keep changes within
the owning component and preserve the board's assignment and transaction semantics. The focused
session tests exercise local state and mocked processes without launching a live model:

```sh
cargo test -p fleet session_host::tests
```

The existing workspace CI checks are:

```sh
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo run -q -p fleet -- lint-prose \
  --ruleset crates/fleet/prose-style.toml \
  --dir crates/fleet --dir loops \
  --baseline crates/fleet/prose-style.baseline.json
```

The default `kb` build downloads ONNX Runtime. If that dependency is unavailable, adding `--exclude kb`
to the workspace clippy and test commands validates the remaining crates. Record that limitation
alongside the results; the full workspace gate still needs to pass in an environment with the dependency.

For changes to the CLI/session boundary, the smoke test substitutes a fake Codex executable and uses a
temporary runtime directory:

```sh
cargo build -p fleet
python3 crates/fleet/tests/codex_cli_smoke.py target/debug/fleet
```

The default workspace checks omit feature-gated daemon binaries. A tunnel transport change also needs
`cargo test -p fleet-tunnel --features transport`; other daemon feature checks are recorded in
[checks.yml](.github/workflows/checks.yml). Local tests and builds leave deployment to the operator's
normal review and release workflow.
