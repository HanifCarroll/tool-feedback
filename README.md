# tool-feedback

Local Rust runtime for routing tool-improvement feedback, running tool-specific Codex triage and patch jobs, and sending status updates through Telegram.

## Current scope

The v1 feedback loop is wired and verified:

- stores the Telegram bot token in a local secret file
- stores the paired Telegram chat id and Telegram update offset in local config
- stores cases, case events, and maintainer runs in a local SQLite database
- dedupes repeated submissions against still-open cases
- bootstraps a local tool-owner registry with separate triage and patch settings
- runs `triage` first for accepted cases
- sends Telegram when triage closes a case or returns `awaiting_approval`
- starts `patch` only after `case approve-patch ...` or a Telegram reply like `approve case_000123`
- records structured maintainer results, files changed, and tests run
- keeps run artifacts under `~/.codex/tool-feedback/runs/`

State lives under `~/.codex/tool-feedback/`.

## Commands

Run a full local health check:

```bash
cargo run -- doctor
```

Install the current binary into `~/.local/bin/tool-feedback` so agents can call it directly:

```bash
cargo run -- install
```

You can override the destination:

```bash
cargo run -- install --dest /custom/bin/tool-feedback
```

Initialize local config:

```bash
cargo run -- telegram init
```

Store the bot token:

```bash
cargo run -- telegram set-bot-token <token>
```

Pair the bot to the current Telegram chat after sending `/start`:

```bash
cargo run -- telegram pair
```

Inspect local notifier status:

```bash
cargo run -- telegram doctor
```

Send a test notification:

```bash
cargo run -- notify test --text "tool-feedback notifier wired successfully"
```

Refresh the default owner registry after upgrading to the triage/patch flow:

```bash
cargo run -- owner init --force
```

Submit a new tool-feedback case:

```bash
cargo run -- submit \
  --tool codex-recall \
  --summary "Search returns noisy snippets" \
  --details "Phrase search is matching nested command echoes instead of the user's actual ask."
```

List and inspect cases:

```bash
cargo run -- case list
cargo run -- case list --status awaiting_approval
cargo run -- case show case_000001 --events-limit 20
```

Transition a case. Every transition runs one daemon cycle immediately, so `case accept` can start triage and `case approve-patch` can start patching right away:

```bash
cargo run -- case accept case_000001 --message "Accepted for triage."
cargo run -- case approve-patch case_000001 --message "Approved for patch."
cargo run -- case block case_000001 --message "Waiting on reproduction details."
cargo run -- case complete case_000001 --message "Patched and verified."
cargo run -- case reject case_000001 --message "Existing flag already covers this."
cargo run -- case defer case_000001 --message "Valid idea, but not for v1."
cargo run -- case duplicate case_000001 --message "Duplicate of case_000004."
cargo run -- case note case_000001 --message "Need to inspect the originating transcript with codex-recall."
```

Telegram approval syntax from the paired chat:

```text
approve case_000001
approve case_000001 keep the fix small and repo-local
```

Inspect the owner registry:

```bash
cargo run -- owner list
cargo run -- owner show codex-recall
```

Inspect maintainer runs:

```bash
cargo run -- run list
cargo run -- run list --kind triage
cargo run -- run list --kind patch
cargo run -- run list --status running
cargo run -- run list --case-id case_000001
cargo run -- run show run_000001
```

Run the daemon once or continuously. Each cycle polls Telegram approvals, queues eligible triage/patch jobs, executes the next run per tool, and then flushes pending Telegram notifications:

```bash
cargo run -- daemon run --once
cargo run -- daemon run
```

## Local files

- config: `~/.codex/tool-feedback/config.json`
- bot token: `~/.codex/tool-feedback/telegram-bot-token`
- state db: `~/.codex/tool-feedback/state.db`
- owner registry: `~/.codex/tool-feedback/tool-owners.toml`
- run artifacts: `~/.codex/tool-feedback/runs/<tool>-<case_id>-<run_kind>-<timestamp>/`

Secret and config files are written with user-only permissions on macOS and Linux.

## Launchd

Build a release binary first if you want launchd to run the optimized executable:

```bash
cargo build --release
./target/release/tool-feedback install
```

Render a launchd plist without installing it:

```bash
tool-feedback launchd render \
  --label com.tool-feedback \
  --repo /path/to/tool-feedback \
  --bin "$(command -v tool-feedback)" \
  --output ~/Library/LaunchAgents/com.tool-feedback.plist
```

Install and bootstrap the LaunchAgent directly:

```bash
tool-feedback launchd install \
  --label com.tool-feedback \
  --repo /path/to/tool-feedback \
  --bin "$(command -v tool-feedback)"
```

Use `--no-bootstrap` if you only want the plist written under `~/Library/LaunchAgents/`.

The daemon still needs an explicit developer `PATH` in launchd so background `codex exec` workers can find Node, Bun, and Cargo tools. `tool-feedback launchd render` and `tool-feedback launchd install` fill that in automatically unless you override `--path`.
