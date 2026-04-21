mod config;
mod daemon;
mod maintainer;
mod owners;
mod state;
mod telegram;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use config::{
    TelegramConfig, load_or_default_config, redacted_config_value, resolve_telegram_bot_token,
    write_config, write_telegram_bot_token,
};
use serde_json::json;
use std::time::Duration;

const NETWORK_TIMEOUT: Duration = Duration::from_secs(20);
const DEFAULT_DAEMON_POLL_INTERVAL_MS: u64 = 30_000;
const DEFAULT_CASE_EVENTS_LIMIT: usize = 10;

#[derive(Debug, Parser)]
#[command(name = "tool-feedback")]
#[command(about = "Local tool feedback runtime")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Telegram {
        #[command(subcommand)]
        command: TelegramCommand,
    },
    Notify {
        #[command(subcommand)]
        command: NotifyCommand,
    },
    Submit(SubmitArgs),
    Case {
        #[command(subcommand)]
        command: CaseCommand,
    },
    Owner {
        #[command(subcommand)]
        command: OwnerCommand,
    },
    Run {
        #[command(subcommand)]
        command: RunCommand,
    },
    Daemon {
        #[command(subcommand)]
        command: DaemonCommand,
    },
}

#[derive(Debug, Subcommand)]
enum TelegramCommand {
    Init,
    SetBotToken {
        token: String,
    },
    Pair {
        #[arg(long)]
        chat_id: Option<String>,
    },
    Doctor,
}

#[derive(Debug, Subcommand)]
enum NotifyCommand {
    Test {
        #[arg(long, default_value = "tool-feedback test notification")]
        text: String,
    },
}

#[derive(Debug, clap::Args)]
struct SubmitArgs {
    #[arg(long)]
    tool: String,
    #[arg(long)]
    summary: String,
    #[arg(long)]
    details: Option<String>,
    #[arg(long)]
    dedupe_key: Option<String>,
    #[arg(long)]
    source_thread_id: Option<String>,
    #[arg(long)]
    source_session_id: Option<String>,
    #[arg(long)]
    cwd: Option<String>,
    #[arg(long = "command")]
    command_text: Option<String>,
}

#[derive(Debug, Subcommand)]
enum CaseCommand {
    List {
        #[arg(long)]
        status: Option<String>,
    },
    Show {
        case_id: String,
        #[arg(long, default_value_t = DEFAULT_CASE_EVENTS_LIMIT)]
        events_limit: usize,
    },
    Accept(CaseTransitionArgs),
    ApprovePatch(CaseTransitionArgs),
    Block(CaseTransitionArgs),
    Complete(CaseTransitionArgs),
    Reject(CaseTransitionArgs),
    Defer(CaseTransitionArgs),
    Duplicate(CaseTransitionArgs),
    Note {
        case_id: String,
        #[arg(long)]
        message: String,
    },
}

#[derive(Debug, clap::Args)]
struct CaseTransitionArgs {
    case_id: String,
    #[arg(long)]
    message: String,
}

#[derive(Debug, Subcommand)]
enum DaemonCommand {
    Run {
        #[arg(long)]
        once: bool,
        #[arg(long, default_value_t = DEFAULT_DAEMON_POLL_INTERVAL_MS)]
        poll_interval_ms: u64,
    },
}

#[derive(Debug, Subcommand)]
enum OwnerCommand {
    Init {
        #[arg(long)]
        force: bool,
    },
    List,
    Show {
        tool: String,
    },
}

#[derive(Debug, Subcommand)]
enum RunCommand {
    List {
        #[arg(long)]
        status: Option<String>,
        #[arg(long)]
        case_id: Option<String>,
        #[arg(long)]
        kind: Option<String>,
    },
    Show {
        run_id: String,
    },
}

fn main() {
    if let Err(error) = run() {
        eprintln!("{error:#}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Telegram { command } => run_telegram(command),
        Command::Notify { command } => run_notify(command),
        Command::Submit(args) => run_submit(args),
        Command::Case { command } => run_case(command),
        Command::Owner { command } => run_owner(command),
        Command::Run { command } => run_run(command),
        Command::Daemon { command } => run_daemon_command(command),
    }
}

fn run_telegram(command: TelegramCommand) -> Result<()> {
    match command {
        TelegramCommand::Init => {
            let config = load_or_default_config()?;
            let path = write_config(&config)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "ok": true,
                    "action": "telegram_init",
                    "configPath": path.display().to_string(),
                    "config": redacted_config_value(&config)
                }))?
            );
            Ok(())
        }
        TelegramCommand::SetBotToken { token } => {
            let path = write_telegram_bot_token(&token)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "ok": true,
                    "action": "telegram_set_bot_token",
                    "tokenPath": path.display().to_string()
                }))?
            );
            Ok(())
        }
        TelegramCommand::Pair { chat_id } => {
            let bot_token = resolve_telegram_bot_token()?;
            let mut config = load_or_default_config()?;
            let updates = telegram::telegram_get_updates(&bot_token, None, 1, NETWORK_TIMEOUT)?;
            let chat_id = match chat_id {
                Some(chat_id) => chat_id,
                None => {
                    let chat = telegram::latest_private_chat(&updates)?
                        .context("no private chat found in Telegram updates. Open the bot and send /start first, or pass --chat-id explicitly")?;
                    chat.chat_id
                }
            };
            let update_offset = telegram::next_update_offset(&updates)?.or(config
                .telegram
                .as_ref()
                .and_then(|telegram| telegram.update_offset));
            config.telegram = Some(TelegramConfig {
                chat_id: chat_id.clone(),
                update_offset,
            });
            let path = write_config(&config)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "ok": true,
                    "action": "telegram_pair",
                    "configPath": path.display().to_string(),
                    "config": redacted_config_value(&config)
                }))?
            );
            Ok(())
        }
        TelegramCommand::Doctor => {
            let config = load_or_default_config()?;
            let bot_token = resolve_telegram_bot_token().ok();
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "ok": true,
                    "action": "telegram_doctor",
                    "config": redacted_config_value(&config),
                    "botToken": if bot_token.is_some() { "<configured>" } else { "<missing>" }
                }))?
            );
            Ok(())
        }
    }
}

fn run_notify(command: NotifyCommand) -> Result<()> {
    match command {
        NotifyCommand::Test { text } => {
            let config = load_or_default_config()?;
            let telegram = config.telegram.as_ref().context(
                "Telegram chat is not configured. Run `tool-feedback telegram pair` first.",
            )?;
            let bot_token = resolve_telegram_bot_token()?;
            telegram::send_text(&bot_token, &telegram.chat_id, &text, None, NETWORK_TIMEOUT)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "ok": true,
                    "action": "notify_test",
                    "chatId": telegram.chat_id
                }))?
            );
            Ok(())
        }
    }
}

fn run_submit(args: SubmitArgs) -> Result<()> {
    let conn = state::open_db()?;
    let outcome = state::submit_case(
        &conn,
        state::NewCaseInput {
            tool: args.tool,
            summary: args.summary,
            details: args.details,
            dedupe_key: args.dedupe_key,
            source_thread_id: args.source_thread_id,
            source_session_id: args.source_session_id,
            cwd: args.cwd,
            command_text: args.command_text,
        },
        state::now_millis()?,
    )?;
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "ok": true,
            "action": "case_submit",
            "created": outcome.created,
            "case": state::format_case_summary(&outcome.case_record)
        }))?
    );
    Ok(())
}

fn run_case(command: CaseCommand) -> Result<()> {
    match command {
        CaseCommand::List { status } => {
            let conn = state::open_db()?;
            let status_filter = status
                .as_deref()
                .map(state::CaseStatus::parse)
                .transpose()?;
            let cases = state::list_cases(&conn, status_filter)?;
            let items = cases
                .iter()
                .map(state::format_case_summary)
                .collect::<Vec<_>>();
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "ok": true,
                    "action": "case_list",
                    "count": items.len(),
                    "cases": items
                }))?
            );
            Ok(())
        }
        CaseCommand::Show {
            case_id,
            events_limit,
        } => {
            let conn = state::open_db()?;
            let id = state::parse_case_id(&case_id)?;
            let case_record = state::load_case(&conn, id)?.context("case not found")?;
            let events = state::recent_case_events(&conn, id, events_limit)?;
            let runs = state::list_maintainer_runs(&conn, None, Some(id), None)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "ok": true,
                    "action": "case_show",
                    "case": state::format_case_summary(&case_record),
                    "events": events,
                    "runs": runs
                }))?
            );
            Ok(())
        }
        CaseCommand::Accept(args) => {
            run_case_transition("case_accept", args, state::CaseStatus::Accepted)
        }
        CaseCommand::ApprovePatch(args) => {
            run_case_transition("case_approve_patch", args, state::CaseStatus::PatchApproved)
        }
        CaseCommand::Block(args) => {
            run_case_transition("case_block", args, state::CaseStatus::Blocked)
        }
        CaseCommand::Complete(args) => {
            run_case_transition("case_complete", args, state::CaseStatus::Completed)
        }
        CaseCommand::Reject(args) => {
            run_case_transition("case_reject", args, state::CaseStatus::Rejected)
        }
        CaseCommand::Defer(args) => {
            run_case_transition("case_defer", args, state::CaseStatus::Deferred)
        }
        CaseCommand::Duplicate(args) => {
            run_case_transition("case_duplicate", args, state::CaseStatus::Duplicate)
        }
        CaseCommand::Note { case_id, message } => {
            let conn = state::open_db()?;
            let id = state::parse_case_id(&case_id)?;
            let case_record = state::append_case_note(&conn, id, message, state::now_millis()?)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "ok": true,
                    "action": "case_note",
                    "case": state::format_case_summary(&case_record)
                }))?
            );
            Ok(())
        }
    }
}

fn run_case_transition(
    action: &str,
    args: CaseTransitionArgs,
    new_status: state::CaseStatus,
) -> Result<()> {
    let conn = state::open_db()?;
    let id = state::parse_case_id(&args.case_id)?;
    let case_record =
        state::transition_case(&conn, id, new_status, args.message, state::now_millis()?)?;
    let daemon_result = match daemon::daemon_cycle(NETWORK_TIMEOUT) {
        Ok(result) => result,
        Err(error) => json!({
            "ok": false,
            "error": format!("{error:#}")
        }),
    };
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "ok": true,
            "action": action,
            "case": state::format_case_summary(&case_record),
            "daemon": daemon_result
        }))?
    );
    Ok(())
}

fn run_owner(command: OwnerCommand) -> Result<()> {
    match command {
        OwnerCommand::Init { force } => {
            let path = owners::write_default_owner_registry(force)?;
            let registry = owners::load_owner_registry()?;
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "ok": true,
                    "action": "owner_init",
                    "path": path.display().to_string(),
                    "registry": owners::registry_json(&registry)
                }))?
            );
            Ok(())
        }
        OwnerCommand::List => {
            let registry = owners::load_or_bootstrap_owner_registry()?;
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "ok": true,
                    "action": "owner_list",
                    "registry": owners::registry_json(&registry)
                }))?
            );
            Ok(())
        }
        OwnerCommand::Show { tool } => {
            let registry = owners::load_or_bootstrap_owner_registry()?;
            let owner = owners::owner_for_tool(&registry, &tool)
                .with_context(|| format!("no owner configured for tool `{tool}`"))?;
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "ok": true,
                    "action": "owner_show",
                    "tool": tool,
                    "owner": owner
                }))?
            );
            Ok(())
        }
    }
}

fn run_run(command: RunCommand) -> Result<()> {
    match command {
        RunCommand::List {
            status,
            case_id,
            kind,
        } => {
            let conn = state::open_db()?;
            let status_filter = status
                .as_deref()
                .map(state::MaintainerRunStatus::parse)
                .transpose()?;
            let case_filter = case_id.as_deref().map(state::parse_case_id).transpose()?;
            let kind_filter = kind
                .as_deref()
                .map(state::MaintainerRunKind::parse)
                .transpose()?;
            let runs = state::list_maintainer_runs(&conn, status_filter, case_filter, kind_filter)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "ok": true,
                    "action": "run_list",
                    "count": runs.len(),
                    "runs": runs
                }))?
            );
            Ok(())
        }
        RunCommand::Show { run_id } => {
            let conn = state::open_db()?;
            let id = state::parse_run_id(&run_id)?;
            let run = state::load_maintainer_run(&conn, id)?.context("run not found")?;
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "ok": true,
                    "action": "run_show",
                    "run": run
                }))?
            );
            Ok(())
        }
    }
}

fn run_daemon_command(command: DaemonCommand) -> Result<()> {
    match command {
        DaemonCommand::Run {
            once,
            poll_interval_ms,
        } => daemon::run_daemon(once, poll_interval_ms, NETWORK_TIMEOUT),
    }
}

fn redact_secret_text(text: &str, secret: &str) -> String {
    if secret.is_empty() {
        return text.to_string();
    }
    text.replace(secret, "<redacted>")
}
