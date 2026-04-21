use anyhow::{Context, Result};
use serde_json::json;
use std::thread;
use std::time::Duration;

use crate::config::{load_or_default_config, write_config};
use crate::maintainer;
use crate::state::{self, CaseEventRecord, CaseStatus};
use crate::telegram::{self, TelegramMessageUpdate};

pub(crate) fn run_daemon(once: bool, poll_interval_ms: u64, timeout: Duration) -> Result<()> {
    if once {
        let result = daemon_cycle(timeout)?;
        println!("{}", serde_json::to_string_pretty(&result)?);
        return Ok(());
    }

    loop {
        let result = daemon_cycle(timeout)?;
        println!("{}", serde_json::to_string(&result)?);
        thread::sleep(Duration::from_millis(poll_interval_ms));
    }
}

pub(crate) fn daemon_cycle(timeout: Duration) -> Result<serde_json::Value> {
    let telegram_commands = match process_telegram_commands(timeout) {
        Ok(result) => result,
        Err(error) => json!({
            "ok": false,
            "error": format!("{error:#}")
        }),
    };
    let maintainer_runs = maintainer::maintainer_cycle()?;
    let notifications = match flush_notifications(timeout) {
        Ok(result) => result,
        Err(error) => json!({
            "ok": false,
            "error": format!("{error:#}")
        }),
    };
    Ok(json!({
        "ok": true,
        "action": "daemon_cycle",
        "telegramCommands": telegram_commands,
        "maintainerRuns": maintainer_runs,
        "notifications": notifications
    }))
}

pub(crate) fn flush_notifications(timeout: Duration) -> Result<serde_json::Value> {
    let config = load_or_default_config()?;
    let telegram_config = config
        .telegram
        .as_ref()
        .context("Telegram chat is not configured. Run `tool-feedback telegram pair` first.")?;
    let bot_token = crate::config::resolve_telegram_bot_token()?;
    deliver_pending_notifications(&bot_token, &telegram_config.chat_id, timeout)
}

fn process_telegram_commands(timeout: Duration) -> Result<serde_json::Value> {
    let mut config = load_or_default_config()?;
    let Some(telegram_config) = config.telegram.clone() else {
        return Ok(json!({
            "ok": true,
            "action": "telegram_commands",
            "processed": 0,
            "ignored": 0,
            "reason": "telegram_not_configured"
        }));
    };
    let bot_token = crate::config::resolve_telegram_bot_token()?;
    let updates =
        telegram::telegram_get_updates(&bot_token, telegram_config.update_offset, 1, timeout)?;
    let next_offset = telegram::next_update_offset(&updates)?.or(telegram_config.update_offset);
    let messages = telegram::message_updates(&updates)?;

    let mut processed = Vec::new();
    let mut ignored = 0usize;
    for update in messages {
        if update.chat_id != telegram_config.chat_id {
            ignored += 1;
            continue;
        }
        match parse_inbound_command(&update.text) {
            ParsedTelegramCommand::Ignore => ignored += 1,
            ParsedTelegramCommand::Help(message) => {
                let reply = telegram::send_text(
                    &bot_token,
                    &update.chat_id,
                    &message,
                    Some(update.message_id),
                    timeout,
                )?;
                processed.push(json!({
                    "updateId": update.update_id,
                    "command": "help",
                    "replyMessageId": reply.pointer("/result/message_id")
                }));
            }
            ParsedTelegramCommand::ApprovePatch { case_id, note } => {
                processed.push(handle_approve_patch(
                    &bot_token,
                    &update,
                    &case_id,
                    note.as_deref(),
                    timeout,
                )?);
            }
        }
    }

    if let Some(ref mut telegram) = config.telegram {
        if telegram.update_offset != next_offset {
            telegram.update_offset = next_offset;
            write_config(&config)?;
        }
    }

    Ok(json!({
        "ok": true,
        "action": "telegram_commands",
        "processed": processed.len(),
        "ignored": ignored,
        "commands": processed,
        "nextOffset": next_offset
    }))
}

fn handle_approve_patch(
    bot_token: &str,
    update: &TelegramMessageUpdate,
    case_public_id: &str,
    note: Option<&str>,
    timeout: Duration,
) -> Result<serde_json::Value> {
    let reply_text = match approve_patch(case_public_id, note) {
        Ok(case_record) => {
            format!(
                "Patch approved for {case_id}.\nSummary: {summary}\nStatus: {status}\nThe daemon will start the patch run now.",
                case_id = case_record.public_id,
                summary = case_record.summary,
                status = case_record.status,
            )
        }
        Err(error) => format!("Could not approve patch for {case_public_id}: {error:#}"),
    };
    let reply = telegram::send_text(
        bot_token,
        &update.chat_id,
        &reply_text,
        Some(update.message_id),
        timeout,
    )?;
    Ok(json!({
        "updateId": update.update_id,
        "command": "approve",
        "caseId": case_public_id,
        "replyMessageId": reply.pointer("/result/message_id"),
        "ok": true
    }))
}

fn approve_patch(case_public_id: &str, note: Option<&str>) -> Result<state::CaseRecord> {
    let conn = state::open_db()?;
    let case_id = state::parse_case_id(case_public_id)?;
    let case_record = state::load_case(&conn, case_id)?.context("case not found")?;
    if case_record.status != CaseStatus::AwaitingApproval.as_str() {
        anyhow::bail!(
            "{case_public_id} is `{}` rather than `awaiting_approval`",
            case_record.status
        );
    }
    let message = note
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| format!("Patch approved from Telegram. {value}"))
        .unwrap_or_else(|| "Patch approved from Telegram.".to_string());
    state::transition_case(
        &conn,
        case_id,
        CaseStatus::PatchApproved,
        message,
        state::now_millis()?,
    )
}

enum ParsedTelegramCommand {
    Ignore,
    Help(String),
    ApprovePatch {
        case_id: String,
        note: Option<String>,
    },
}

fn parse_inbound_command(text: &str) -> ParsedTelegramCommand {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return ParsedTelegramCommand::Ignore;
    }
    let mut parts = trimmed.split_whitespace();
    let Some(raw_command) = parts.next() else {
        return ParsedTelegramCommand::Ignore;
    };
    let command = raw_command
        .trim_start_matches('/')
        .split('@')
        .next()
        .unwrap_or("")
        .to_ascii_lowercase();
    match command.as_str() {
        "approve" => {
            let Some(case_id) = parts.next() else {
                return ParsedTelegramCommand::Help(
                    "Usage: approve case_000123 optional note".to_string(),
                );
            };
            let remainder = parts.collect::<Vec<_>>().join(" ");
            ParsedTelegramCommand::ApprovePatch {
                case_id: case_id.to_string(),
                note: (!remainder.trim().is_empty()).then_some(remainder),
            }
        }
        "help" => ParsedTelegramCommand::Help(telegram_help_text()),
        _ => ParsedTelegramCommand::Ignore,
    }
}

fn telegram_help_text() -> String {
    "tool-feedback commands:\n- approve case_000123 optional note".to_string()
}

pub(crate) fn deliver_pending_notifications(
    bot_token: &str,
    chat_id: &str,
    timeout: Duration,
) -> Result<serde_json::Value> {
    let conn = state::open_db()?;
    let pending = state::pending_notification_events(&conn)?;
    let mut delivered = Vec::new();
    for event in pending {
        let text = format_case_event_notification(&event);
        let response = telegram::send_text(bot_token, chat_id, &text, None, timeout)?;
        state::record_event_delivery(&conn, event.id, "telegram", &response, state::now_millis()?)?;
        delivered.push(json!({
            "eventId": event.id,
            "caseId": event.case_public_id,
            "status": event.to_status,
        }));
    }
    Ok(json!({
        "ok": true,
        "action": "daemon_cycle",
        "delivered": delivered.len(),
        "events": delivered
    }))
}

pub(crate) fn format_case_event_notification(event: &CaseEventRecord) -> String {
    let status = event.to_status.as_deref().unwrap_or("updated");
    let approval_hint = if status == CaseStatus::AwaitingApproval.as_str() {
        format!(
            "\nAction: reply `approve {}` to start patch.",
            event.case_public_id
        )
    } else {
        String::new()
    };
    format!(
        "tool-feedback\n{tool}\n{case_id} {status}\nSummary: {summary}\nNote: {message}{approval_hint}",
        tool = event.tool,
        case_id = event.case_public_id,
        status = status,
        summary = event.case_summary,
        message = event.message,
        approval_hint = approval_hint
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn notification_format_is_scannable() {
        let event = CaseEventRecord {
            id: 1,
            case_id: 1,
            case_public_id: "case_000001".into(),
            tool: "codex-recall".into(),
            case_summary: "Search is noisy".into(),
            kind: "status_changed".into(),
            from_status: Some("accepted".into()),
            to_status: Some("awaiting_approval".into()),
            message: "Patch looks warranted in src/search.rs".into(),
            created_at: 1,
        };
        let text = format_case_event_notification(&event);
        assert!(text.contains("case_000001 awaiting_approval"));
        assert!(text.contains("Summary: Search is noisy"));
        assert!(text.contains("approve case_000001"));
    }

    #[test]
    fn parses_approve_command_and_help() {
        match parse_inbound_command("/approve case_000123 keep it small") {
            ParsedTelegramCommand::ApprovePatch { case_id, note } => {
                assert_eq!(case_id, "case_000123");
                assert_eq!(note.as_deref(), Some("keep it small"));
            }
            _ => panic!("expected approve"),
        }
        match parse_inbound_command("approve") {
            ParsedTelegramCommand::Help(message) => assert!(message.contains("Usage")),
            _ => panic!("expected help"),
        }
    }
}
