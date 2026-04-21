use anyhow::{Context, Result, anyhow, bail};
use serde_json::{Value, json};
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TelegramPrivateChat {
    pub(crate) chat_id: String,
    pub(crate) username: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TelegramMessageUpdate {
    pub(crate) update_id: i64,
    pub(crate) message_id: i64,
    pub(crate) chat_id: String,
    pub(crate) username: Option<String>,
    pub(crate) text: String,
}

pub(crate) fn telegram_api_post(
    bot_token: &str,
    method: &str,
    payload: &Value,
    timeout: Duration,
) -> Result<Value> {
    let agent = ureq::AgentBuilder::new().timeout(timeout).build();
    let url = format!(
        "https://api.telegram.org/bot{}/{}",
        bot_token.trim(),
        method.trim()
    );
    let response = agent
        .post(&url)
        .send_json(payload.clone())
        .map_err(|error| {
            anyhow!(
                "telegram API {method} request failed: {}",
                crate::redact_secret_text(&error.to_string(), bot_token)
            )
        })?;
    let value: Value = response
        .into_json()
        .with_context(|| format!("telegram API {method} returned invalid JSON"))?;
    if value.get("ok").and_then(Value::as_bool) != Some(true) {
        bail!(
            "telegram API {method} returned error: {}",
            crate::redact_secret_text(&value.to_string(), bot_token)
        );
    }
    Ok(value)
}

pub(crate) fn telegram_get_updates(
    bot_token: &str,
    offset: Option<i64>,
    timeout_seconds: u64,
    timeout: Duration,
) -> Result<Value> {
    let mut payload = json!({
        "timeout": timeout_seconds,
        "allowed_updates": ["message"]
    });
    if let Some(offset) = offset {
        payload["offset"] = json!(offset);
    }
    telegram_api_post(bot_token, "getUpdates", &payload, timeout)
}

pub(crate) fn latest_private_chat(updates: &Value) -> Result<Option<TelegramPrivateChat>> {
    let result = updates_array(updates)?;
    let maybe_chat = result.iter().rev().find_map(|update| {
        let message = update.get("message")?;
        if message.pointer("/chat/type").and_then(Value::as_str) != Some("private") {
            return None;
        }
        let chat_id = message.pointer("/chat/id").and_then(value_as_string)?;
        let username = message
            .pointer("/from/username")
            .and_then(Value::as_str)
            .map(str::to_string);
        Some(TelegramPrivateChat { chat_id, username })
    });
    Ok(maybe_chat)
}

pub(crate) fn next_update_offset(updates: &Value) -> Result<Option<i64>> {
    let result = updates_array(updates)?;
    Ok(result
        .iter()
        .filter_map(|update| update.get("update_id").and_then(Value::as_i64))
        .max()
        .map(|value| value + 1))
}

pub(crate) fn message_updates(updates: &Value) -> Result<Vec<TelegramMessageUpdate>> {
    let result = updates_array(updates)?;
    let mut items = Vec::new();
    for update in result {
        let Some(message) = update.get("message") else {
            continue;
        };
        let Some(text) = message.get("text").and_then(Value::as_str) else {
            continue;
        };
        let Some(chat_id) = message.pointer("/chat/id").and_then(value_as_string) else {
            continue;
        };
        let Some(update_id) = update.get("update_id").and_then(Value::as_i64) else {
            continue;
        };
        let Some(message_id) = message.get("message_id").and_then(Value::as_i64) else {
            continue;
        };
        let username = message
            .pointer("/from/username")
            .and_then(Value::as_str)
            .map(str::to_string);
        items.push(TelegramMessageUpdate {
            update_id,
            message_id,
            chat_id,
            username,
            text: text.trim().to_string(),
        });
    }
    items.sort_by_key(|item| item.update_id);
    Ok(items)
}

pub(crate) fn send_text(
    bot_token: &str,
    chat_id: &str,
    text: &str,
    reply_to_message_id: Option<i64>,
    timeout: Duration,
) -> Result<Value> {
    let mut payload = json!({
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": true
    });
    if let Some(reply_to_message_id) = reply_to_message_id {
        payload["reply_parameters"] = json!({
            "message_id": reply_to_message_id
        });
    }
    telegram_api_post(bot_token, "sendMessage", &payload, timeout)
}

fn updates_array(updates: &Value) -> Result<&Vec<Value>> {
    updates
        .get("result")
        .and_then(Value::as_array)
        .context("telegram getUpdates response did not contain a result array")
}

fn value_as_string(value: &Value) -> Option<String> {
    value
        .as_i64()
        .map(|id| id.to_string())
        .or_else(|| value.as_str().map(str::to_string))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn extracts_latest_private_chat() {
        let updates = json!({
            "ok": true,
            "result": [
                {
                    "update_id": 1,
                    "message": {
                        "chat": { "id": -1001, "type": "supergroup" },
                        "from": { "username": "groupuser" }
                    }
                },
                {
                    "update_id": 2,
                    "message": {
                        "chat": { "id": 4242, "type": "private" },
                        "from": { "username": "exampleuser" }
                    }
                }
            ]
        });
        let chat = latest_private_chat(&updates)
            .expect("parse updates")
            .expect("private chat");
        assert_eq!(chat.chat_id, "4242");
        assert_eq!(chat.username.as_deref(), Some("exampleuser"));
    }

    #[test]
    fn parses_message_updates_and_next_offset() {
        let updates = json!({
            "ok": true,
            "result": [
                {
                    "update_id": 41,
                    "message": {
                        "message_id": 7,
                        "chat": { "id": 4242, "type": "private" },
                        "text": "approve case_000001",
                        "from": { "username": "exampleuser" }
                    }
                },
                {
                    "update_id": 42,
                    "message": {
                        "message_id": 8,
                        "chat": { "id": 4242, "type": "private" }
                    }
                }
            ]
        });
        let items = message_updates(&updates).expect("message updates");
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].text, "approve case_000001");
        assert_eq!(next_update_offset(&updates).expect("offset"), Some(43));
    }
}
