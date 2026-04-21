use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::env;
use std::fs;
use std::path::PathBuf;
#[cfg(test)]
use std::sync::{Mutex, OnceLock};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AppConfig {
    pub(crate) version: u32,
    #[serde(default)]
    pub(crate) telegram: Option<TelegramConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TelegramConfig {
    pub(crate) chat_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) update_offset: Option<i64>,
}

pub(crate) fn state_dir_path() -> PathBuf {
    resolve_codex_home().join("tool-feedback")
}

pub(crate) fn home_dir_path() -> PathBuf {
    resolve_home_dir()
}

pub(crate) fn local_bin_dir_path() -> PathBuf {
    home_dir_path().join(".local/bin")
}

pub(crate) fn launch_agents_dir_path() -> PathBuf {
    home_dir_path().join("Library/LaunchAgents")
}

pub(crate) fn config_path() -> PathBuf {
    state_dir_path().join("config.json")
}

pub(crate) fn telegram_bot_token_path() -> PathBuf {
    state_dir_path().join("telegram-bot-token")
}

pub(crate) fn tool_owners_path() -> PathBuf {
    state_dir_path().join("tool-owners.toml")
}

pub(crate) fn runs_dir_path() -> PathBuf {
    state_dir_path().join("runs")
}

pub(crate) fn ensure_state_dir() -> Result<PathBuf> {
    let path = state_dir_path();
    fs::create_dir_all(&path)
        .with_context(|| format!("failed to create state dir at {}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700))
            .with_context(|| format!("failed to set permissions on {}", path.display()))?;
    }
    Ok(path)
}

pub(crate) fn ensure_runs_dir() -> Result<PathBuf> {
    let path = runs_dir_path();
    fs::create_dir_all(&path)
        .with_context(|| format!("failed to create runs dir at {}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700))
            .with_context(|| format!("failed to set permissions on {}", path.display()))?;
    }
    Ok(path)
}

pub(crate) fn write_config(config: &AppConfig) -> Result<PathBuf> {
    ensure_state_dir()?;
    let path = config_path();
    fs::write(&path, serde_json::to_vec_pretty(config)?)
        .with_context(|| format!("failed to write config to {}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600))
            .with_context(|| format!("failed to set permissions on {}", path.display()))?;
    }
    Ok(path)
}

pub(crate) fn load_config() -> Result<Option<AppConfig>> {
    let path = config_path();
    if !path.exists() {
        return Ok(None);
    }
    let raw = fs::read_to_string(&path)
        .with_context(|| format!("failed to read config at {}", path.display()))?;
    let config: AppConfig = serde_json::from_str(&raw)
        .with_context(|| format!("failed to parse config at {}", path.display()))?;
    Ok(Some(config))
}

pub(crate) fn load_or_default_config() -> Result<AppConfig> {
    Ok(load_config()?.unwrap_or(AppConfig {
        version: 1,
        telegram: None,
    }))
}

pub(crate) fn redacted_config_value(config: &AppConfig) -> Value {
    json!({
        "version": config.version,
        "telegram": config.telegram.as_ref().map(|telegram| json!({
            "chatId": telegram.chat_id,
            "updateOffset": telegram.update_offset
        })),
        "telegramBotToken": if telegram_bot_token_path().exists() { "<configured>" } else { "<missing>" }
    })
}

pub(crate) fn write_telegram_bot_token(secret: &str) -> Result<PathBuf> {
    if secret.trim().is_empty() {
        bail!("telegram bot token cannot be empty");
    }
    ensure_state_dir()?;
    let path = telegram_bot_token_path();
    fs::write(&path, format!("{}\n", secret.trim()))
        .with_context(|| format!("failed to write telegram bot token to {}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600))
            .with_context(|| format!("failed to set permissions on {}", path.display()))?;
    }
    Ok(path)
}

pub(crate) fn resolve_telegram_bot_token() -> Result<String> {
    if let Ok(token) = env::var("TOOL_FEEDBACK_TELEGRAM_BOT_TOKEN") {
        if !token.trim().is_empty() {
            return Ok(token.trim().to_string());
        }
    }
    let path = telegram_bot_token_path();
    let token = fs::read_to_string(&path).with_context(|| {
        format!(
            "telegram bot token is not configured. Populate {} or set TOOL_FEEDBACK_TELEGRAM_BOT_TOKEN",
            path.display()
        )
    })?;
    let token = token.trim().to_string();
    if token.is_empty() {
        bail!("telegram bot token file at {} is empty", path.display());
    }
    Ok(token)
}

fn resolve_codex_home() -> PathBuf {
    env::var_os("CODEX_HOME")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .or_else(|| env::var_os("HOME").map(|home| PathBuf::from(home).join(".codex")))
        .unwrap_or_else(|| {
            env::current_dir()
                .map(|cwd| cwd.join(".codex"))
                .unwrap_or_else(|_| PathBuf::from(".codex"))
        })
}

fn resolve_home_dir() -> PathBuf {
    env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| env::current_dir().unwrap_or_else(|_| PathBuf::from(".")))
}

#[cfg(test)]
pub(crate) fn test_env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn with_temp_codex_home<T>(f: impl FnOnce(&TempDir) -> T) -> T {
        let _guard = test_env_lock()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let temp = TempDir::new().expect("temp dir");
        let previous = env::var_os("CODEX_HOME");
        unsafe {
            env::set_var("CODEX_HOME", temp.path());
        }
        let result = f(&temp);
        unsafe {
            match previous {
                Some(value) => env::set_var("CODEX_HOME", value),
                None => env::remove_var("CODEX_HOME"),
            }
        }
        result
    }

    #[test]
    fn writes_and_loads_redacted_config() {
        with_temp_codex_home(|_| {
            let config = AppConfig {
                version: 1,
                telegram: Some(TelegramConfig {
                    chat_id: "12345".to_string(),
                    update_offset: Some(99),
                }),
            };
            write_telegram_bot_token("123:secret").expect("write token");
            write_config(&config).expect("write config");
            let loaded = load_config().expect("load config").expect("config");
            assert_eq!(loaded, config);
            let redacted = redacted_config_value(&loaded);
            assert_eq!(redacted["telegram"]["chatId"], "12345");
            assert_eq!(redacted["telegram"]["updateOffset"], 99);
            assert_eq!(redacted["telegramBotToken"], "<configured>");
        });
    }

    #[test]
    fn env_token_overrides_file() {
        with_temp_codex_home(|_| {
            write_telegram_bot_token("123:file-secret").expect("write token");
            unsafe {
                env::set_var("TOOL_FEEDBACK_TELEGRAM_BOT_TOKEN", "123:env-secret");
            }
            let token = resolve_telegram_bot_token().expect("resolve token");
            unsafe {
                env::remove_var("TOOL_FEEDBACK_TELEGRAM_BOT_TOKEN");
            }
            assert_eq!(token, "123:env-secret");
        });
    }
}
