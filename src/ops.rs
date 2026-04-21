use anyhow::{Context, Result, bail};
use serde_json::json;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use crate::config::{
    config_path, launch_agents_dir_path, load_config, local_bin_dir_path, redacted_config_value,
    resolve_telegram_bot_token, state_dir_path, telegram_bot_token_path, tool_owners_path,
};
use crate::maintainer::{developer_path, resolve_codex_binary};
use crate::owners;
use crate::state;

const LAUNCHD_TEMPLATE: &str = include_str!("../launchd/com.example.tool-feedback.plist.template");
const DEFAULT_LAUNCHD_LABEL: &str = "com.tool-feedback";

#[derive(Debug, Clone)]
pub(crate) struct InstallOptions {
    pub(crate) dest: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct LaunchdRenderOptions {
    pub(crate) label: String,
    pub(crate) repo: Option<String>,
    pub(crate) bin: Option<String>,
    pub(crate) path: Option<String>,
    pub(crate) state_dir: Option<String>,
    pub(crate) output: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct LaunchdInstallOptions {
    pub(crate) render: LaunchdRenderOptions,
    pub(crate) no_bootstrap: bool,
}

pub(crate) fn doctor() -> Result<serde_json::Value> {
    let config = load_config()?;
    let config_value = config
        .as_ref()
        .map(redacted_config_value)
        .unwrap_or_else(|| json!(null));

    let state_dir = state_dir_path();
    let config_path = config_path();
    let owner_registry_path = tool_owners_path();
    let db_path = state::state_db_path()?;
    let installed_binary_path = local_bin_dir_path().join("tool-feedback");
    let path_binary = command_on_path("tool-feedback");
    let launch_agents_dir = launch_agents_dir_path();
    let launch_agents = discover_launch_agents(&launch_agents_dir)?;
    let codex_binary = resolve_codex_binary().ok();

    let owner_registry = if owner_registry_path.exists() {
        Some(match owners::load_owner_registry() {
            Ok(registry) => json!({
                "ok": true,
                "version": registry.version,
                "toolCount": registry.tool.len()
            }),
            Err(error) => json!({
                "ok": false,
                "error": format!("{error:#}")
            }),
        })
    } else {
        None
    };

    let db_check = if db_path.exists() {
        Some(match state::open_db() {
            Ok(_) => json!({"ok": true}),
            Err(error) => json!({
                "ok": false,
                "error": format!("{error:#}")
            }),
        })
    } else {
        None
    };

    Ok(json!({
        "ok": true,
        "action": "doctor",
        "checks": {
            "stateDir": {
                "path": state_dir.display().to_string(),
                "exists": state_dir.exists()
            },
            "config": {
                "path": config_path.display().to_string(),
                "exists": config_path.exists(),
                "value": config_value
            },
            "telegramBotToken": {
                "path": telegram_bot_token_path().display().to_string(),
                "configured": resolve_telegram_bot_token().is_ok()
            },
            "ownerRegistry": {
                "path": owner_registry_path.display().to_string(),
                "exists": owner_registry_path.exists(),
                "status": owner_registry
            },
            "stateDb": {
                "path": db_path.display().to_string(),
                "exists": db_path.exists(),
                "status": db_check
            },
            "toolFeedbackOnPath": {
                "ok": path_binary.is_some(),
                "path": path_binary.as_ref().map(|path| path.display().to_string()),
                "pathHint": if path_binary.is_some() { serde_json::Value::Null } else { json!(format!("Run `tool-feedback install` to copy the binary into {}", local_bin_dir_path().display())) }
            },
            "installedBinary": {
                "path": installed_binary_path.display().to_string(),
                "exists": installed_binary_path.exists(),
                "parentOnPath": path_contains(&local_bin_dir_path())
            },
            "codexBinary": {
                "ok": codex_binary.is_some(),
                "path": codex_binary.as_ref().map(|path| path.display().to_string())
            },
            "launchd": {
                "available": command_on_path("launchctl").is_some(),
                "launchAgentsDir": launch_agents_dir.display().to_string(),
                "plists": launch_agents,
                "defaultLabel": DEFAULT_LAUNCHD_LABEL
            }
        }
    }))
}

pub(crate) fn install_binary(options: InstallOptions) -> Result<serde_json::Value> {
    let source = env::current_exe().context("failed to resolve current executable")?;
    let source = fs::canonicalize(&source)
        .with_context(|| format!("failed to resolve executable path for {}", source.display()))?;
    let dest = options
        .dest
        .map(PathBuf::from)
        .unwrap_or_else(|| local_bin_dir_path().join("tool-feedback"));
    let dest_parent = dest
        .parent()
        .map(Path::to_path_buf)
        .context("install destination must have a parent directory")?;
    fs::create_dir_all(&dest_parent)
        .with_context(|| format!("failed to create install dir {}", dest_parent.display()))?;

    let same_path = fs::canonicalize(&dest)
        .ok()
        .map(|canonical| canonical == source)
        .unwrap_or(false);
    if !same_path {
        fs::copy(&source, &dest).with_context(|| {
            format!(
                "failed to copy executable from {} to {}",
                source.display(),
                dest.display()
            )
        })?;
    }
    set_executable_permissions(&dest)?;

    Ok(json!({
        "ok": true,
        "action": "install",
        "source": source.display().to_string(),
        "dest": dest.display().to_string(),
        "parentOnPath": path_contains(&dest_parent),
        "pathHint": if path_contains(&dest_parent) {
            serde_json::Value::Null
        } else {
            json!(format!("Add {} to PATH to call `tool-feedback` directly.", dest_parent.display()))
        }
    }))
}

pub(crate) fn render_launchd(options: LaunchdRenderOptions) -> Result<serde_json::Value> {
    let render = build_launchd_render(&options)?;
    if let Some(output) = options.output.as_deref() {
        let output_path = PathBuf::from(output);
        if let Some(parent) = output_path.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!("failed to create launchd output dir {}", parent.display())
            })?;
        }
        fs::write(&output_path, &render.contents).with_context(|| {
            format!("failed to write launchd plist to {}", output_path.display())
        })?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&output_path, fs::Permissions::from_mode(0o644)).with_context(
                || format!("failed to set permissions on {}", output_path.display()),
            )?;
        }
        return Ok(json!({
            "ok": true,
            "action": "launchd_render",
            "label": render.label,
            "output": output_path.display().to_string(),
            "repo": render.repo.display().to_string(),
            "bin": render.bin.display().to_string(),
            "stateDir": render.state_dir.display().to_string(),
            "path": render.path
        }));
    }

    Ok(json!({
        "ok": true,
        "action": "launchd_render",
        "label": render.label,
        "repo": render.repo.display().to_string(),
        "bin": render.bin.display().to_string(),
        "stateDir": render.state_dir.display().to_string(),
        "path": render.path,
        "plist": render.contents
    }))
}

pub(crate) fn install_launchd(options: LaunchdInstallOptions) -> Result<serde_json::Value> {
    if cfg!(not(target_os = "macos")) {
        bail!("launchd install is only supported on macOS");
    }
    let render = build_launchd_render(&options.render)?;
    let launch_agents_dir = launch_agents_dir_path();
    fs::create_dir_all(&launch_agents_dir).with_context(|| {
        format!(
            "failed to create launch agents dir {}",
            launch_agents_dir.display()
        )
    })?;
    let plist_path = launch_agents_dir.join(format!("{}.plist", render.label));
    fs::write(&plist_path, &render.contents)
        .with_context(|| format!("failed to write {}", plist_path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&plist_path, fs::Permissions::from_mode(0o644))
            .with_context(|| format!("failed to set permissions on {}", plist_path.display()))?;
    }

    let mut bootstrap = json!({
        "bootout": "skipped",
        "bootstrap": "skipped",
        "kickstart": "skipped"
    });

    if !options.no_bootstrap {
        run_launchctl(&["bootout", &gui_target()?, &plist_path.display().to_string()])
            .map(|output| bootstrap["bootout"] = output)
            .unwrap_or_else(|error| {
                bootstrap["bootout"] = json!({"ok": false, "error": format!("{error:#}")})
            });
        bootstrap["bootstrap"] = run_launchctl(&[
            "bootstrap",
            &gui_target()?,
            &plist_path.display().to_string(),
        ])?;
        bootstrap["kickstart"] = run_launchctl(&[
            "kickstart",
            "-k",
            &format!("{}/{}", gui_target()?, render.label),
        ])?;
    }

    Ok(json!({
        "ok": true,
        "action": "launchd_install",
        "label": render.label,
        "plist": plist_path.display().to_string(),
        "repo": render.repo.display().to_string(),
        "bin": render.bin.display().to_string(),
        "stateDir": render.state_dir.display().to_string(),
        "bootstrap": bootstrap
    }))
}

struct LaunchdRender {
    label: String,
    repo: PathBuf,
    bin: PathBuf,
    state_dir: PathBuf,
    path: String,
    contents: String,
}

fn build_launchd_render(options: &LaunchdRenderOptions) -> Result<LaunchdRender> {
    let repo = options
        .repo
        .as_deref()
        .map(PathBuf::from)
        .map(Ok)
        .unwrap_or_else(|| env::current_dir().context("failed to resolve current directory"))?;
    let bin = if let Some(bin) = options.bin.as_deref() {
        PathBuf::from(bin)
    } else if let Some(installed) = command_on_path("tool-feedback") {
        installed
    } else {
        env::current_exe().context("failed to resolve current executable")?
    };
    let bin = fs::canonicalize(&bin)
        .with_context(|| format!("failed to resolve binary path {}", bin.display()))?;
    let repo = fs::canonicalize(&repo)
        .with_context(|| format!("failed to resolve repo path {}", repo.display()))?;
    let state_dir = options
        .state_dir
        .as_deref()
        .map(PathBuf::from)
        .unwrap_or_else(state_dir_path);
    let path = options.path.clone().unwrap_or_else(developer_path);
    let label = sanitize_label(&options.label)?;
    let contents = LAUNCHD_TEMPLATE
        .replace("__TOOL_FEEDBACK_LABEL__", &label)
        .replace(
            "__TOOL_FEEDBACK_BIN__",
            &xml_escape(&bin.display().to_string()),
        )
        .replace(
            "__TOOL_FEEDBACK_WORKDIR__",
            &xml_escape(&repo.display().to_string()),
        )
        .replace("__TOOL_FEEDBACK_PATH__", &xml_escape(&path))
        .replace(
            "__TOOL_FEEDBACK_STATE_DIR__",
            &xml_escape(&state_dir.display().to_string()),
        );
    Ok(LaunchdRender {
        label,
        repo,
        bin,
        state_dir,
        path,
        contents,
    })
}

fn sanitize_label(raw: &str) -> Result<String> {
    let label = raw.trim();
    if label.is_empty() {
        bail!("launchd label cannot be empty");
    }
    if label.contains('/') || label.contains(char::is_whitespace) {
        bail!("launchd label cannot contain slashes or whitespace");
    }
    Ok(label.to_string())
}

fn discover_launch_agents(dir: &Path) -> Result<Vec<String>> {
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut matches = fs::read_dir(dir)
        .with_context(|| format!("failed to read {}", dir.display()))?
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| {
            path.file_name()
                .and_then(|value| value.to_str())
                .map(|name| name.contains("tool-feedback") && name.ends_with(".plist"))
                .unwrap_or(false)
        })
        .map(|path| path.display().to_string())
        .collect::<Vec<_>>();
    matches.sort();
    Ok(matches)
}

fn command_on_path(binary: &str) -> Option<PathBuf> {
    env::var_os("PATH").and_then(|raw| {
        env::split_paths(&raw)
            .map(|segment| segment.join(binary))
            .find(|candidate| candidate.exists())
    })
}

fn path_contains(dir: &Path) -> bool {
    env::var_os("PATH")
        .map(|raw| env::split_paths(&raw).any(|segment| segment == dir))
        .unwrap_or(false)
}

fn set_executable_permissions(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o755))
            .with_context(|| format!("failed to set permissions on {}", path.display()))?;
    }
    Ok(())
}

fn gui_target() -> Result<String> {
    Ok(format!(
        "gui/{}",
        current_uid().context("failed to resolve current uid for launchctl target")?
    ))
}

fn current_uid() -> Result<u32> {
    let output = Command::new("/usr/bin/id")
        .arg("-u")
        .output()
        .context("failed to run `id -u`")?;
    if !output.status.success() {
        bail!("`id -u` failed with status {}", output.status);
    }
    let uid = String::from_utf8(output.stdout)
        .context("`id -u` returned invalid utf-8")?
        .trim()
        .parse::<u32>()
        .context("failed to parse `id -u` output")?;
    Ok(uid)
}

fn run_launchctl(args: &[&str]) -> Result<serde_json::Value> {
    let output = Command::new("/bin/launchctl")
        .args(args)
        .output()
        .with_context(|| format!("failed to run launchctl {}", args.join(" ")))?;
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    if !output.status.success() {
        bail!(
            "launchctl {} failed with status {}: {}",
            args.join(" "),
            output.status,
            if stderr.is_empty() { stdout } else { stderr }
        );
    }
    Ok(json!({
        "ok": true,
        "stdout": if stdout.is_empty() { serde_json::Value::Null } else { json!(stdout) },
        "stderr": if stderr.is_empty() { serde_json::Value::Null } else { json!(stderr) }
    }))
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

pub(crate) fn default_launchd_label() -> &'static str {
    DEFAULT_LAUNCHD_LABEL
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_launchd_template_replaces_placeholders() {
        let render = build_launchd_render(&LaunchdRenderOptions {
            label: "com.tool-feedback".to_string(),
            repo: Some("/tmp/tool-feedback".to_string()),
            bin: Some("/tmp/tool-feedback/bin/tool-feedback".to_string()),
            path: Some("/usr/bin:/bin".to_string()),
            state_dir: Some("/tmp/tool-feedback-state".to_string()),
            output: None,
        });
        assert!(render.is_err());
    }

    #[test]
    fn sanitize_label_rejects_whitespace() {
        assert!(sanitize_label("com tool feedback").is_err());
        assert!(sanitize_label("com.tool-feedback").is_ok());
    }

    #[test]
    fn xml_escape_replaces_unsafe_characters() {
        assert_eq!(xml_escape("a&b<c>"), "a&amp;b&lt;c&gt;");
    }
}
