use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::PathBuf;

use crate::config::{ensure_state_dir, tool_owners_path};
use crate::state::MaintainerRunKind;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct OwnerRegistry {
    pub(crate) version: u32,
    #[serde(default)]
    pub(crate) tool: BTreeMap<String, ToolOwner>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ToolOwner {
    pub(crate) repo: String,
    #[serde(default)]
    pub(crate) instructions: String,
    #[serde(default)]
    pub(crate) triage_instructions: String,
    #[serde(default)]
    pub(crate) patch_instructions: String,
    #[serde(default)]
    pub(crate) model: Option<String>,
    #[serde(default)]
    pub(crate) triage_model: Option<String>,
    #[serde(default)]
    pub(crate) patch_model: Option<String>,
    #[serde(default = "default_auto_run_on_accept")]
    pub(crate) auto_run_on_accept: bool,
}

impl ToolOwner {
    pub(crate) fn model_for(&self, kind: MaintainerRunKind) -> Option<&str> {
        match kind {
            MaintainerRunKind::Triage => self
                .triage_model
                .as_deref()
                .or(self.model.as_deref())
                .filter(|value| !value.trim().is_empty()),
            MaintainerRunKind::Patch => self
                .patch_model
                .as_deref()
                .or(self.model.as_deref())
                .filter(|value| !value.trim().is_empty()),
        }
    }

    pub(crate) fn instructions_for(&self, kind: MaintainerRunKind) -> String {
        let mut lines = Vec::new();
        if !self.instructions.trim().is_empty() {
            lines.push(self.instructions.trim().to_string());
        }
        let specific = match kind {
            MaintainerRunKind::Triage => self.triage_instructions.trim(),
            MaintainerRunKind::Patch => self.patch_instructions.trim(),
        };
        if !specific.is_empty() {
            lines.push(specific.to_string());
        }
        if lines.is_empty() {
            "- No extra tool-specific instructions.".to_string()
        } else {
            lines.join("\n\n")
        }
    }
}

fn default_auto_run_on_accept() -> bool {
    true
}

pub(crate) fn load_or_bootstrap_owner_registry() -> Result<OwnerRegistry> {
    let path = tool_owners_path();
    if !path.exists() {
        write_default_owner_registry(false)?;
    }
    load_owner_registry()
}

pub(crate) fn write_default_owner_registry(force: bool) -> Result<PathBuf> {
    ensure_state_dir()?;
    let path = tool_owners_path();
    if path.exists() && !force {
        return Ok(path);
    }
    fs::write(&path, default_owner_registry_toml()?)
        .with_context(|| format!("failed to write owner registry to {}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600))
            .with_context(|| format!("failed to set permissions on {}", path.display()))?;
    }
    Ok(path)
}

pub(crate) fn load_owner_registry() -> Result<OwnerRegistry> {
    let path = tool_owners_path();
    let raw = fs::read_to_string(&path)
        .with_context(|| format!("failed to read owner registry at {}", path.display()))?;
    let registry: OwnerRegistry = toml::from_str(&raw)
        .with_context(|| format!("failed to parse owner registry at {}", path.display()))?;
    if registry.tool.is_empty() {
        bail!(
            "owner registry at {} does not define any tools",
            path.display()
        );
    }
    Ok(registry)
}

pub(crate) fn registry_json(registry: &OwnerRegistry) -> serde_json::Value {
    let tools = registry
        .tool
        .iter()
        .map(|(name, owner)| {
            json!({
                "tool": name,
                "repo": owner.repo,
                "model": owner.model,
                "triageModel": owner.triage_model,
                "patchModel": owner.patch_model,
                "autoRunOnAccept": owner.auto_run_on_accept
            })
        })
        .collect::<Vec<_>>();
    json!({
        "version": registry.version,
        "path": tool_owners_path().display().to_string(),
        "tools": tools
    })
}

pub(crate) fn owner_for_tool<'a>(registry: &'a OwnerRegistry, tool: &str) -> Option<&'a ToolOwner> {
    registry.tool.get(tool.trim())
}

fn default_owner_registry_toml() -> Result<String> {
    let home = env::var_os("HOME")
        .map(PathBuf::from)
        .or_else(|| env::current_dir().ok())
        .unwrap_or_else(|| PathBuf::from("."));
    let tool_feedback_repo = home.join("projects/tool-feedback");
    let codex_recall_repo = home.join("projects/codex-recall");
    Ok(format!(
        r#"version = 2

[tool."tool-feedback"]
repo = "{tool_feedback_repo}"
instructions = """
You maintain the tool-feedback runtime itself.
Prefer small, production-friendly Rust changes.
Keep the queue, registry, launchd, and Telegram behavior deterministic.
Do not commit or push.
"""
triage_model = "gpt-5.4-mini"
triage_instructions = """
Triage only. Do not edit files.
Stay repo-local and narrow.
Do not read cross-project memory, journal notes, or shared tool docs unless the case explicitly requires them.
If a patch looks warranted, return `awaiting_approval` with the exact area that should be changed.
"""
patch_instructions = """
Patch mode. Make the smallest durable fix, run the relevant checks, and keep the repo publishable.
"""
auto_run_on_accept = true

[tool."codex-recall"]
repo = "{codex_recall_repo}"
instructions = """
You maintain codex-recall.
Prioritize retrieval quality, deterministic CLI behavior, automation-friendly JSON, clear errors, and docs that prevent agent misuse.
Do not commit or push.
"""
triage_model = "gpt-5.4-mini"
triage_instructions = """
Triage only. Do not edit files.
Stay repo-local and narrow.
Do not read cross-project memory, journal notes, or shared tool docs unless the case explicitly requires them.
Use codex-recall only when the case explicitly needs transcript evidence from the originating session.
If a patch looks warranted, return `awaiting_approval` with the exact area that should be changed.
"""
patch_instructions = """
Patch mode. Make the smallest durable fix, run the relevant checks, and keep the CLI predictable for future agent use.
Use codex-recall only if the patch genuinely depends on transcript evidence.
"""
auto_run_on_accept = true
"#,
        tool_feedback_repo = toml_escape_path(&tool_feedback_repo),
        codex_recall_repo = toml_escape_path(&codex_recall_repo)
    ))
}

fn toml_escape_path(path: &PathBuf) -> String {
    path.display()
        .to_string()
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
}
