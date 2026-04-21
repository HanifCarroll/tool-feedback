use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::BTreeSet;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use crate::config::ensure_runs_dir;
use crate::owners::{ToolOwner, load_or_bootstrap_owner_registry, owner_for_tool};
use crate::state::{
    self, CaseRecord, CaseStatus, MaintainerRunKind, MaintainerRunRecord, MaintainerRunStatus,
    NewMaintainerRunInput,
};

const DEFAULT_CODEX_BINARY: &str = ".bun/bin/codex";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MaintainerResult {
    pub(crate) case_id: String,
    pub(crate) status: String,
    pub(crate) resolution: String,
    pub(crate) summary: String,
    pub(crate) files_changed: Vec<String>,
    pub(crate) tests_run: Vec<String>,
    pub(crate) follow_up: Option<String>,
}

pub(crate) fn maintainer_cycle() -> Result<serde_json::Value> {
    let conn = state::open_db()?;
    let registry = load_or_bootstrap_owner_registry()?;

    let mut finalized = reconcile_running_runs(&conn)?;
    let (queued, warnings) = queue_ready_cases(&conn, &registry)?;
    let (started, finalized_from_queue) = start_queued_runs(&conn)?;
    finalized.extend(finalized_from_queue);
    let running =
        state::list_maintainer_runs(&conn, Some(MaintainerRunStatus::Running), None, None)?;

    Ok(json!({
        "ok": true,
        "action": "maintainer_cycle",
        "queued": queued,
        "started": started,
        "finalized": finalized,
        "running": running.iter().map(run_summary_json).collect::<Vec<_>>(),
        "warnings": warnings
    }))
}

fn queue_ready_cases(
    conn: &rusqlite::Connection,
    registry: &crate::owners::OwnerRegistry,
) -> Result<(Vec<serde_json::Value>, Vec<String>)> {
    let mut queued = Vec::new();
    let mut warnings = Vec::new();
    queue_cases_for_status(
        conn,
        registry,
        CaseStatus::Accepted,
        MaintainerRunKind::Triage,
        &mut queued,
        &mut warnings,
    )?;
    queue_cases_for_status(
        conn,
        registry,
        CaseStatus::PatchApproved,
        MaintainerRunKind::Patch,
        &mut queued,
        &mut warnings,
    )?;
    Ok((queued, warnings))
}

fn queue_cases_for_status(
    conn: &rusqlite::Connection,
    registry: &crate::owners::OwnerRegistry,
    status: CaseStatus,
    run_kind: MaintainerRunKind,
    queued: &mut Vec<serde_json::Value>,
    warnings: &mut Vec<String>,
) -> Result<()> {
    for case_record in state::cases_ready_for_run(conn, status, run_kind)? {
        match owner_for_tool(registry, &case_record.tool) {
            Some(owner) if owner.auto_run_on_accept => {
                let queued_run = queue_run_for_case(conn, &case_record, owner, run_kind)?;
                queued.push(run_summary_json(&queued_run));
            }
            Some(_) => {
                warnings.push(format!(
                    "tool `{}` is configured but auto_run_on_accept is false for {}",
                    case_record.tool, case_record.public_id
                ));
            }
            None => {
                warnings.push(format!(
                    "no owner configured for tool `{}` while {} is `{}`",
                    case_record.tool,
                    case_record.public_id,
                    status.as_str()
                ));
            }
        }
    }
    Ok(())
}

fn queue_run_for_case(
    conn: &rusqlite::Connection,
    case_record: &CaseRecord,
    owner: &ToolOwner,
    run_kind: MaintainerRunKind,
) -> Result<MaintainerRunRecord> {
    let now = state::now_millis()?;
    let run_dir = ensure_runs_dir()?.join(format!(
        "{}-{}-{}-{}",
        case_record.tool,
        case_record.public_id,
        run_kind.as_str(),
        now
    ));
    fs::create_dir_all(&run_dir)
        .with_context(|| format!("failed to create run dir at {}", run_dir.display()))?;
    set_dir_permissions(&run_dir)?;

    let prompt_path = run_dir.join("prompt.md");
    let schema_path = run_dir.join("schema.json");
    let result_path = run_dir.join("result.json");
    let stdout_path = run_dir.join("stdout.jsonl");
    let stderr_path = run_dir.join("stderr.log");
    let exit_code_path = run_dir.join("exit_code");
    let launcher_path = run_dir.join("launch-maintainer.sh");
    let codex_path = resolve_codex_binary()?;
    let triage_handoff =
        state::latest_succeeded_run_for_case(conn, case_record.id, MaintainerRunKind::Triage)?;
    let prompt = build_prompt(case_record, owner, run_kind, triage_handoff.as_ref());

    write_private_file(&prompt_path, &prompt)?;
    write_private_file(&schema_path, result_schema(run_kind))?;
    write_executable_file(
        &launcher_path,
        &build_launcher_script(
            &codex_path,
            &owner.repo,
            owner.model_for(run_kind),
            &schema_path,
            &result_path,
            &prompt_path,
            &stdout_path,
            &stderr_path,
            &exit_code_path,
        ),
    )?;

    state::create_queued_run(
        conn,
        NewMaintainerRunInput {
            case_id: case_record.id,
            run_kind,
            tool: case_record.tool.clone(),
            owner_tool: case_record.tool.clone(),
            repo_path: owner.repo.clone(),
            codex_path: codex_path.display().to_string(),
            prompt_path: prompt_path.display().to_string(),
            schema_path: schema_path.display().to_string(),
            result_path: result_path.display().to_string(),
            stdout_path: stdout_path.display().to_string(),
            stderr_path: stderr_path.display().to_string(),
            exit_code_path: exit_code_path.display().to_string(),
            launcher_path: launcher_path.display().to_string(),
        },
        now,
    )
}

fn start_queued_runs(
    conn: &rusqlite::Connection,
) -> Result<(Vec<serde_json::Value>, Vec<serde_json::Value>)> {
    let mut started = Vec::new();
    let mut finalized = Vec::new();
    let queued_runs =
        state::list_maintainer_runs(conn, Some(MaintainerRunStatus::Queued), None, None)?;
    let mut running_tools =
        state::list_maintainer_runs(conn, Some(MaintainerRunStatus::Running), None, None)?
            .into_iter()
            .map(|run| run.tool)
            .collect::<BTreeSet<_>>();

    for run in queued_runs {
        if running_tools.contains(&run.tool) {
            continue;
        }
        let (started_run, finished_run) = execute_run(conn, &run)?;
        running_tools.insert(started_run.tool.clone());
        started.push(run_summary_json(&started_run));
        finalized.push(run_summary_json(&finished_run));
    }
    Ok((started, finalized))
}

fn reconcile_running_runs(conn: &rusqlite::Connection) -> Result<Vec<serde_json::Value>> {
    let mut finalized = Vec::new();
    for run in state::list_maintainer_runs(conn, Some(MaintainerRunStatus::Running), None, None)? {
        let maybe_exit_code = read_exit_code(&PathBuf::from(&run.exit_code_path))?;
        let Some(exit_code) = maybe_exit_code.or_else(|| {
            run.pid
                .filter(|pid| !pid_is_alive(*pid).unwrap_or(true))
                .map(|_| 1)
        }) else {
            continue;
        };
        if exit_code == 0 {
            let result = load_result(&run)?;
            let finished_run = state::mark_maintainer_run_succeeded(
                conn,
                run.id,
                exit_code,
                &result,
                state::now_millis()?,
            )?;
            apply_success_result(conn, &finished_run, &result)?;
            finalized.push(run_summary_json(&finished_run));
        } else {
            let error_text = if maybe_exit_code.is_some() {
                load_failure_text(&run)?
            } else {
                "maintainer launcher exited without writing an exit code".to_string()
            };
            let finished_run = state::mark_maintainer_run_failed(
                conn,
                run.id,
                exit_code,
                &error_text,
                state::now_millis()?,
            )?;
            apply_failed_result(conn, &finished_run, &error_text)?;
            finalized.push(run_summary_json(&finished_run));
        }
    }
    Ok(finalized)
}

fn execute_run(
    conn: &rusqlite::Connection,
    run: &MaintainerRunRecord,
) -> Result<(MaintainerRunRecord, MaintainerRunRecord)> {
    let prompt = fs::File::open(&run.prompt_path)
        .with_context(|| format!("failed to open prompt at {}", run.prompt_path))?;
    let stdout = fs::File::create(&run.stdout_path)
        .with_context(|| format!("failed to open stdout log at {}", run.stdout_path))?;
    let stderr = fs::File::create(&run.stderr_path)
        .with_context(|| format!("failed to open stderr log at {}", run.stderr_path))?;

    let mut command = Command::new(&run.codex_path);
    command
        .arg("exec")
        .arg("--cd")
        .arg(&run.repo_path)
        .arg("--skip-git-repo-check")
        .arg("--full-auto")
        .arg("--output-schema")
        .arg(&run.schema_path)
        .arg("-o")
        .arg(&run.result_path)
        .arg("--json")
        .arg("-")
        .stdin(Stdio::from(prompt))
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .env("PATH", developer_path());

    let run_kind = MaintainerRunKind::parse(&run.run_kind)?;
    if let Some(model) = load_or_bootstrap_owner_registry()?
        .tool
        .get(&run.owner_tool)
        .and_then(|owner| owner.model_for(run_kind))
    {
        command.arg("--model").arg(model);
    }

    let mut child = command
        .spawn()
        .with_context(|| format!("failed to spawn maintainer run for {}", run.public_id))?;
    let started_run = state::mark_maintainer_run_started(
        conn,
        run.id,
        i64::from(child.id()),
        state::now_millis()?,
    )?;
    let status = child
        .wait()
        .with_context(|| format!("failed while waiting for {}", run.public_id))?;
    let exit_code = status.code().unwrap_or(1);
    write_exit_code_file(&run.exit_code_path, exit_code)?;

    let finished_run = if exit_code == 0 {
        let result = load_result(&started_run)?;
        let finished_run = state::mark_maintainer_run_succeeded(
            conn,
            run.id,
            exit_code,
            &result,
            state::now_millis()?,
        )?;
        apply_success_result(conn, &finished_run, &result)?;
        finished_run
    } else {
        let error_text = load_failure_text(&started_run)?;
        let finished_run = state::mark_maintainer_run_failed(
            conn,
            run.id,
            exit_code,
            &error_text,
            state::now_millis()?,
        )?;
        apply_failed_result(conn, &finished_run, &error_text)?;
        finished_run
    };

    Ok((started_run, finished_run))
}

fn apply_success_result(
    conn: &rusqlite::Connection,
    run: &MaintainerRunRecord,
    result: &MaintainerResult,
) -> Result<()> {
    let current_case =
        state::load_case(conn, run.case_id)?.context("case not found while applying run result")?;
    let run_kind = MaintainerRunKind::parse(&run.run_kind)?;
    let expected_status = expected_case_status_for_run(run_kind);
    let target_status = CaseStatus::parse(&result.status)?;
    let message = resolution_message(result);
    if current_case.status == expected_status.as_str() {
        state::transition_case(
            conn,
            run.case_id,
            target_status,
            message,
            state::now_millis()?,
        )?;
    } else {
        state::append_case_note(
            conn,
            run.case_id,
            format!(
                "Maintainer run {} ({}) finished with `{}` after the case had already moved to `{}`. {}",
                run.public_id, run.run_kind, result.status, current_case.status, message
            ),
            state::now_millis()?,
        )?;
    }
    Ok(())
}

fn apply_failed_result(
    conn: &rusqlite::Connection,
    run: &MaintainerRunRecord,
    error_text: &str,
) -> Result<()> {
    let current_case =
        state::load_case(conn, run.case_id)?.context("case not found while applying failed run")?;
    let run_kind = MaintainerRunKind::parse(&run.run_kind)?;
    let expected_status = expected_case_status_for_run(run_kind);
    let compact_error = first_non_empty_line(error_text).unwrap_or("maintainer run failed");
    if current_case.status == expected_status.as_str() {
        state::transition_case(
            conn,
            run.case_id,
            CaseStatus::Blocked,
            format!(
                "{} run failed: {}",
                run.run_kind,
                compact_text(compact_error, 240)
            ),
            state::now_millis()?,
        )?;
    } else {
        state::append_case_note(
            conn,
            run.case_id,
            format!(
                "Maintainer run {} ({}) failed after the case had already moved to `{}`: {}",
                run.public_id,
                run.run_kind,
                current_case.status,
                compact_text(compact_error, 240)
            ),
            state::now_millis()?,
        )?;
    }
    Ok(())
}

fn load_result(run: &MaintainerRunRecord) -> Result<MaintainerResult> {
    let raw = fs::read_to_string(&run.result_path)
        .with_context(|| format!("failed to read maintainer result at {}", run.result_path))?;
    let result: MaintainerResult = serde_json::from_str(&raw)
        .with_context(|| format!("failed to parse maintainer result at {}", run.result_path))?;
    if result.case_id != run.case_public_id {
        bail!(
            "maintainer result case id mismatch: expected `{}`, got `{}`",
            run.case_public_id,
            result.case_id
        );
    }
    validate_result_status(&result, MaintainerRunKind::parse(&run.run_kind)?)?;
    if result.summary.trim().is_empty() {
        bail!("maintainer result summary cannot be empty");
    }
    if result.resolution.trim().is_empty() {
        bail!("maintainer result resolution cannot be empty");
    }
    Ok(result)
}

fn validate_result_status(result: &MaintainerResult, run_kind: MaintainerRunKind) -> Result<()> {
    let valid = match run_kind {
        MaintainerRunKind::Triage => matches!(
            result.status.as_str(),
            "awaiting_approval" | "blocked" | "rejected" | "deferred" | "duplicate"
        ),
        MaintainerRunKind::Patch => matches!(
            result.status.as_str(),
            "completed" | "blocked" | "rejected" | "deferred" | "duplicate"
        ),
    };
    if !valid {
        bail!(
            "unsupported maintainer result status `{}` for {} run",
            result.status,
            run_kind.as_str()
        );
    }
    Ok(())
}

fn load_failure_text(run: &MaintainerRunRecord) -> Result<String> {
    let stderr_path = PathBuf::from(&run.stderr_path);
    if stderr_path.exists() {
        let text = fs::read_to_string(&stderr_path)
            .with_context(|| format!("failed to read stderr log at {}", stderr_path.display()))?;
        if !text.trim().is_empty() {
            return Ok(text);
        }
    }
    let stdout_path = PathBuf::from(&run.stdout_path);
    if stdout_path.exists() {
        let text = fs::read_to_string(&stdout_path)
            .with_context(|| format!("failed to read stdout log at {}", stdout_path.display()))?;
        if !text.trim().is_empty() {
            return Ok(text);
        }
    }
    Ok("maintainer run exited without producing diagnostics".to_string())
}

fn read_exit_code(path: &Path) -> Result<Option<i32>> {
    if !path.exists() {
        return Ok(None);
    }
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read exit code file at {}", path.display()))?;
    let exit_code = raw
        .trim()
        .parse::<i32>()
        .with_context(|| format!("invalid exit code in {}", path.display()))?;
    Ok(Some(exit_code))
}

fn build_prompt(
    case_record: &CaseRecord,
    owner: &ToolOwner,
    run_kind: MaintainerRunKind,
    triage_handoff: Option<&MaintainerRunRecord>,
) -> String {
    match run_kind {
        MaintainerRunKind::Triage => build_triage_prompt(case_record, owner),
        MaintainerRunKind::Patch => build_patch_prompt(case_record, owner, triage_handoff),
    }
}

fn build_triage_prompt(case_record: &CaseRecord, owner: &ToolOwner) -> String {
    format!(
        r#"You are the triage maintainer for the CLI/tool `{tool}`.

Repository:
- {repo}

Tool-specific instructions:
{instructions}

Your job is to decide whether this case should be closed now or moved to human approval for a patch.

Rules:
- Do not edit files in triage mode.
- Keep inspection repo-local and narrow.
- Do not read cross-project memory, journal notes, or shared tool docs unless the case explicitly requires them.
- Use at most three cheap verification commands.
- Use `codex-recall` only if the case explicitly needs transcript evidence from the originating session.
- `filesChanged` must always be an empty array in triage mode.
- `testsRun` must list the exact commands you actually executed.

Case:
- Case ID: {case_id}
- Summary: {summary}
- Details: {details}
- Source thread ID: {source_thread_id}
- Source session ID: {source_session_id}
- Source cwd: {cwd}
- Source command: {command_text}

Required final JSON fields:
- `caseId`: repeat `{case_id}`
- `status`: one of `awaiting_approval`, `blocked`, `rejected`, `deferred`, `duplicate`
- `resolution`: short label like `patch_needed`, `investigated`, `duplicate`, `out_of_scope`
- `summary`: short concrete outcome summary
- `filesChanged`: empty array
- `testsRun`: array of exact commands executed
- `followUp`: string or null. If you return `awaiting_approval`, use this for the patch handoff with the file(s) or area to inspect.
"#,
        tool = case_record.tool,
        repo = owner.repo,
        instructions = owner.instructions_for(MaintainerRunKind::Triage),
        case_id = case_record.public_id,
        summary = case_record.summary,
        details = case_record.details.as_deref().unwrap_or("none"),
        source_thread_id = case_record.source_thread_id.as_deref().unwrap_or("none"),
        source_session_id = case_record.source_session_id.as_deref().unwrap_or("none"),
        cwd = case_record.cwd.as_deref().unwrap_or("none"),
        command_text = case_record.command_text.as_deref().unwrap_or("none"),
    )
}

fn build_patch_prompt(
    case_record: &CaseRecord,
    owner: &ToolOwner,
    triage_handoff: Option<&MaintainerRunRecord>,
) -> String {
    format!(
        r#"You are the patch maintainer for the CLI/tool `{tool}`.

Repository:
- {repo}

Tool-specific instructions:
{instructions}

This case already passed triage and the human approved a patch run.

Rules:
- Make the smallest durable fix that resolves the case.
- Keep changes explicit, production-friendly, and publishable.
- Run the relevant checks when you change files.
- Use `codex-recall` only if the patch truly depends on transcript evidence from the originating session.
- `filesChanged` must list only the repo-relative files you changed in this run.
- `testsRun` must list the exact commands you actually executed.

Case:
- Case ID: {case_id}
- Summary: {summary}
- Details: {details}
- Source thread ID: {source_thread_id}
- Source session ID: {source_session_id}
- Source cwd: {cwd}
- Source command: {command_text}

Triage handoff:
{triage_handoff}

Required final JSON fields:
- `caseId`: repeat `{case_id}`
- `status`: one of `completed`, `blocked`, `rejected`, `deferred`, `duplicate`
- `resolution`: short label like `patched`, `docs_updated`, `investigated`, `no_change`
- `summary`: short concrete outcome summary
- `filesChanged`: array of repo-relative paths
- `testsRun`: array of exact commands executed
- `followUp`: string or null
"#,
        tool = case_record.tool,
        repo = owner.repo,
        instructions = owner.instructions_for(MaintainerRunKind::Patch),
        case_id = case_record.public_id,
        summary = case_record.summary,
        details = case_record.details.as_deref().unwrap_or("none"),
        source_thread_id = case_record.source_thread_id.as_deref().unwrap_or("none"),
        source_session_id = case_record.source_session_id.as_deref().unwrap_or("none"),
        cwd = case_record.cwd.as_deref().unwrap_or("none"),
        command_text = case_record.command_text.as_deref().unwrap_or("none"),
        triage_handoff = triage_handoff
            .map(|run| {
                format!(
                    "- Run ID: {}\n- Resolution: {}\n- Summary: {}\n- Follow up: {}",
                    run.public_id,
                    run.resolution.as_deref().unwrap_or("none"),
                    run.result_summary.as_deref().unwrap_or("none"),
                    run.follow_up.as_deref().unwrap_or("none"),
                )
            })
            .unwrap_or_else(|| "- No successful triage handoff recorded.".to_string()),
    )
}

fn result_schema(run_kind: MaintainerRunKind) -> &'static str {
    match run_kind {
        MaintainerRunKind::Triage => TRIAGE_RESULT_SCHEMA,
        MaintainerRunKind::Patch => PATCH_RESULT_SCHEMA,
    }
}

fn build_launcher_script(
    codex_path: &Path,
    repo_path: &str,
    model: Option<&str>,
    schema_path: &Path,
    result_path: &Path,
    prompt_path: &Path,
    stdout_path: &Path,
    stderr_path: &Path,
    exit_code_path: &Path,
) -> String {
    let mut args = vec![
        sh_quote(&codex_path.display().to_string()),
        "exec".to_string(),
        "--cd".to_string(),
        sh_quote(repo_path),
        "--skip-git-repo-check".to_string(),
        "--full-auto".to_string(),
        "--output-schema".to_string(),
        sh_quote(&schema_path.display().to_string()),
        "-o".to_string(),
        sh_quote(&result_path.display().to_string()),
        "--json".to_string(),
        "-".to_string(),
    ];
    if let Some(model) = model.filter(|value| !value.trim().is_empty()) {
        args.insert(2, sh_quote(model));
        args.insert(2, "--model".to_string());
    }

    format!(
        r#"#!/bin/zsh
umask 077
export PATH={path}
{command} < {prompt} > {stdout} 2> {stderr}
exit_code=$?
printf '%s\n' "$exit_code" > {exit_code_path}
exit "$exit_code"
"#,
        path = sh_quote(&developer_path()),
        command = args.join(" "),
        prompt = sh_quote(&prompt_path.display().to_string()),
        stdout = sh_quote(&stdout_path.display().to_string()),
        stderr = sh_quote(&stderr_path.display().to_string()),
        exit_code_path = sh_quote(&exit_code_path.display().to_string()),
    )
}

fn run_summary_json(run: &MaintainerRunRecord) -> serde_json::Value {
    json!({
        "id": run.public_id,
        "caseId": run.case_public_id,
        "runKind": run.run_kind,
        "tool": run.tool,
        "status": run.status,
        "repoPath": run.repo_path,
        "pid": run.pid,
        "resultStatus": run.result_status,
        "resolution": run.resolution,
        "summary": run.result_summary,
        "error": run.error_text
    })
}

fn resolution_message(result: &MaintainerResult) -> String {
    match result
        .follow_up
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        Some(follow_up) => format!("{} Follow up: {}", result.summary.trim(), follow_up.trim()),
        None => result.summary.trim().to_string(),
    }
}

fn expected_case_status_for_run(run_kind: MaintainerRunKind) -> CaseStatus {
    match run_kind {
        MaintainerRunKind::Triage => CaseStatus::Accepted,
        MaintainerRunKind::Patch => CaseStatus::PatchApproved,
    }
}

fn resolve_codex_binary() -> Result<PathBuf> {
    if let Ok(value) = env::var("TOOL_FEEDBACK_CODEX_BIN") {
        let path = PathBuf::from(value.trim());
        if path.exists() {
            return Ok(path);
        }
    }
    if let Ok(path) = which_from_path("codex") {
        return Ok(path);
    }
    if let Some(home) = env::var_os("HOME") {
        let fallback = PathBuf::from(home).join(DEFAULT_CODEX_BINARY);
        if fallback.exists() {
            return Ok(fallback);
        }
    }
    bail!("failed to resolve a codex binary; set TOOL_FEEDBACK_CODEX_BIN explicitly")
}

fn which_from_path(binary: &str) -> Result<PathBuf> {
    let path_var = env::var("PATH").unwrap_or_default();
    for segment in path_var.split(':').filter(|segment| !segment.is_empty()) {
        let candidate = PathBuf::from(segment).join(binary);
        if candidate.exists() {
            return Ok(candidate);
        }
    }
    bail!("binary `{binary}` was not found on PATH")
}

pub(crate) fn developer_path() -> String {
    let mut dirs = Vec::new();
    if let Ok(current_path) = env::var("PATH") {
        append_path_segments(&mut dirs, &current_path);
    }
    if let Some(home) = env::var_os("HOME").map(PathBuf::from) {
        if let Some(nvm_node_dir) = discover_nvm_node_dir(&home) {
            push_path_dir(&mut dirs, nvm_node_dir.display().to_string());
        }
        push_path_dir(&mut dirs, home.join(".cargo/bin").display().to_string());
        push_path_dir(&mut dirs, home.join(".bun/bin").display().to_string());
    }
    push_path_dir(&mut dirs, "/opt/homebrew/bin".to_string());
    push_path_dir(&mut dirs, "/usr/local/bin".to_string());
    push_path_dir(&mut dirs, "/usr/bin".to_string());
    push_path_dir(&mut dirs, "/bin".to_string());
    push_path_dir(&mut dirs, "/usr/sbin".to_string());
    push_path_dir(&mut dirs, "/sbin".to_string());
    dirs.join(":")
}

fn discover_nvm_node_dir(home: &Path) -> Option<PathBuf> {
    let versions_dir = home.join(".config/nvm/versions/node");
    let mut candidates = fs::read_dir(&versions_dir)
        .ok()?
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| path.join("bin/node").exists())
        .collect::<Vec<_>>();
    candidates.sort();
    candidates.pop().map(|path| path.join("bin"))
}

fn append_path_segments(target: &mut Vec<String>, raw_path: &str) {
    for segment in raw_path.split(':').filter(|segment| !segment.is_empty()) {
        push_path_dir(target, segment.to_string());
    }
}

fn push_path_dir(target: &mut Vec<String>, candidate: String) {
    if !candidate.is_empty() && !target.iter().any(|existing| existing == &candidate) {
        target.push(candidate);
    }
}

fn write_private_file(path: &Path, contents: &str) -> Result<()> {
    fs::write(path, contents)
        .with_context(|| format!("failed to write file at {}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))
            .with_context(|| format!("failed to set permissions on {}", path.display()))?;
    }
    Ok(())
}

fn write_executable_file(path: &Path, contents: &str) -> Result<()> {
    fs::write(path, contents)
        .with_context(|| format!("failed to write launcher at {}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))
            .with_context(|| format!("failed to set permissions on {}", path.display()))?;
    }
    Ok(())
}

fn write_exit_code_file(path: &str, exit_code: i32) -> Result<()> {
    let path = PathBuf::from(path);
    fs::write(&path, format!("{exit_code}\n"))
        .with_context(|| format!("failed to write exit code at {}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600))
            .with_context(|| format!("failed to set permissions on {}", path.display()))?;
    }
    Ok(())
}

fn set_dir_permissions(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))
            .with_context(|| format!("failed to set permissions on {}", path.display()))?;
    }
    Ok(())
}

fn sh_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

fn first_non_empty_line(text: &str) -> Option<&str> {
    text.lines().map(str::trim).find(|line| !line.is_empty())
}

fn compact_text(text: &str, max_len: usize) -> String {
    let trimmed = text.trim();
    if trimmed.chars().count() <= max_len {
        return trimmed.to_string();
    }
    let compact = trimmed
        .chars()
        .take(max_len.saturating_sub(1))
        .collect::<String>();
    format!("{compact}…")
}

fn pid_is_alive(pid: i64) -> Result<bool> {
    let status = Command::new("/bin/kill")
        .arg("-0")
        .arg(pid.to_string())
        .status()
        .with_context(|| format!("failed to probe pid {pid}"))?;
    Ok(status.success())
}

const TRIAGE_RESULT_SCHEMA: &str = r#"{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": [
    "caseId",
    "status",
    "resolution",
    "summary",
    "filesChanged",
    "testsRun",
    "followUp"
  ],
  "properties": {
    "caseId": {
      "type": "string",
      "minLength": 1
    },
    "status": {
      "type": "string",
      "enum": ["awaiting_approval", "blocked", "rejected", "deferred", "duplicate"]
    },
    "resolution": {
      "type": "string",
      "minLength": 1
    },
    "summary": {
      "type": "string",
      "minLength": 1
    },
    "filesChanged": {
      "type": "array",
      "items": {
        "type": "string"
      }
    },
    "testsRun": {
      "type": "array",
      "items": {
        "type": "string"
      }
    },
    "followUp": {
      "type": ["string", "null"]
    }
  }
}"#;

const PATCH_RESULT_SCHEMA: &str = r#"{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": [
    "caseId",
    "status",
    "resolution",
    "summary",
    "filesChanged",
    "testsRun",
    "followUp"
  ],
  "properties": {
    "caseId": {
      "type": "string",
      "minLength": 1
    },
    "status": {
      "type": "string",
      "enum": ["completed", "blocked", "rejected", "deferred", "duplicate"]
    },
    "resolution": {
      "type": "string",
      "minLength": 1
    },
    "summary": {
      "type": "string",
      "minLength": 1
    },
    "filesChanged": {
      "type": "array",
      "items": {
        "type": "string"
      }
    },
    "testsRun": {
      "type": "array",
      "items": {
        "type": "string"
      }
    },
    "followUp": {
      "type": ["string", "null"]
    }
  }
}"#;
