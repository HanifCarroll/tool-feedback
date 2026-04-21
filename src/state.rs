use anyhow::{Context, Result, bail};
use rusqlite::types::Value as SqlValue;
use rusqlite::{Connection, OptionalExtension, params, params_from_iter};
use serde::Serialize;
use serde_json::{Value, json};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::config::ensure_state_dir;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CaseStatus {
    New,
    Accepted,
    AwaitingApproval,
    PatchApproved,
    Blocked,
    Completed,
    Rejected,
    Deferred,
    Duplicate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MaintainerRunStatus {
    Queued,
    Running,
    Succeeded,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MaintainerRunKind {
    Triage,
    Patch,
}

impl MaintainerRunStatus {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
        }
    }

    pub(crate) fn parse(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "queued" => Ok(Self::Queued),
            "running" => Ok(Self::Running),
            "succeeded" => Ok(Self::Succeeded),
            "failed" => Ok(Self::Failed),
            other => bail!("unsupported maintainer run status `{other}`"),
        }
    }
}

impl MaintainerRunKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Triage => "triage",
            Self::Patch => "patch",
        }
    }

    pub(crate) fn parse(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "triage" => Ok(Self::Triage),
            "patch" => Ok(Self::Patch),
            other => bail!("unsupported maintainer run kind `{other}`"),
        }
    }
}

impl CaseStatus {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::New => "new",
            Self::Accepted => "accepted",
            Self::AwaitingApproval => "awaiting_approval",
            Self::PatchApproved => "patch_approved",
            Self::Blocked => "blocked",
            Self::Completed => "completed",
            Self::Rejected => "rejected",
            Self::Deferred => "deferred",
            Self::Duplicate => "duplicate",
        }
    }

    pub(crate) fn parse(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "new" => Ok(Self::New),
            "accepted" => Ok(Self::Accepted),
            "awaiting_approval" => Ok(Self::AwaitingApproval),
            "patch_approved" => Ok(Self::PatchApproved),
            "blocked" => Ok(Self::Blocked),
            "completed" => Ok(Self::Completed),
            "rejected" => Ok(Self::Rejected),
            "deferred" => Ok(Self::Deferred),
            "duplicate" => Ok(Self::Duplicate),
            other => bail!("unsupported case status `{other}`"),
        }
    }

    fn is_open(self) -> bool {
        matches!(
            self,
            Self::New
                | Self::Accepted
                | Self::AwaitingApproval
                | Self::PatchApproved
                | Self::Blocked
                | Self::Deferred
        )
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CaseRecord {
    pub(crate) id: i64,
    pub(crate) public_id: String,
    pub(crate) tool: String,
    pub(crate) summary: String,
    pub(crate) details: Option<String>,
    pub(crate) dedupe_key: String,
    pub(crate) source_thread_id: Option<String>,
    pub(crate) source_session_id: Option<String>,
    pub(crate) cwd: Option<String>,
    pub(crate) command_text: Option<String>,
    pub(crate) status: String,
    pub(crate) occurrence_count: i64,
    pub(crate) created_at: i64,
    pub(crate) updated_at: i64,
    pub(crate) last_seen_at: i64,
}

#[derive(Debug, Clone)]
pub(crate) struct NewCaseInput {
    pub(crate) tool: String,
    pub(crate) summary: String,
    pub(crate) details: Option<String>,
    pub(crate) dedupe_key: Option<String>,
    pub(crate) source_thread_id: Option<String>,
    pub(crate) source_session_id: Option<String>,
    pub(crate) cwd: Option<String>,
    pub(crate) command_text: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SubmitOutcome {
    pub(crate) created: bool,
    pub(crate) case_record: CaseRecord,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CaseEventRecord {
    pub(crate) id: i64,
    pub(crate) case_id: i64,
    pub(crate) case_public_id: String,
    pub(crate) tool: String,
    pub(crate) case_summary: String,
    pub(crate) kind: String,
    pub(crate) from_status: Option<String>,
    pub(crate) to_status: Option<String>,
    pub(crate) message: String,
    pub(crate) created_at: i64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MaintainerRunRecord {
    pub(crate) id: i64,
    pub(crate) public_id: String,
    pub(crate) case_id: i64,
    pub(crate) case_public_id: String,
    pub(crate) run_kind: String,
    pub(crate) tool: String,
    pub(crate) owner_tool: String,
    pub(crate) repo_path: String,
    pub(crate) status: String,
    pub(crate) queued_at: i64,
    pub(crate) started_at: Option<i64>,
    pub(crate) updated_at: i64,
    pub(crate) completed_at: Option<i64>,
    pub(crate) pid: Option<i64>,
    pub(crate) codex_path: String,
    pub(crate) prompt_path: String,
    pub(crate) schema_path: String,
    pub(crate) result_path: String,
    pub(crate) stdout_path: String,
    pub(crate) stderr_path: String,
    pub(crate) exit_code_path: String,
    pub(crate) launcher_path: String,
    pub(crate) exit_code: Option<i32>,
    pub(crate) result_status: Option<String>,
    pub(crate) resolution: Option<String>,
    pub(crate) result_summary: Option<String>,
    pub(crate) files_changed: Vec<String>,
    pub(crate) tests_run: Vec<String>,
    pub(crate) follow_up: Option<String>,
    pub(crate) error_text: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct NewMaintainerRunInput {
    pub(crate) case_id: i64,
    pub(crate) run_kind: MaintainerRunKind,
    pub(crate) tool: String,
    pub(crate) owner_tool: String,
    pub(crate) repo_path: String,
    pub(crate) codex_path: String,
    pub(crate) prompt_path: String,
    pub(crate) schema_path: String,
    pub(crate) result_path: String,
    pub(crate) stdout_path: String,
    pub(crate) stderr_path: String,
    pub(crate) exit_code_path: String,
    pub(crate) launcher_path: String,
}

pub(crate) fn state_db_path() -> Result<PathBuf> {
    Ok(ensure_state_dir()?.join("state.db"))
}

pub(crate) fn open_db() -> Result<Connection> {
    let path = state_db_path()?;
    let conn = Connection::open(&path)
        .with_context(|| format!("failed to open state db at {}", path.display()))?;
    conn.pragma_update(None, "foreign_keys", "ON")
        .context("failed to enable sqlite foreign keys")?;
    init_db(&conn)?;
    Ok(conn)
}

pub(crate) fn now_millis() -> Result<i64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before the unix epoch")?;
    let millis = i64::try_from(duration.as_millis()).context("timestamp overflow")?;
    Ok(millis)
}

fn init_db(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS cases (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tool TEXT NOT NULL,
            summary TEXT NOT NULL,
            details TEXT,
            dedupe_key TEXT NOT NULL,
            source_thread_id TEXT,
            source_session_id TEXT,
            cwd TEXT,
            command_text TEXT,
            status TEXT NOT NULL,
            occurrence_count INTEGER NOT NULL DEFAULT 1,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            last_seen_at INTEGER NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_cases_dedupe_status
            ON cases (dedupe_key, status);

        CREATE TABLE IF NOT EXISTS case_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            case_id INTEGER NOT NULL REFERENCES cases(id) ON DELETE CASCADE,
            kind TEXT NOT NULL,
            from_status TEXT,
            to_status TEXT,
            message TEXT NOT NULL,
            created_at INTEGER NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_case_events_case
            ON case_events (case_id, id);

        CREATE TABLE IF NOT EXISTS event_deliveries (
            event_id INTEGER NOT NULL REFERENCES case_events(id) ON DELETE CASCADE,
            transport TEXT NOT NULL,
            delivered_at INTEGER NOT NULL,
            payload_json TEXT NOT NULL,
            PRIMARY KEY (event_id, transport)
        );
        "#,
    )
    .context("failed to initialize sqlite schema")?;
    ensure_maintainer_runs_schema(conn)?;
    Ok(())
}

fn ensure_maintainer_runs_schema(conn: &Connection) -> Result<()> {
    if !table_exists(conn, "maintainer_runs")? {
        create_maintainer_runs_table(conn)?;
        return Ok(());
    }
    if !table_has_column(conn, "maintainer_runs", "run_kind")? {
        migrate_maintainer_runs_to_v2(conn)?;
        return Ok(());
    }
    create_maintainer_runs_indexes(conn)?;
    Ok(())
}

fn create_maintainer_runs_table(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS maintainer_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            case_id INTEGER NOT NULL REFERENCES cases(id) ON DELETE CASCADE,
            run_kind TEXT NOT NULL,
            tool TEXT NOT NULL,
            owner_tool TEXT NOT NULL,
            repo_path TEXT NOT NULL,
            status TEXT NOT NULL,
            queued_at INTEGER NOT NULL,
            started_at INTEGER,
            updated_at INTEGER NOT NULL,
            completed_at INTEGER,
            pid INTEGER,
            codex_path TEXT NOT NULL,
            prompt_path TEXT NOT NULL,
            schema_path TEXT NOT NULL,
            result_path TEXT NOT NULL,
            stdout_path TEXT NOT NULL,
            stderr_path TEXT NOT NULL,
            exit_code_path TEXT NOT NULL,
            launcher_path TEXT NOT NULL,
            exit_code INTEGER,
            result_status TEXT,
            resolution TEXT,
            result_summary TEXT,
            files_changed_json TEXT,
            tests_run_json TEXT,
            follow_up TEXT,
            error_text TEXT
        );
        "#,
    )
    .context("failed to create maintainer_runs table")?;
    create_maintainer_runs_indexes(conn)
}

fn create_maintainer_runs_indexes(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        CREATE INDEX IF NOT EXISTS idx_maintainer_runs_status_tool
            ON maintainer_runs (status, tool, id);
        CREATE INDEX IF NOT EXISTS idx_maintainer_runs_case_kind
            ON maintainer_runs (case_id, run_kind, id);
        "#,
    )
    .context("failed to create maintainer_runs indexes")?;
    Ok(())
}

fn migrate_maintainer_runs_to_v2(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        ALTER TABLE maintainer_runs RENAME TO maintainer_runs_old;
        "#,
    )
    .context("failed to rename old maintainer_runs table")?;
    create_maintainer_runs_table(conn)?;
    conn.execute_batch(
        r#"
        INSERT INTO maintainer_runs (
            case_id, run_kind, tool, owner_tool, repo_path, status, queued_at, started_at, updated_at,
            completed_at, pid, codex_path, prompt_path, schema_path, result_path, stdout_path,
            stderr_path, exit_code_path, launcher_path, exit_code, result_status, resolution,
            result_summary, files_changed_json, tests_run_json, follow_up, error_text
        )
        SELECT
            case_id, 'triage', tool, owner_tool, repo_path, status, queued_at, started_at, updated_at,
            completed_at, pid, codex_path, prompt_path, schema_path, result_path, stdout_path,
            stderr_path, exit_code_path, launcher_path, exit_code, result_status, resolution,
            result_summary, files_changed_json, tests_run_json, follow_up, error_text
        FROM maintainer_runs_old;

        DROP TABLE maintainer_runs_old;
        "#,
    )
    .context("failed to migrate maintainer_runs to v2")?;
    Ok(())
}

fn table_exists(conn: &Connection, table_name: &str) -> Result<bool> {
    let exists = conn
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1 LIMIT 1",
            params![table_name],
            |_| Ok(()),
        )
        .optional()
        .context("failed to query sqlite_master")?
        .is_some();
    Ok(exists)
}

fn table_has_column(conn: &Connection, table_name: &str, column_name: &str) -> Result<bool> {
    let pragma = format!("PRAGMA table_info({table_name})");
    let mut stmt = conn
        .prepare(&pragma)
        .with_context(|| format!("failed to inspect schema for table `{table_name}`"))?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let name: String = row.get(1)?;
        if name == column_name {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(crate) fn public_case_id(id: i64) -> String {
    format!("case_{id:06}")
}

pub(crate) fn parse_case_id(raw: &str) -> Result<i64> {
    let trimmed = raw.trim();
    let numeric = trimmed.strip_prefix("case_").unwrap_or(trimmed);
    numeric
        .parse::<i64>()
        .with_context(|| format!("invalid case id `{trimmed}`"))
}

pub(crate) fn submit_case(
    conn: &Connection,
    input: NewCaseInput,
    now: i64,
) -> Result<SubmitOutcome> {
    let tool = input.tool.trim();
    let summary = input.summary.trim();
    if tool.is_empty() {
        bail!("tool cannot be empty");
    }
    if summary.is_empty() {
        bail!("summary cannot be empty");
    }
    let dedupe_key = normalized_dedupe_key(tool, summary, input.dedupe_key.as_deref());
    if let Some(existing_id) = find_open_case_by_dedupe_key(conn, &dedupe_key)? {
        conn.execute(
            "UPDATE cases
             SET occurrence_count = occurrence_count + 1,
                 last_seen_at = ?1,
                 updated_at = ?1
             WHERE id = ?2",
            params![now, existing_id],
        )
        .context("failed to update deduped case")?;
        insert_case_event(
            conn,
            existing_id,
            "deduped_submission",
            None,
            None,
            format!("Observed another submission for `{summary}`"),
            now,
        )?;
        return Ok(SubmitOutcome {
            created: false,
            case_record: load_case(conn, existing_id)?
                .context("deduped case disappeared after update")?,
        });
    }

    conn.execute(
        "INSERT INTO cases (
            tool, summary, details, dedupe_key, source_thread_id, source_session_id,
            cwd, command_text, status, occurrence_count, created_at, updated_at, last_seen_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 1, ?10, ?10, ?10)",
        params![
            tool,
            summary,
            input.details,
            dedupe_key,
            input.source_thread_id,
            input.source_session_id,
            input.cwd,
            input.command_text,
            CaseStatus::New.as_str(),
            now,
        ],
    )
    .context("failed to insert case")?;
    let case_id = conn.last_insert_rowid();
    insert_case_event(
        conn,
        case_id,
        "submitted",
        None,
        Some(CaseStatus::New),
        format!("Submitted feedback for `{summary}`"),
        now,
    )?;
    Ok(SubmitOutcome {
        created: true,
        case_record: load_case(conn, case_id)?.context("inserted case disappeared after submit")?,
    })
}

pub(crate) fn list_cases(
    conn: &Connection,
    status_filter: Option<CaseStatus>,
) -> Result<Vec<CaseRecord>> {
    let mut cases = Vec::new();
    if let Some(status) = status_filter {
        let mut stmt = conn.prepare(
            "SELECT id, tool, summary, details, dedupe_key, source_thread_id, source_session_id,
                    cwd, command_text, status, occurrence_count, created_at, updated_at, last_seen_at
             FROM cases
             WHERE status = ?1
             ORDER BY updated_at DESC, id DESC",
        )?;
        let rows = stmt.query_map(params![status.as_str()], row_to_case_record)?;
        for row in rows {
            cases.push(row?);
        }
    } else {
        let mut stmt = conn.prepare(
            "SELECT id, tool, summary, details, dedupe_key, source_thread_id, source_session_id,
                    cwd, command_text, status, occurrence_count, created_at, updated_at, last_seen_at
             FROM cases
             ORDER BY updated_at DESC, id DESC",
        )?;
        let rows = stmt.query_map([], row_to_case_record)?;
        for row in rows {
            cases.push(row?);
        }
    }
    Ok(cases)
}

pub(crate) fn load_case(conn: &Connection, case_id: i64) -> Result<Option<CaseRecord>> {
    conn.query_row(
        "SELECT id, tool, summary, details, dedupe_key, source_thread_id, source_session_id,
                cwd, command_text, status, occurrence_count, created_at, updated_at, last_seen_at
         FROM cases
         WHERE id = ?1",
        params![case_id],
        row_to_case_record,
    )
    .optional()
    .context("failed to load case")
}

pub(crate) fn transition_case(
    conn: &Connection,
    case_id: i64,
    new_status: CaseStatus,
    message: String,
    now: i64,
) -> Result<CaseRecord> {
    let existing = load_case(conn, case_id)?.context("case not found")?;
    let current_status = CaseStatus::parse(&existing.status)?;
    validate_transition(current_status, new_status)?;
    conn.execute(
        "UPDATE cases
         SET status = ?1,
             updated_at = ?2,
             last_seen_at = ?2
         WHERE id = ?3",
        params![new_status.as_str(), now, case_id],
    )
    .context("failed to update case status")?;
    insert_case_event(
        conn,
        case_id,
        "status_changed",
        Some(current_status),
        Some(new_status),
        message,
        now,
    )?;
    load_case(conn, case_id)?.context("case disappeared after transition")
}

pub(crate) fn append_case_note(
    conn: &Connection,
    case_id: i64,
    message: String,
    now: i64,
) -> Result<CaseRecord> {
    load_case(conn, case_id)?.context("case not found")?;
    conn.execute(
        "UPDATE cases
         SET updated_at = ?1,
             last_seen_at = ?1
         WHERE id = ?2",
        params![now, case_id],
    )
    .context("failed to touch case timestamp")?;
    insert_case_event(conn, case_id, "note", None, None, message, now)?;
    load_case(conn, case_id)?.context("case disappeared after note")
}

pub(crate) fn recent_case_events(
    conn: &Connection,
    case_id: i64,
    limit: usize,
) -> Result<Vec<CaseEventRecord>> {
    let mut stmt = conn.prepare(
        "SELECT e.id, c.id, c.tool, c.summary, e.kind, e.from_status, e.to_status, e.message, e.created_at
         FROM case_events e
         JOIN cases c ON c.id = e.case_id
         WHERE c.id = ?1
         ORDER BY e.id DESC
         LIMIT ?2",
    )?;
    let rows = stmt.query_map(params![case_id, limit as i64], row_to_case_event_record)?;
    let mut events = Vec::new();
    for row in rows {
        events.push(row?);
    }
    Ok(events)
}

pub(crate) fn pending_notification_events(conn: &Connection) -> Result<Vec<CaseEventRecord>> {
    let mut stmt = conn.prepare(
        "SELECT e.id, c.id, c.tool, c.summary, e.kind, e.from_status, e.to_status, e.message, e.created_at
         FROM case_events e
         JOIN cases c ON c.id = e.case_id
         WHERE e.kind = 'status_changed'
           AND e.to_status IN (
             'awaiting_approval', 'blocked', 'completed', 'rejected', 'deferred', 'duplicate'
           )
           AND NOT EXISTS (
             SELECT 1 FROM event_deliveries d
             WHERE d.event_id = e.id AND d.transport = 'telegram'
           )
         ORDER BY e.id ASC",
    )?;
    let rows = stmt.query_map([], row_to_case_event_record)?;
    let mut events = Vec::new();
    for row in rows {
        events.push(row?);
    }
    Ok(events)
}

pub(crate) fn record_event_delivery(
    conn: &Connection,
    event_id: i64,
    transport: &str,
    payload: &Value,
    now: i64,
) -> Result<()> {
    conn.execute(
        "INSERT INTO event_deliveries (event_id, transport, delivered_at, payload_json)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(event_id, transport) DO UPDATE
         SET delivered_at = excluded.delivered_at,
             payload_json = excluded.payload_json",
        params![event_id, transport, now, payload.to_string()],
    )
    .context("failed to record event delivery")?;
    Ok(())
}

pub(crate) fn public_run_id(id: i64) -> String {
    format!("run_{id:06}")
}

pub(crate) fn parse_run_id(raw: &str) -> Result<i64> {
    let trimmed = raw.trim();
    let numeric = trimmed.strip_prefix("run_").unwrap_or(trimmed);
    numeric
        .parse::<i64>()
        .with_context(|| format!("invalid run id `{trimmed}`"))
}

pub(crate) fn cases_ready_for_run(
    conn: &Connection,
    status: CaseStatus,
    run_kind: MaintainerRunKind,
) -> Result<Vec<CaseRecord>> {
    let mut stmt = conn.prepare(
        "SELECT c.id, c.tool, c.summary, c.details, c.dedupe_key, c.source_thread_id, c.source_session_id,
                c.cwd, c.command_text, c.status, c.occurrence_count, c.created_at, c.updated_at, c.last_seen_at
         FROM cases c
         WHERE c.status = ?1
           AND NOT EXISTS (
             SELECT 1
             FROM maintainer_runs r
             WHERE r.case_id = c.id
               AND r.run_kind = ?2
               AND r.status IN ('queued', 'running')
           )
           AND NOT EXISTS (
             SELECT 1
             FROM maintainer_runs r
             WHERE r.case_id = c.id
               AND r.run_kind = ?2
               AND r.status = 'succeeded'
               AND COALESCE(r.completed_at, r.updated_at) >= COALESCE(
                 (
                   SELECT MAX(e.created_at)
                   FROM case_events e
                   WHERE e.case_id = c.id
                     AND e.kind = 'status_changed'
                     AND e.to_status = c.status
                 ),
                 c.created_at
               )
           )
         ORDER BY c.updated_at ASC, c.id ASC",
    )?;
    let rows = stmt.query_map(
        params![status.as_str(), run_kind.as_str()],
        row_to_case_record,
    )?;
    let mut cases = Vec::new();
    for row in rows {
        cases.push(row?);
    }
    Ok(cases)
}

pub(crate) fn create_queued_run(
    conn: &Connection,
    input: NewMaintainerRunInput,
    now: i64,
) -> Result<MaintainerRunRecord> {
    conn.execute(
        "INSERT INTO maintainer_runs (
            case_id, run_kind, tool, owner_tool, repo_path, status, queued_at, started_at, updated_at,
            completed_at, pid, codex_path, prompt_path, schema_path, result_path, stdout_path, stderr_path,
            exit_code_path, launcher_path, exit_code, result_status, resolution, result_summary,
            files_changed_json, tests_run_json, follow_up, error_text
         ) VALUES (
            ?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL, ?7, NULL, NULL, ?8, ?9, ?10, ?11, ?12, ?13, ?14,
            ?15, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
         )",
        params![
            input.case_id,
            input.run_kind.as_str(),
            input.tool,
            input.owner_tool,
            input.repo_path,
            MaintainerRunStatus::Queued.as_str(),
            now,
            input.codex_path,
            input.prompt_path,
            input.schema_path,
            input.result_path,
            input.stdout_path,
            input.stderr_path,
            input.exit_code_path,
            input.launcher_path,
        ],
    )
    .context("failed to insert maintainer run")?;
    load_maintainer_run(conn, conn.last_insert_rowid())?
        .context("queued maintainer run disappeared")
}

pub(crate) fn list_maintainer_runs(
    conn: &Connection,
    status_filter: Option<MaintainerRunStatus>,
    case_filter: Option<i64>,
    kind_filter: Option<MaintainerRunKind>,
) -> Result<Vec<MaintainerRunRecord>> {
    let mut sql = String::from(
        "SELECT id, case_id, run_kind, tool, owner_tool, repo_path, status, queued_at, started_at, updated_at,
                completed_at, pid, codex_path, prompt_path, schema_path, result_path, stdout_path,
                stderr_path, exit_code_path, launcher_path, exit_code, result_status, resolution,
                result_summary, files_changed_json, tests_run_json, follow_up, error_text
         FROM maintainer_runs",
    );
    let mut conditions = Vec::new();
    let mut bind_values = Vec::new();
    if let Some(status) = status_filter {
        conditions.push("status = ?".to_string());
        bind_values.push(SqlValue::Text(status.as_str().to_string()));
    }
    if let Some(case_id) = case_filter {
        conditions.push("case_id = ?".to_string());
        bind_values.push(SqlValue::Integer(case_id));
    }
    if let Some(kind) = kind_filter {
        conditions.push("run_kind = ?".to_string());
        bind_values.push(SqlValue::Text(kind.as_str().to_string()));
    }
    if !conditions.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&conditions.join(" AND "));
    }
    sql.push_str(" ORDER BY id DESC");

    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(params_from_iter(bind_values), row_to_maintainer_run_record)?;
    let mut runs = Vec::new();
    for row in rows {
        runs.push(row?);
    }
    Ok(runs)
}

pub(crate) fn load_maintainer_run(
    conn: &Connection,
    run_id: i64,
) -> Result<Option<MaintainerRunRecord>> {
    conn.query_row(
        "SELECT id, case_id, run_kind, tool, owner_tool, repo_path, status, queued_at, started_at, updated_at,
                completed_at, pid, codex_path, prompt_path, schema_path, result_path, stdout_path,
                stderr_path, exit_code_path, launcher_path, exit_code, result_status, resolution,
                result_summary, files_changed_json, tests_run_json, follow_up, error_text
         FROM maintainer_runs
         WHERE id = ?1",
        params![run_id],
        row_to_maintainer_run_record,
    )
    .optional()
    .context("failed to load maintainer run")
}

pub(crate) fn latest_succeeded_run_for_case(
    conn: &Connection,
    case_id: i64,
    run_kind: MaintainerRunKind,
) -> Result<Option<MaintainerRunRecord>> {
    let mut runs = list_maintainer_runs(
        conn,
        Some(MaintainerRunStatus::Succeeded),
        Some(case_id),
        Some(run_kind),
    )?;
    Ok(runs.drain(..).next())
}

pub(crate) fn mark_maintainer_run_started(
    conn: &Connection,
    run_id: i64,
    pid: i64,
    now: i64,
) -> Result<MaintainerRunRecord> {
    conn.execute(
        "UPDATE maintainer_runs
         SET status = ?1,
             pid = ?2,
             started_at = ?3,
             updated_at = ?3
         WHERE id = ?4",
        params![MaintainerRunStatus::Running.as_str(), pid, now, run_id],
    )
    .context("failed to mark maintainer run as running")?;
    load_maintainer_run(conn, run_id)?.context("running maintainer run disappeared")
}

pub(crate) fn mark_maintainer_run_succeeded(
    conn: &Connection,
    run_id: i64,
    exit_code: i32,
    result: &crate::maintainer::MaintainerResult,
    now: i64,
) -> Result<MaintainerRunRecord> {
    conn.execute(
        "UPDATE maintainer_runs
         SET status = ?1,
             updated_at = ?2,
             completed_at = ?2,
             exit_code = ?3,
             result_status = ?4,
             resolution = ?5,
             result_summary = ?6,
             files_changed_json = ?7,
             tests_run_json = ?8,
             follow_up = ?9,
             error_text = NULL
         WHERE id = ?10",
        params![
            MaintainerRunStatus::Succeeded.as_str(),
            now,
            exit_code,
            result.status,
            result.resolution,
            result.summary,
            serde_json::to_string(&result.files_changed)
                .context("failed to encode files_changed")?,
            serde_json::to_string(&result.tests_run).context("failed to encode tests_run")?,
            result.follow_up,
            run_id,
        ],
    )
    .context("failed to mark maintainer run as succeeded")?;
    load_maintainer_run(conn, run_id)?.context("succeeded maintainer run disappeared")
}

pub(crate) fn mark_maintainer_run_failed(
    conn: &Connection,
    run_id: i64,
    exit_code: i32,
    error_text: &str,
    now: i64,
) -> Result<MaintainerRunRecord> {
    conn.execute(
        "UPDATE maintainer_runs
         SET status = ?1,
             updated_at = ?2,
             completed_at = ?2,
             exit_code = ?3,
             error_text = ?4
         WHERE id = ?5",
        params![
            MaintainerRunStatus::Failed.as_str(),
            now,
            exit_code,
            error_text,
            run_id,
        ],
    )
    .context("failed to mark maintainer run as failed")?;
    load_maintainer_run(conn, run_id)?.context("failed maintainer run disappeared")
}

fn row_to_case_record(row: &rusqlite::Row<'_>) -> rusqlite::Result<CaseRecord> {
    let id: i64 = row.get(0)?;
    Ok(CaseRecord {
        id,
        public_id: public_case_id(id),
        tool: row.get(1)?,
        summary: row.get(2)?,
        details: row.get(3)?,
        dedupe_key: row.get(4)?,
        source_thread_id: row.get(5)?,
        source_session_id: row.get(6)?,
        cwd: row.get(7)?,
        command_text: row.get(8)?,
        status: row.get(9)?,
        occurrence_count: row.get(10)?,
        created_at: row.get(11)?,
        updated_at: row.get(12)?,
        last_seen_at: row.get(13)?,
    })
}

fn row_to_case_event_record(row: &rusqlite::Row<'_>) -> rusqlite::Result<CaseEventRecord> {
    let case_id: i64 = row.get(1)?;
    Ok(CaseEventRecord {
        id: row.get(0)?,
        case_id,
        case_public_id: public_case_id(case_id),
        tool: row.get(2)?,
        case_summary: row.get(3)?,
        kind: row.get(4)?,
        from_status: row.get(5)?,
        to_status: row.get(6)?,
        message: row.get(7)?,
        created_at: row.get(8)?,
    })
}

fn row_to_maintainer_run_record(row: &rusqlite::Row<'_>) -> rusqlite::Result<MaintainerRunRecord> {
    let id: i64 = row.get(0)?;
    let case_id: i64 = row.get(1)?;
    let files_changed_json: Option<String> = row.get(24)?;
    let tests_run_json: Option<String> = row.get(25)?;
    Ok(MaintainerRunRecord {
        id,
        public_id: public_run_id(id),
        case_id,
        case_public_id: public_case_id(case_id),
        run_kind: row.get(2)?,
        tool: row.get(3)?,
        owner_tool: row.get(4)?,
        repo_path: row.get(5)?,
        status: row.get(6)?,
        queued_at: row.get(7)?,
        started_at: row.get(8)?,
        updated_at: row.get(9)?,
        completed_at: row.get(10)?,
        pid: row.get(11)?,
        codex_path: row.get(12)?,
        prompt_path: row.get(13)?,
        schema_path: row.get(14)?,
        result_path: row.get(15)?,
        stdout_path: row.get(16)?,
        stderr_path: row.get(17)?,
        exit_code_path: row.get(18)?,
        launcher_path: row.get(19)?,
        exit_code: row.get(20)?,
        result_status: row.get(21)?,
        resolution: row.get(22)?,
        result_summary: row.get(23)?,
        files_changed: parse_string_vec(files_changed_json.as_deref())?,
        tests_run: parse_string_vec(tests_run_json.as_deref())?,
        follow_up: row.get(26)?,
        error_text: row.get(27)?,
    })
}

fn parse_string_vec(raw: Option<&str>) -> rusqlite::Result<Vec<String>> {
    match raw {
        Some(value) if !value.trim().is_empty() => serde_json::from_str(value).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                value.len(),
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        }),
        _ => Ok(Vec::new()),
    }
}

fn insert_case_event(
    conn: &Connection,
    case_id: i64,
    kind: &str,
    from_status: Option<CaseStatus>,
    to_status: Option<CaseStatus>,
    message: String,
    now: i64,
) -> Result<()> {
    conn.execute(
        "INSERT INTO case_events (case_id, kind, from_status, to_status, message, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            case_id,
            kind,
            from_status.map(CaseStatus::as_str),
            to_status.map(CaseStatus::as_str),
            message,
            now
        ],
    )
    .context("failed to insert case event")?;
    Ok(())
}

fn normalized_dedupe_key(tool: &str, summary: &str, explicit: Option<&str>) -> String {
    explicit
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| {
            format!(
                "{}|{}",
                tool.trim().to_ascii_lowercase(),
                summary.trim().to_ascii_lowercase()
            )
        })
}

fn find_open_case_by_dedupe_key(conn: &Connection, dedupe_key: &str) -> Result<Option<i64>> {
    let mut stmt = conn.prepare(
        "SELECT id, status
         FROM cases
         WHERE dedupe_key = ?1
         ORDER BY updated_at DESC, id DESC",
    )?;
    let mut rows = stmt.query(params![dedupe_key])?;
    while let Some(row) = rows.next()? {
        let id: i64 = row.get(0)?;
        let status = CaseStatus::parse(&row.get::<_, String>(1)?)?;
        if status.is_open() {
            return Ok(Some(id));
        }
    }
    Ok(None)
}

fn validate_transition(current: CaseStatus, next: CaseStatus) -> Result<()> {
    if current == next {
        bail!("case is already `{}`", current.as_str());
    }
    let allowed = match current {
        CaseStatus::New => matches!(
            next,
            CaseStatus::Accepted
                | CaseStatus::Blocked
                | CaseStatus::Rejected
                | CaseStatus::Deferred
                | CaseStatus::Duplicate
        ),
        CaseStatus::Accepted => matches!(
            next,
            CaseStatus::AwaitingApproval
                | CaseStatus::Blocked
                | CaseStatus::Completed
                | CaseStatus::Rejected
                | CaseStatus::Deferred
                | CaseStatus::Duplicate
        ),
        CaseStatus::AwaitingApproval => matches!(
            next,
            CaseStatus::PatchApproved
                | CaseStatus::Blocked
                | CaseStatus::Rejected
                | CaseStatus::Deferred
                | CaseStatus::Duplicate
        ),
        CaseStatus::PatchApproved => matches!(
            next,
            CaseStatus::Blocked
                | CaseStatus::Completed
                | CaseStatus::Rejected
                | CaseStatus::Deferred
                | CaseStatus::Duplicate
        ),
        CaseStatus::Blocked => matches!(
            next,
            CaseStatus::Accepted
                | CaseStatus::AwaitingApproval
                | CaseStatus::PatchApproved
                | CaseStatus::Completed
                | CaseStatus::Rejected
                | CaseStatus::Deferred
                | CaseStatus::Duplicate
        ),
        CaseStatus::Deferred => matches!(
            next,
            CaseStatus::Accepted
                | CaseStatus::AwaitingApproval
                | CaseStatus::PatchApproved
                | CaseStatus::Blocked
                | CaseStatus::Rejected
                | CaseStatus::Duplicate
        ),
        CaseStatus::Completed | CaseStatus::Rejected | CaseStatus::Duplicate => false,
    };
    if !allowed {
        bail!(
            "cannot transition case from `{}` to `{}`",
            current.as_str(),
            next.as_str()
        );
    }
    Ok(())
}

pub(crate) fn format_case_summary(case_record: &CaseRecord) -> Value {
    json!({
        "id": case_record.public_id,
        "tool": case_record.tool,
        "summary": case_record.summary,
        "status": case_record.status,
        "occurrenceCount": case_record.occurrence_count,
        "sourceThreadId": case_record.source_thread_id,
        "sourceSessionId": case_record.source_session_id,
        "cwd": case_record.cwd,
        "command": case_record.command_text,
        "createdAt": case_record.created_at,
        "updatedAt": case_record.updated_at,
        "details": case_record.details
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::test_env_lock;
    use std::env;
    use tempfile::TempDir;

    fn with_temp_db<T>(f: impl FnOnce(Connection) -> T) -> T {
        let _guard = test_env_lock()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let temp = TempDir::new().expect("temp dir");
        let previous = env::var_os("CODEX_HOME");
        unsafe {
            env::set_var("CODEX_HOME", temp.path());
        }
        let conn = open_db().expect("open db");
        let result = f(conn);
        unsafe {
            match previous {
                Some(value) => env::set_var("CODEX_HOME", value),
                None => env::remove_var("CODEX_HOME"),
            }
        }
        result
    }

    #[test]
    fn submit_dedupes_open_case() {
        with_temp_db(|conn| {
            let now = 1_700_000_000_000i64;
            let first = submit_case(
                &conn,
                NewCaseInput {
                    tool: "codex-recall".into(),
                    summary: "Search is noisy".into(),
                    details: None,
                    dedupe_key: None,
                    source_thread_id: None,
                    source_session_id: None,
                    cwd: None,
                    command_text: None,
                },
                now,
            )
            .expect("first submit");
            assert!(first.created);

            let second = submit_case(
                &conn,
                NewCaseInput {
                    tool: "codex-recall".into(),
                    summary: "Search is noisy".into(),
                    details: Some("same issue".into()),
                    dedupe_key: None,
                    source_thread_id: None,
                    source_session_id: None,
                    cwd: None,
                    command_text: None,
                },
                now + 5,
            )
            .expect("second submit");
            assert!(!second.created);
            assert_eq!(second.case_record.public_id, first.case_record.public_id);
            assert_eq!(second.case_record.occurrence_count, 2);
        });
    }

    #[test]
    fn transitioned_case_creates_pending_notification_once() {
        with_temp_db(|conn| {
            let now = 1_700_000_000_000i64;
            let submitted = submit_case(
                &conn,
                NewCaseInput {
                    tool: "codex-recall".into(),
                    summary: "Search is noisy".into(),
                    details: None,
                    dedupe_key: Some("case-a".into()),
                    source_thread_id: None,
                    source_session_id: None,
                    cwd: None,
                    command_text: None,
                },
                now,
            )
            .expect("submit");
            let id = submitted.case_record.id;
            transition_case(
                &conn,
                id,
                CaseStatus::Accepted,
                "Accepted for triage".into(),
                now + 1,
            )
            .expect("accept");
            assert!(
                pending_notification_events(&conn)
                    .expect("pending")
                    .is_empty()
            );

            let updated = transition_case(
                &conn,
                id,
                CaseStatus::AwaitingApproval,
                "Patch looks warranted".into(),
                now + 2,
            )
            .expect("transition");
            assert_eq!(updated.status, "awaiting_approval");

            let pending = pending_notification_events(&conn).expect("pending events");
            assert_eq!(pending.len(), 1);
            assert_eq!(pending[0].to_status.as_deref(), Some("awaiting_approval"));

            record_event_delivery(
                &conn,
                pending[0].id,
                "telegram",
                &json!({"ok": true}),
                now + 3,
            )
            .expect("record delivery");
            let pending_after = pending_notification_events(&conn).expect("pending after");
            assert!(pending_after.is_empty());
        });
    }

    #[test]
    fn cases_queue_triage_then_patch() {
        with_temp_db(|conn| {
            let now = 1_700_000_000_000i64;
            let submitted = submit_case(
                &conn,
                NewCaseInput {
                    tool: "tool-feedback".into(),
                    summary: "Synthetic maintainer test".into(),
                    details: Some("no-op".into()),
                    dedupe_key: Some("maintainer-test".into()),
                    source_thread_id: None,
                    source_session_id: None,
                    cwd: None,
                    command_text: None,
                },
                now,
            )
            .expect("submit");

            transition_case(
                &conn,
                submitted.case_record.id,
                CaseStatus::Accepted,
                "Accepted for triage".into(),
                now + 1,
            )
            .expect("accept");

            let triage_ready =
                cases_ready_for_run(&conn, CaseStatus::Accepted, MaintainerRunKind::Triage)
                    .expect("triage ready");
            assert_eq!(triage_ready.len(), 1);

            let queued_triage = create_queued_run(
                &conn,
                NewMaintainerRunInput {
                    case_id: submitted.case_record.id,
                    run_kind: MaintainerRunKind::Triage,
                    tool: "tool-feedback".into(),
                    owner_tool: "tool-feedback".into(),
                    repo_path: "/tmp/tool-feedback".into(),
                    codex_path: "/tmp/codex".into(),
                    prompt_path: "/tmp/prompt-triage.md".into(),
                    schema_path: "/tmp/schema-triage.json".into(),
                    result_path: "/tmp/result-triage.json".into(),
                    stdout_path: "/tmp/stdout-triage.log".into(),
                    stderr_path: "/tmp/stderr-triage.log".into(),
                    exit_code_path: "/tmp/exit_code-triage".into(),
                    launcher_path: "/tmp/launcher-triage.sh".into(),
                },
                now + 2,
            )
            .expect("queue triage");
            assert_eq!(queued_triage.run_kind, "triage");

            let triage_ready_after =
                cases_ready_for_run(&conn, CaseStatus::Accepted, MaintainerRunKind::Triage)
                    .expect("triage ready after");
            assert!(triage_ready_after.is_empty());

            transition_case(
                &conn,
                submitted.case_record.id,
                CaseStatus::AwaitingApproval,
                "Patch looks warranted".into(),
                now + 3,
            )
            .expect("awaiting approval");
            transition_case(
                &conn,
                submitted.case_record.id,
                CaseStatus::PatchApproved,
                "Approved".into(),
                now + 4,
            )
            .expect("patch approved");

            let patch_ready =
                cases_ready_for_run(&conn, CaseStatus::PatchApproved, MaintainerRunKind::Patch)
                    .expect("patch ready");
            assert_eq!(patch_ready.len(), 1);
        });
    }

    #[test]
    fn migrates_old_maintainer_runs_schema() {
        let _guard = test_env_lock()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let temp = TempDir::new().expect("temp dir");
        let previous = env::var_os("CODEX_HOME");
        unsafe {
            env::set_var("CODEX_HOME", temp.path());
        }

        let db_path = state_db_path().expect("db path");
        let legacy = Connection::open(&db_path).expect("legacy open");
        legacy
            .execute_batch(
                r#"
                CREATE TABLE cases (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tool TEXT NOT NULL,
                    summary TEXT NOT NULL,
                    details TEXT,
                    dedupe_key TEXT NOT NULL,
                    source_thread_id TEXT,
                    source_session_id TEXT,
                    cwd TEXT,
                    command_text TEXT,
                    status TEXT NOT NULL,
                    occurrence_count INTEGER NOT NULL DEFAULT 1,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    last_seen_at INTEGER NOT NULL
                );

                INSERT INTO cases (
                    id, tool, summary, details, dedupe_key, source_thread_id, source_session_id,
                    cwd, command_text, status, occurrence_count, created_at, updated_at, last_seen_at
                ) VALUES (
                    1, 'tool-feedback', 'legacy run', NULL, 'legacy', NULL, NULL, NULL, NULL,
                    'blocked', 1, 1, 2, 2
                );

                CREATE TABLE maintainer_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    case_id INTEGER NOT NULL UNIQUE REFERENCES cases(id) ON DELETE CASCADE,
                    tool TEXT NOT NULL,
                    owner_tool TEXT NOT NULL,
                    repo_path TEXT NOT NULL,
                    status TEXT NOT NULL,
                    queued_at INTEGER NOT NULL,
                    started_at INTEGER,
                    updated_at INTEGER NOT NULL,
                    completed_at INTEGER,
                    pid INTEGER,
                    codex_path TEXT NOT NULL,
                    prompt_path TEXT NOT NULL,
                    schema_path TEXT NOT NULL,
                    result_path TEXT NOT NULL,
                    stdout_path TEXT NOT NULL,
                    stderr_path TEXT NOT NULL,
                    exit_code_path TEXT NOT NULL,
                    launcher_path TEXT NOT NULL,
                    exit_code INTEGER,
                    result_status TEXT,
                    resolution TEXT,
                    result_summary TEXT,
                    files_changed_json TEXT,
                    tests_run_json TEXT,
                    follow_up TEXT,
                    error_text TEXT
                );

                INSERT INTO maintainer_runs (
                    case_id, tool, owner_tool, repo_path, status, queued_at, started_at, updated_at,
                    completed_at, pid, codex_path, prompt_path, schema_path, result_path, stdout_path,
                    stderr_path, exit_code_path, launcher_path, exit_code, result_status, resolution,
                    result_summary, files_changed_json, tests_run_json, follow_up, error_text
                ) VALUES (
                    1, 'tool-feedback', 'tool-feedback', '/tmp/repo', 'succeeded', 1, 1, 2, 2, NULL,
                    '/tmp/codex', '/tmp/prompt', '/tmp/schema', '/tmp/result', '/tmp/stdout',
                    '/tmp/stderr', '/tmp/exit', '/tmp/launcher', 0, 'blocked', 'legacy',
                    'legacy summary', '[]', '[]', NULL, NULL
                );
                "#,
            )
            .expect("seed legacy schema");
        drop(legacy);

        let conn = open_db().expect("migrated open");
        let run = load_maintainer_run(&conn, 1)
            .expect("load run")
            .expect("run exists");
        assert_eq!(run.run_kind, "triage");

        unsafe {
            match previous {
                Some(value) => env::set_var("CODEX_HOME", value),
                None => env::remove_var("CODEX_HOME"),
            }
        }
    }
}
