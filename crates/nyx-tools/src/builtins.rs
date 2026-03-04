#[cfg(feature = "terminal")]
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
#[cfg(feature = "shell")]
use std::time::Duration;

use async_trait::async_trait;
use nyx_security::{Sandbox, SandboxedCommand};
use serde_json::{Value, json};

#[cfg(feature = "sub-agent")]
use crate::SubAgentTool;
#[cfg(feature = "terminal")]
use crate::TerminalRegistry;
use crate::{RegistryError, SkillTool, Tool, ToolContext, ToolError, ToolRegistry, ToolResult};

pub fn register_builtins(
    registry: &mut ToolRegistry,
    sandbox: Arc<dyn Sandbox>,
    #[cfg(feature = "terminal")] terminal_registry: Arc<TerminalRegistry>,
) -> Result<(), RegistryError> {
    let _ = sandbox;
    #[cfg(feature = "terminal")]
    let _ = terminal_registry;

    #[cfg(feature = "file")]
    {
        registry.register(Arc::new(FileReadTool))?;
        registry.register(Arc::new(FileWriteTool))?;
        registry.register(Arc::new(FileApplyPatchTool))?;
    }

    #[cfg(feature = "shell")]
    registry.register(Arc::new(ShellTool))?;

    #[cfg(feature = "terminal")]
    {
        registry.register(Arc::new(ProcessStartTool::new(Arc::clone(
            &terminal_registry,
        ))))?;
        registry.register(Arc::new(ProcessReadTool::new(Arc::clone(
            &terminal_registry,
        ))))?;
        registry.register(Arc::new(ProcessWaitTool::new(Arc::clone(
            &terminal_registry,
        ))))?;
        registry.register(Arc::new(ProcessKillTool::new(Arc::clone(
            &terminal_registry,
        ))))?;
        registry.register(Arc::new(ProcessListTool::new(terminal_registry)))?;
    }

    #[cfg(feature = "http")]
    registry.register(Arc::new(HttpTool::default()))?;

    registry.register(Arc::new(SkillTool))?;
    #[cfg(feature = "sub-agent")]
    registry.register(Arc::new(SubAgentTool))?;

    Ok(())
}

#[cfg(feature = "file")]
#[derive(Debug, Default)]
pub struct FileReadTool;

#[cfg(feature = "file")]
impl FileReadTool {
    /// Maximum number of lines returned in a single read (without explicit range).
    const MAX_LINES: usize = 500;
    /// Maximum characters per line before truncation.
    const MAX_LINE_LEN: usize = 2000;
    /// Maximum total characters returned.
    const MAX_TOTAL_CHARS: usize = 100_000;
}

#[cfg(feature = "file")]
#[async_trait]
impl Tool for FileReadTool {
    fn name(&self) -> &str {
        "read"
    }

    fn description(&self) -> &str {
        "Read file content from disk. Supports optional line range to avoid flooding context. \
         Returns at most 500 lines per call. Use offset/limit to paginate large files."
    }

    fn schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["path"],
            "properties": {
                "path": {
                    "type": "string",
                    "description": "File path (relative to workspace or absolute)"
                },
                "offset": {
                    "type": "integer",
                    "description": "1-based starting line number (default: 1)"
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum number of lines to return (default/max: 500)"
                }
            }
        })
    }

    async fn invoke(&self, input: Value, ctx: &ToolContext) -> Result<ToolResult, ToolError> {
        let path = input
            .get("path")
            .and_then(Value::as_str)
            .ok_or_else(|| ToolError::InvalidInput("missing path".to_string()))?;
        let path = Path::new(path);
        let resolved_path = if path.is_relative() {
            ctx.workspace_dir.join(path)
        } else {
            path.to_path_buf()
        };
        let resolved_path = ctx.sandbox.validate_read_path(&resolved_path)?;
        let content = tokio::fs::read_to_string(&resolved_path).await?;

        let offset = input
            .get("offset")
            .and_then(Value::as_u64)
            .map(|v| v.max(1) as usize)
            .unwrap_or(1);

        let limit = input
            .get("limit")
            .and_then(Value::as_u64)
            .map(|v| (v as usize).min(Self::MAX_LINES))
            .unwrap_or(Self::MAX_LINES);

        let all_lines: Vec<&str> = content.lines().collect();
        let total_lines = all_lines.len();

        // Select the requested range (offset is 1-based).
        let start = (offset - 1).min(total_lines);
        let end = (start + limit).min(total_lines);
        let selected = &all_lines[start..end];

        // Build output with line numbers and per-line truncation.
        let mut out = String::new();
        let mut total_chars: usize = 0;
        let mut lines_included: usize = 0;

        for (i, line) in selected.iter().enumerate() {
            let line_num = start + i + 1; // 1-based
            let truncated = if line.len() > Self::MAX_LINE_LEN {
                format!(
                    "{}… [truncated, {} chars total]",
                    &line[..Self::MAX_LINE_LEN],
                    line.len()
                )
            } else {
                line.to_string()
            };

            let formatted = format!("{line_num}\t{truncated}\n");
            total_chars += formatted.len();
            if total_chars > Self::MAX_TOTAL_CHARS {
                out.push_str(&format!(
                    "… [output truncated at {lines_included} lines due to size limit]\n"
                ));
                break;
            }
            out.push_str(&formatted);
            lines_included += 1;
        }

        // Append a summary so the agent knows what it got vs what exists.
        let showing_end = start + lines_included;
        let truncated = showing_end < total_lines;
        out.push_str(&format!(
            "\n--- showing lines {}-{} of {} total",
            start + 1,
            showing_end,
            total_lines
        ));
        if truncated {
            out.push_str(&format!(" (use offset={} to see more)", showing_end + 1));
        }
        out.push_str(" ---\n");

        Ok(ToolResult::text(out))
    }
}

#[cfg(feature = "file")]
#[derive(Debug, Default)]
pub struct FileWriteTool;

#[cfg(feature = "file")]
#[async_trait]
impl Tool for FileWriteTool {
    fn name(&self) -> &str {
        "write"
    }

    fn description(&self) -> &str {
        "Write file content to disk"
    }

    fn schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["path", "content"],
            "properties": {
                "path": { "type": "string" },
                "content": { "type": "string" }
            }
        })
    }

    async fn invoke(&self, input: Value, ctx: &ToolContext) -> Result<ToolResult, ToolError> {
        let path = input
            .get("path")
            .and_then(Value::as_str)
            .ok_or_else(|| ToolError::InvalidInput("missing path".to_string()))?;
        let content = input
            .get("content")
            .and_then(Value::as_str)
            .ok_or_else(|| ToolError::InvalidInput("missing content".to_string()))?;

        let path = Path::new(path);
        let resolved_path = if path.is_relative() {
            ctx.workspace_dir.join(path)
        } else {
            path.to_path_buf()
        };
        let resolved_path = ctx.sandbox.validate_write_path(&resolved_path)?;

        if let Some(parent) = resolved_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(&resolved_path, content).await?;

        Ok(ToolResult::json(json!({
            "path": resolved_path,
            "status": "written"
        })))
    }
}

#[cfg(feature = "file")]
#[derive(Debug, Default)]
pub struct FileApplyPatchTool;

#[cfg(feature = "file")]
#[async_trait]
impl Tool for FileApplyPatchTool {
    fn name(&self) -> &str {
        "apply_patch"
    }

    fn description(&self) -> &str {
        "Apply a unified diff patch to a file"
    }

    fn schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["path", "patch"],
            "properties": {
                "path": { "type": "string" },
                "patch": { "type": "string" }
            }
        })
    }

    async fn invoke(&self, input: Value, ctx: &ToolContext) -> Result<ToolResult, ToolError> {
        let path = input
            .get("path")
            .and_then(Value::as_str)
            .ok_or_else(|| ToolError::InvalidInput("missing path".to_string()))?;
        let patch = input
            .get("patch")
            .and_then(Value::as_str)
            .ok_or_else(|| ToolError::InvalidInput("missing patch".to_string()))?;

        let file_path = Path::new(path);
        let resolved_path = if file_path.is_relative() {
            ctx.workspace_dir.join(file_path)
        } else {
            file_path.to_path_buf()
        };
        let resolved_path = ctx.sandbox.validate_write_path(&resolved_path)?;

        let current = if tokio::fs::try_exists(&resolved_path).await? {
            tokio::fs::read_to_string(&resolved_path).await?
        } else {
            String::new()
        };
        let (patched, lines_changed) = apply_unified_patch(&current, patch)?;

        if let Some(parent) = resolved_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(&resolved_path, patched).await?;

        Ok(ToolResult::json(json!({
            "path": resolved_path,
            "status": "patched",
            "lines_changed": lines_changed
        })))
    }
}

#[cfg(feature = "shell")]
#[derive(Debug, Default)]
pub struct ShellTool;

#[cfg(feature = "shell")]
#[async_trait]
impl Tool for ShellTool {
    fn name(&self) -> &str {
        "shell"
    }

    fn description(&self) -> &str {
        "Execute a shell command (30s limit). Use process_start for long-running tasks."
    }

    fn schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["command"],
            "properties": {
                "command": { "type": "string" },
                "pwd": { "type": "string" }
            }
        })
    }

    async fn invoke(&self, input: Value, ctx: &ToolContext) -> Result<ToolResult, ToolError> {
        let command_text = input
            .get("command")
            .and_then(Value::as_str)
            .ok_or_else(|| ToolError::InvalidInput("missing command".to_string()))?;
        let default_pwd = ctx.workspace_dir.to_str().unwrap_or(".");
        let pwd = input
            .get("pwd")
            .and_then(Value::as_str)
            .unwrap_or(default_pwd);

        let mut command = SandboxedCommand::new("sh")
            .arg("-lc")
            .arg(command_text.to_string());
        command = command.working_dir(pwd);
        let output = match tokio::time::timeout(
            Duration::from_secs(30),
            ctx.sandbox.execute(command),
        )
        .await
        {
            Ok(output) => output?,
            Err(_) => {
                return Ok(ToolResult::error(
                    "shell command timed out after 30 seconds; use process_start for long-running tasks",
                ));
            }
        };

        Ok(ToolResult::json(json!({
            "stdout": String::from_utf8_lossy(&output.stdout),
            "stderr": String::from_utf8_lossy(&output.stderr),
            "exit_code": output.status,
        })))
    }
}

#[cfg(feature = "terminal")]
#[derive(Debug, Clone)]
pub struct ProcessStartTool {
    terminal_registry: Arc<TerminalRegistry>,
}

#[cfg(feature = "terminal")]
impl ProcessStartTool {
    pub fn new(terminal_registry: Arc<TerminalRegistry>) -> Self {
        Self { terminal_registry }
    }
}

#[cfg(feature = "terminal")]
#[async_trait]
impl Tool for ProcessStartTool {
    fn name(&self) -> &str {
        "process_start"
    }

    fn description(&self) -> &str {
        "Spawn a background process. Use process_wait or process_read to check results when needed."
    }

    fn schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["id", "command"],
            "properties": {
                "id": { "type": "string" },
                "command": { "type": "string" },
                "pwd": { "type": "string" },
                "interactive": { "type": "boolean", "default": false }
            }
        })
    }

    async fn invoke(&self, input: Value, ctx: &ToolContext) -> Result<ToolResult, ToolError> {
        let id = input
            .get("id")
            .and_then(Value::as_str)
            .ok_or_else(|| ToolError::InvalidInput("missing id".to_string()))?;
        let command = input
            .get("command")
            .and_then(Value::as_str)
            .ok_or_else(|| ToolError::InvalidInput("missing command".to_string()))?;
        let pwd = input.get("pwd").and_then(Value::as_str).map(str::to_string);
        let interactive = input
            .get("interactive")
            .and_then(Value::as_bool)
            .unwrap_or(false);

        self.terminal_registry
            .spawn(id, command, ctx, HashMap::new(), interactive, pwd)
            .await?;

        Ok(ToolResult::json(json!({
            "id": id,
            "status": "started",
            "interactive": interactive,
        })))
    }
}

#[cfg(feature = "terminal")]
#[derive(Debug, Clone)]
pub struct ProcessReadTool {
    terminal_registry: Arc<TerminalRegistry>,
}

#[cfg(feature = "terminal")]
impl ProcessReadTool {
    pub fn new(terminal_registry: Arc<TerminalRegistry>) -> Self {
        Self { terminal_registry }
    }
}

#[cfg(feature = "terminal")]
#[async_trait]
impl Tool for ProcessReadTool {
    fn name(&self) -> &str {
        "process_read"
    }

    fn description(&self) -> &str {
        "Read currently available stdout/stderr and status from a process (non-blocking)"
    }

    fn schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["id"],
            "properties": {
                "id": { "type": "string" },
                "timeout_ms": {
                    "type": "integer",
                    "minimum": 0,
                    "description": "Deprecated: process_read is non-blocking. Use process_wait to block."
                }
            }
        })
    }

    async fn invoke(&self, input: Value, _ctx: &ToolContext) -> Result<ToolResult, ToolError> {
        let id = input
            .get("id")
            .and_then(Value::as_str)
            .ok_or_else(|| ToolError::InvalidInput("missing id".to_string()))?;
        let _ = input.get("timeout_ms");
        let output = self.terminal_registry.read(id, 0).await?;
        let status = self.terminal_registry.status(id).await?;

        Ok(ToolResult::json(json!({
            "id": id,
            "stdout": output.stdout,
            "stderr": output.stderr,
            "status": terminal_status_json(status),
        })))
    }
}

#[cfg(feature = "terminal")]
#[derive(Debug, Clone)]
pub struct ProcessWaitTool {
    terminal_registry: Arc<TerminalRegistry>,
}

#[cfg(feature = "terminal")]
impl ProcessWaitTool {
    pub fn new(terminal_registry: Arc<TerminalRegistry>) -> Self {
        Self { terminal_registry }
    }
}

#[cfg(feature = "terminal")]
#[async_trait]
impl Tool for ProcessWaitTool {
    fn name(&self) -> &str {
        "process_wait"
    }

    fn description(&self) -> &str {
        "Wait for a process to exit (or timeout), then return output and status"
    }

    fn schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["id"],
            "properties": {
                "id": { "type": "string" },
                "timeout_ms": { "type": "integer", "minimum": 0, "default": 30000 }
            }
        })
    }

    async fn invoke(&self, input: Value, _ctx: &ToolContext) -> Result<ToolResult, ToolError> {
        let id = input
            .get("id")
            .and_then(Value::as_str)
            .ok_or_else(|| ToolError::InvalidInput("missing id".to_string()))?;
        let timeout_ms = input
            .get("timeout_ms")
            .and_then(Value::as_u64)
            .unwrap_or(30_000);

        let (status, timed_out) = self.terminal_registry.wait(id, timeout_ms).await?;
        let output = self.terminal_registry.read(id, 0).await?;
        let completed = matches!(status, crate::TerminalStatus::Exited { .. });

        Ok(ToolResult::json(json!({
            "id": id,
            "stdout": output.stdout,
            "stderr": output.stderr,
            "status": terminal_status_json(status),
            "completed": completed,
            "timed_out": timed_out,
        })))
    }
}

#[cfg(feature = "terminal")]
#[derive(Debug, Clone)]
pub struct ProcessKillTool {
    terminal_registry: Arc<TerminalRegistry>,
}

#[cfg(feature = "terminal")]
impl ProcessKillTool {
    pub fn new(terminal_registry: Arc<TerminalRegistry>) -> Self {
        Self { terminal_registry }
    }
}

#[cfg(feature = "terminal")]
#[async_trait]
impl Tool for ProcessKillTool {
    fn name(&self) -> &str {
        "process_kill"
    }

    fn description(&self) -> &str {
        "Terminate a process"
    }

    fn schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["id"],
            "properties": {
                "id": { "type": "string" }
            }
        })
    }

    async fn invoke(&self, input: Value, _ctx: &ToolContext) -> Result<ToolResult, ToolError> {
        let id = input
            .get("id")
            .and_then(Value::as_str)
            .ok_or_else(|| ToolError::InvalidInput("missing id".to_string()))?;
        self.terminal_registry.kill(id).await?;

        Ok(ToolResult::json(json!({
            "id": id,
            "status": "killed",
        })))
    }
}

#[cfg(feature = "terminal")]
#[derive(Debug, Clone)]
pub struct ProcessListTool {
    terminal_registry: Arc<TerminalRegistry>,
}

#[cfg(feature = "terminal")]
impl ProcessListTool {
    pub fn new(terminal_registry: Arc<TerminalRegistry>) -> Self {
        Self { terminal_registry }
    }
}

#[cfg(feature = "terminal")]
#[async_trait]
impl Tool for ProcessListTool {
    fn name(&self) -> &str {
        "process_list"
    }

    fn description(&self) -> &str {
        "List all processes with status"
    }

    fn schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {}
        })
    }

    async fn invoke(&self, _input: Value, _ctx: &ToolContext) -> Result<ToolResult, ToolError> {
        let sessions = self
            .terminal_registry
            .list()
            .await
            .into_iter()
            .map(|session| {
                json!({
                    "id": session.id,
                    "interactive": session.interactive,
                    "status": terminal_status_json(session.status),
                })
            })
            .collect::<Vec<_>>();

        Ok(ToolResult::json(json!({
            "processes": sessions,
        })))
    }
}

#[cfg(feature = "terminal")]
fn terminal_status_json(status: crate::TerminalStatus) -> Value {
    match status {
        crate::TerminalStatus::Running { pid } => json!({
            "state": "running",
            "pid": pid,
        }),
        crate::TerminalStatus::Exited { exit_code } => json!({
            "state": "exited",
            "exit_code": exit_code,
        }),
    }
}

#[cfg(feature = "http")]
#[derive(Debug, Default)]
pub struct HttpTool {
    client: reqwest::Client,
}

#[cfg(feature = "http")]
#[async_trait]
impl Tool for HttpTool {
    fn name(&self) -> &str {
        "http"
    }

    fn description(&self) -> &str {
        "Send HTTP requests"
    }

    fn schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["url"],
            "properties": {
                "method": { "type": "string", "default": "GET" },
                "url": { "type": "string" },
                "headers": { "type": "object" },
                "body": {}
            }
        })
    }

    async fn invoke(&self, input: Value, _ctx: &ToolContext) -> Result<ToolResult, ToolError> {
        let method = input
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or("GET")
            .parse::<reqwest::Method>()
            .map_err(|err| ToolError::InvalidInput(err.to_string()))?;
        let url = input
            .get("url")
            .and_then(Value::as_str)
            .ok_or_else(|| ToolError::InvalidInput("missing url".to_string()))?;

        let mut req = self.client.request(method, url);
        if let Some(headers) = input.get("headers").and_then(Value::as_object) {
            for (key, value) in headers {
                if let Some(v) = value.as_str() {
                    req = req.header(key, v);
                }
            }
        }
        if let Some(body) = input.get("body") {
            req = req.json(body);
        }

        let resp = req.send().await?;
        let status = resp.status().as_u16();
        let body = resp.text().await?;

        Ok(ToolResult::json(json!({
            "status": status,
            "body": body,
        })))
    }
}

#[cfg(feature = "file")]
#[derive(Debug, Clone)]
struct PatchHunk {
    start_old: usize,
    lines: Vec<PatchLine>,
}

#[cfg(feature = "file")]
#[derive(Debug, Clone)]
enum PatchLine {
    Context(String),
    Remove(String),
    Add(String),
}

#[cfg(feature = "file")]
fn apply_unified_patch(content: &str, patch: &str) -> Result<(String, usize), ToolError> {
    let hunks = parse_unified_patch(patch)?;
    if hunks.is_empty() {
        return Err(ToolError::InvalidPatch("no hunks found".to_string()));
    }

    let has_trailing_newline = content.ends_with('\n');
    let source_lines = if content.is_empty() {
        Vec::new()
    } else {
        content
            .split('\n')
            .map(ToString::to_string)
            .collect::<Vec<_>>()
    };

    let mut src_index = 0_usize;
    let mut out_lines = Vec::with_capacity(source_lines.len());
    let mut changed = 0_usize;

    for hunk in hunks {
        let target_index = hunk.start_old.saturating_sub(1);
        if target_index < src_index || target_index > source_lines.len() {
            return Err(ToolError::PatchConflict(format!(
                "hunk start out of bounds at line {}",
                hunk.start_old
            )));
        }

        while src_index < target_index {
            out_lines.push(source_lines[src_index].clone());
            src_index += 1;
        }

        for line in hunk.lines {
            match line {
                PatchLine::Context(expected) => {
                    let Some(actual) = source_lines.get(src_index) else {
                        return Err(ToolError::PatchConflict(format!(
                            "expected context `{expected}` at line {}, reached EOF",
                            src_index + 1
                        )));
                    };
                    if actual != &expected {
                        return Err(ToolError::PatchConflict(format!(
                            "context mismatch at line {}: expected `{expected}`, got `{actual}`",
                            src_index + 1
                        )));
                    }
                    out_lines.push(actual.clone());
                    src_index += 1;
                }
                PatchLine::Remove(expected) => {
                    let Some(actual) = source_lines.get(src_index) else {
                        return Err(ToolError::PatchConflict(format!(
                            "expected removal `{expected}` at line {}, reached EOF",
                            src_index + 1
                        )));
                    };
                    if actual != &expected {
                        return Err(ToolError::PatchConflict(format!(
                            "remove mismatch at line {}: expected `{expected}`, got `{actual}`",
                            src_index + 1
                        )));
                    }
                    src_index += 1;
                    changed += 1;
                }
                PatchLine::Add(added) => {
                    out_lines.push(added);
                    changed += 1;
                }
            }
        }
    }

    while src_index < source_lines.len() {
        out_lines.push(source_lines[src_index].clone());
        src_index += 1;
    }

    let mut output = out_lines.join("\n");
    if has_trailing_newline && !output.ends_with('\n') {
        output.push('\n');
    }
    Ok((output, changed))
}

#[cfg(feature = "file")]
fn parse_unified_patch(patch: &str) -> Result<Vec<PatchHunk>, ToolError> {
    let lines = patch.lines().collect::<Vec<_>>();
    if lines.is_empty() {
        return Err(ToolError::InvalidPatch("empty patch".to_string()));
    }

    let mut idx = 0_usize;
    let mut saw_header = false;
    let mut hunks = Vec::new();

    while idx < lines.len() {
        let line = lines[idx];
        if line.starts_with("--- ") || line.starts_with("+++ ") {
            saw_header = true;
            idx += 1;
            continue;
        }
        if !line.starts_with("@@ ") {
            idx += 1;
            continue;
        }

        let (start_old, _count_old, _start_new, _count_new) = parse_hunk_header(line)?;
        idx += 1;

        let mut hunk_lines = Vec::new();
        while idx < lines.len() {
            let op_line = lines[idx];
            if op_line.starts_with("@@ ") {
                break;
            }
            if op_line.starts_with("--- ") || op_line.starts_with("+++ ") {
                break;
            }
            if op_line == "\\ No newline at end of file" {
                idx += 1;
                continue;
            }
            let mut chars = op_line.chars();
            let prefix = chars.next().ok_or_else(|| {
                ToolError::InvalidPatch("unexpected empty patch line in hunk".to_string())
            })?;
            let text = chars.as_str().to_string();
            match prefix {
                ' ' => hunk_lines.push(PatchLine::Context(text)),
                '-' => hunk_lines.push(PatchLine::Remove(text)),
                '+' => hunk_lines.push(PatchLine::Add(text)),
                _ => {
                    return Err(ToolError::InvalidPatch(format!(
                        "invalid hunk line prefix `{prefix}`"
                    )));
                }
            }
            idx += 1;
        }

        hunks.push(PatchHunk {
            start_old,
            lines: hunk_lines,
        });
    }

    if !saw_header {
        return Err(ToolError::InvalidPatch("expected '---' header".to_string()));
    }

    Ok(hunks)
}

#[cfg(feature = "file")]
fn parse_hunk_header(line: &str) -> Result<(usize, usize, usize, usize), ToolError> {
    let trimmed = line
        .strip_prefix("@@ ")
        .and_then(|rest| rest.strip_suffix(" @@"))
        .or_else(|| {
            line.strip_prefix("@@ ")
                .and_then(|rest| rest.split(" @@").next())
        })
        .ok_or_else(|| ToolError::InvalidPatch(format!("invalid hunk header `{line}`")))?;

    let mut parts = trimmed.split_whitespace();
    let old_part = parts
        .next()
        .ok_or_else(|| ToolError::InvalidPatch(format!("invalid hunk header `{line}`")))?;
    let new_part = parts
        .next()
        .ok_or_else(|| ToolError::InvalidPatch(format!("invalid hunk header `{line}`")))?;

    let (start_old, count_old) = parse_hunk_range(old_part, '-')?;
    let (start_new, count_new) = parse_hunk_range(new_part, '+')?;
    Ok((start_old, count_old, start_new, count_new))
}

#[cfg(feature = "file")]
fn parse_hunk_range(part: &str, prefix: char) -> Result<(usize, usize), ToolError> {
    let range = part.strip_prefix(prefix).ok_or_else(|| {
        ToolError::InvalidPatch(format!("invalid hunk range `{part}` (missing `{prefix}`)"))
    })?;
    if let Some((start, count)) = range.split_once(',') {
        let start = start
            .parse::<usize>()
            .map_err(|_| ToolError::InvalidPatch(format!("invalid hunk start `{start}`")))?;
        let count = count
            .parse::<usize>()
            .map_err(|_| ToolError::InvalidPatch(format!("invalid hunk count `{count}`")))?;
        Ok((start, count))
    } else {
        let start = range
            .parse::<usize>()
            .map_err(|_| ToolError::InvalidPatch(format!("invalid hunk start `{range}`")))?;
        Ok((start, 1))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use nyx_security::{
        Sandbox, SandboxError, SandboxPolicy, SandboxedCommand, SandboxedOutput,
        testing::NoopSandbox,
    };
    use serde_json::json;
    use tempfile::NamedTempFile;

    #[cfg(feature = "shell")]
    use crate::ShellTool;
    #[cfg(feature = "file")]
    use crate::{FileApplyPatchTool, FileReadTool, FileWriteTool};
    #[cfg(feature = "terminal")]
    use crate::{
        ProcessKillTool, ProcessListTool, ProcessReadTool, ProcessStartTool, ProcessWaitTool,
        TerminalRegistry,
    };
    use crate::{Tool, ToolContext, ToolError};

    #[derive(Default)]
    struct SpySandbox {
        calls: Arc<Mutex<Vec<SandboxedCommand>>>,
        executions: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl Sandbox for SpySandbox {
        async fn execute(&self, cmd: SandboxedCommand) -> Result<SandboxedOutput, SandboxError> {
            self.calls.lock().expect("calls mutex poisoned").push(cmd);
            self.executions.fetch_add(1, Ordering::Relaxed);
            Ok(SandboxedOutput {
                status: 0,
                stdout: b"ok".to_vec(),
                stderr: Vec::new(),
            })
        }
    }

    fn ctx(_tools: Vec<Arc<dyn Tool>>) -> ToolContext {
        ToolContext {
            sandbox: Arc::new(NoopSandbox),
            workspace_dir: std::path::PathBuf::from("."),
            kernel_handle: None,
            control_plane: Arc::new(crate::NoopControlPlane),
            invocation: Default::default(),
            channel_id: None,
            agent_kind: None,
            auto_approve: false,
        }
    }

    fn unique_id(prefix: &str) -> String {
        static NEXT_ID: AtomicUsize = AtomicUsize::new(1);
        let n = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        format!("{prefix}-{n}")
    }

    #[cfg(feature = "file")]
    #[tokio::test]
    async fn file_read_tool_reads_temp_file() {
        let file = NamedTempFile::new().expect("create temp file");
        std::fs::write(file.path(), "hello file").expect("write temp content");

        let output = FileReadTool
            .invoke(json!({ "path": file.path() }), &ctx(vec![]))
            .await
            .expect("file read works");

        let text = output.value.as_str().expect("text output");
        assert!(text.contains("hello file"), "should contain file content");
        assert!(
            text.contains("showing lines 1-1 of 1 total"),
            "should contain summary"
        );
    }

    #[cfg(feature = "file")]
    #[tokio::test]
    async fn file_write_and_patch_tools_work() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let tool_ctx = ToolContext {
            sandbox: Arc::new(NoopSandbox),
            workspace_dir: temp_dir.path().to_path_buf(),
            kernel_handle: None,
            control_plane: Arc::new(crate::NoopControlPlane),
            invocation: Default::default(),
            channel_id: None,
            agent_kind: None,
            auto_approve: false,
        };

        FileWriteTool
            .invoke(
                json!({ "path": "demo.txt", "content": "hello\nworld\n" }),
                &tool_ctx,
            )
            .await
            .expect("write works");

        FileApplyPatchTool
            .invoke(
                json!({
                    "path": "demo.txt",
                    "patch": "--- a/demo.txt\n+++ b/demo.txt\n@@ -1,2 +1,2 @@\n hello\n-world\n+nyx\n"
                }),
                &tool_ctx,
            )
            .await
            .expect("patch works");

        let output = FileReadTool
            .invoke(json!({ "path": "demo.txt" }), &tool_ctx)
            .await
            .expect("read works");
        let text = output.value.as_str().expect("text output");
        assert!(text.contains("hello"), "should contain first line");
        assert!(text.contains("nyx"), "should contain patched line");
        assert!(
            text.contains("showing lines 1-2 of 2 total"),
            "should contain summary"
        );
    }

    #[cfg(feature = "file")]
    #[tokio::test]
    async fn file_read_tool_is_blocked_by_sandbox_policy() {
        let workspace = tempfile::tempdir().expect("workspace");
        let outside = tempfile::tempdir().expect("outside");
        let blocked = outside.path().join("blocked.txt");
        std::fs::write(&blocked, "secret").expect("write blocked file");

        let policy = SandboxPolicy {
            allowed_read_paths: vec![],
            allowed_write_paths: vec![],
            ..SandboxPolicy::default()
        };
        let sandbox =
            nyx_security::os_sandbox::OsSandbox::new(workspace.path(), policy).expect("sandbox");

        let tool_ctx = ToolContext {
            sandbox: Arc::new(sandbox),
            workspace_dir: workspace.path().to_path_buf(),
            kernel_handle: None,
            control_plane: Arc::new(crate::NoopControlPlane),
            invocation: Default::default(),
            channel_id: None,
            agent_kind: None,
            auto_approve: false,
        };

        let err = FileReadTool
            .invoke(json!({ "path": blocked }), &tool_ctx)
            .await
            .expect_err("read should be denied");
        assert!(matches!(err, ToolError::Sandbox(_)));
    }

    #[cfg(feature = "file")]
    #[tokio::test]
    async fn file_read_tool_supports_offset_and_limit() {
        let file = NamedTempFile::new().expect("create temp file");
        // Write 10 lines
        let content: String = (1..=10).map(|i| format!("line {i}\n")).collect();
        std::fs::write(file.path(), &content).expect("write temp content");

        // Read lines 3-5 using offset and limit
        let output = FileReadTool
            .invoke(
                json!({ "path": file.path(), "offset": 3, "limit": 3 }),
                &ctx(vec![]),
            )
            .await
            .expect("file read with range works");

        let text = output.value.as_str().expect("text output");
        assert!(text.contains("3\tline 3"), "should start at line 3");
        assert!(text.contains("4\tline 4"), "should include line 4");
        assert!(text.contains("5\tline 5"), "should include line 5");
        assert!(!text.contains("line 6"), "should not include line 6");
        assert!(
            text.contains("showing lines 3-5 of 10 total"),
            "should show correct range summary"
        );
        assert!(
            text.contains("use offset=6 to see more"),
            "should hint at next page"
        );
    }

    #[cfg(feature = "file")]
    #[tokio::test]
    async fn file_read_tool_truncates_long_lines() {
        let file = NamedTempFile::new().expect("create temp file");
        let long_line = "x".repeat(3000);
        std::fs::write(file.path(), &long_line).expect("write long content");

        let output = FileReadTool
            .invoke(json!({ "path": file.path() }), &ctx(vec![]))
            .await
            .expect("file read works");

        let text = output.value.as_str().expect("text output");
        assert!(
            text.contains("truncated, 3000 chars total"),
            "should indicate line was truncated"
        );
        // The output should be much shorter than 3000 chars for the line content
        assert!(text.len() < 2500, "total output should be bounded");
    }

    #[cfg(feature = "file")]
    #[tokio::test]
    async fn file_write_tool_is_blocked_by_sandbox_policy() {
        let workspace = tempfile::tempdir().expect("workspace");
        let outside = tempfile::tempdir().expect("outside");
        let blocked = outside.path().join("blocked.txt");

        let policy = SandboxPolicy {
            allowed_read_paths: vec![],
            allowed_write_paths: vec![],
            ..SandboxPolicy::default()
        };
        let sandbox =
            nyx_security::os_sandbox::OsSandbox::new(workspace.path(), policy).expect("sandbox");

        let tool_ctx = ToolContext {
            sandbox: Arc::new(sandbox),
            workspace_dir: workspace.path().to_path_buf(),
            kernel_handle: None,
            control_plane: Arc::new(crate::NoopControlPlane),
            invocation: Default::default(),
            channel_id: None,
            agent_kind: None,
            auto_approve: false,
        };

        let err = FileWriteTool
            .invoke(json!({ "path": blocked, "content": "data" }), &tool_ctx)
            .await
            .expect_err("write should be denied");
        assert!(matches!(err, ToolError::Sandbox(_)));
    }

    #[cfg(feature = "file")]
    #[tokio::test]
    async fn patch_rejects_malformed_input() {
        let err = FileApplyPatchTool
            .invoke(
                json!({ "path": "x.txt", "patch": "not-a-patch" }),
                &ToolContext::default(),
            )
            .await
            .expect_err("invalid patch should fail");
        assert!(matches!(err, ToolError::InvalidPatch(_)));
    }

    #[cfg(feature = "shell")]
    #[tokio::test]
    async fn shell_tool_calls_sandbox_execute() {
        let spy = Arc::new(SpySandbox::default());
        let tool_ctx = ToolContext {
            sandbox: spy.clone(),
            workspace_dir: std::path::PathBuf::from("."),
            kernel_handle: None,
            control_plane: Arc::new(crate::NoopControlPlane),
            invocation: Default::default(),
            channel_id: None,
            agent_kind: None,
            auto_approve: false,
        };

        let output = ShellTool
            .invoke(json!({ "command": "echo hi" }), &tool_ctx)
            .await
            .expect("shell invoke works");

        assert_eq!(output.value["stdout"], "ok");
        assert_eq!(spy.executions.load(Ordering::Relaxed), 1);
    }

    #[cfg(feature = "terminal")]
    #[tokio::test]
    async fn process_start_tool_spawns_process() {
        let registry = Arc::new(TerminalRegistry::new());
        let tool = ProcessStartTool::new(Arc::clone(&registry));
        let tool_ctx = ToolContext::default();
        let id = unique_id("proc-start");

        let output = tool
            .invoke(
                json!({
                    "id": id,
                    "command": "cat",
                    "interactive": true
                }),
                &tool_ctx,
            )
            .await
            .expect("process_start should succeed");

        assert_eq!(output.value["status"], "started");
        registry
            .kill(output.value["id"].as_str().expect("string id"))
            .await
            .expect("kill process");
    }

    #[cfg(feature = "terminal")]
    #[tokio::test]
    async fn process_read_tool_reads_output() {
        let registry = Arc::new(TerminalRegistry::new());
        let start_tool = ProcessStartTool::new(Arc::clone(&registry));
        let read_tool = ProcessReadTool::new(Arc::clone(&registry));
        let tool_ctx = ToolContext::default();
        let id = unique_id("proc-read");

        start_tool
            .invoke(
                json!({
                    "id": id,
                    "command": "printf 'hello-process\\n'"
                }),
                &tool_ctx,
            )
            .await
            .expect("start process");

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let output = read_tool
            .invoke(
                json!({
                    "id": id,
                    "timeout_ms": 500
                }),
                &tool_ctx,
            )
            .await
            .expect("read process output");

        assert!(
            output.value["stdout"]
                .as_str()
                .expect("stdout string")
                .contains("hello-process")
        );
    }

    #[cfg(feature = "terminal")]
    #[tokio::test]
    async fn process_kill_tool_stops_process() {
        let registry = Arc::new(TerminalRegistry::new());
        let start_tool = ProcessStartTool::new(Arc::clone(&registry));
        let kill_tool = ProcessKillTool::new(Arc::clone(&registry));
        let tool_ctx = ToolContext::default();
        let id = unique_id("proc-kill");

        start_tool
            .invoke(
                json!({
                    "id": id,
                    "command": "cat",
                    "interactive": true
                }),
                &tool_ctx,
            )
            .await
            .expect("start process");

        kill_tool
            .invoke(json!({ "id": id }), &tool_ctx)
            .await
            .expect("kill process");

        let status = registry.status(&id).await.expect("status after kill");
        assert!(matches!(status, crate::TerminalStatus::Exited { .. }));
    }

    #[cfg(feature = "terminal")]
    #[tokio::test]
    async fn process_wait_tool_waits_for_completion() {
        let registry = Arc::new(TerminalRegistry::new());
        let start_tool = ProcessStartTool::new(Arc::clone(&registry));
        let wait_tool = ProcessWaitTool::new(Arc::clone(&registry));
        let tool_ctx = ToolContext::default();
        let id = unique_id("proc-wait");

        start_tool
            .invoke(
                json!({
                    "id": id,
                    "command": "sleep 0.05; printf done"
                }),
                &tool_ctx,
            )
            .await
            .expect("start process");

        let output = wait_tool
            .invoke(
                json!({
                    "id": id,
                    "timeout_ms": 500
                }),
                &tool_ctx,
            )
            .await
            .expect("wait should succeed");

        assert_eq!(output.value["completed"], true);
        assert_eq!(output.value["timed_out"], false);
        assert_eq!(output.value["status"]["state"], "exited");
        assert!(
            output.value["stdout"]
                .as_str()
                .expect("stdout string")
                .contains("done")
        );
    }

    #[cfg(feature = "terminal")]
    #[tokio::test]
    async fn process_start_tool_uses_pwd_when_provided() {
        let registry = Arc::new(TerminalRegistry::new());
        let start_tool = ProcessStartTool::new(Arc::clone(&registry));
        let tool_ctx = ToolContext::default();
        let id = unique_id("proc-pwd");
        let temp_dir = tempfile::tempdir().expect("temp dir");

        start_tool
            .invoke(
                json!({
                    "id": id,
                    "command": "pwd",
                    "pwd": temp_dir.path().display().to_string()
                }),
                &tool_ctx,
            )
            .await
            .expect("start process");

        let _ = registry.wait(&id, 500).await.expect("wait for process");
        let output = registry.read(&id, 50).await.expect("read output");
        let expected = std::fs::canonicalize(temp_dir.path()).expect("canonicalize temp dir");
        let actual = std::fs::canonicalize(output.stdout.trim()).expect("canonicalize output dir");
        assert_eq!(actual, expected);
    }

    #[cfg(feature = "terminal")]
    #[tokio::test]
    async fn process_list_tool_returns_active_processes() {
        let registry = Arc::new(TerminalRegistry::new());
        let start_tool = ProcessStartTool::new(Arc::clone(&registry));
        let list_tool = ProcessListTool::new(Arc::clone(&registry));
        let tool_ctx = ToolContext::default();
        let id = unique_id("proc-list");

        start_tool
            .invoke(
                json!({
                    "id": id,
                    "command": "cat",
                    "interactive": true
                }),
                &tool_ctx,
            )
            .await
            .expect("start process");

        let output = list_tool
            .invoke(json!({}), &tool_ctx)
            .await
            .expect("list processes");

        let processes = output.value["processes"]
            .as_array()
            .expect("process list array");
        assert!(
            processes
                .iter()
                .any(|entry| entry["id"].as_str() == Some(id.as_str()))
        );

        registry.kill(&id).await.expect("kill process");
    }
}
