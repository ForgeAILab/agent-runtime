//! The per-turn runtime-tool MCP bridge.
//!
//! This is intentionally a small HTTP/1.1 implementation. External CLIs only
//! need the streamable-HTTP MCP subset used here, and keeping the listener in
//! the runtime avoids adding a framework (and its dependency graph) to every
//! host that embeds the direct loop. The bridge is loopback-only, authenticated
//! with a turn token, and owns no conversation history: the external CLI owns
//! that conversation while the runtime owns the tool side effects and events.

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, TryAcquireError};
use tokio::task::JoinSet;
use uuid::Uuid;

use agent_runtime_core::cancel::{CancelReason, Cancellation};
use agent_runtime_core::clock::Deadline;
use agent_runtime_core::content::{ContentPart, ToolCall, ToolResultBlock};
use agent_runtime_core::error::RuntimeError;
use agent_runtime_core::ids::TurnId;

use crate::agent::external::ExternalToolBridge;
use crate::ids::IdMinter;
use crate::runtime::emitter::EventEmitter;
use crate::runtime::state::{SessionExecutionContext, SessionState};

use super::{Driver, TurnMachine, TurnMachineContext};

const MAX_HEADER_BYTES: usize = 16 * 1024;
const MAX_BODY_BYTES: usize = 1024 * 1024;
const MAX_CONCURRENT_CALLS: usize = 8;
/// A connection that sends no complete request for this long is dropped, so
/// an idle or trickling local client cannot pin bridge connections.
const REQUEST_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
const MCP_PROTOCOL_VERSION: &str = "2025-06-18";

/// One MCP tool descriptor projected from the sealed runtime registry.
#[derive(Debug, Clone, Serialize)]
struct McpTool {
    name: String,
    description: String,
    #[serde(rename = "inputSchema")]
    input_schema: Value,
}

/// Shared state for accepted HTTP connections.
struct BridgeState {
    token: String,
    session_id: String,
    tools: Vec<McpTool>,
    active: Arc<AtomicBool>,
    calls: Arc<Semaphore>,
    dispatch: Arc<BridgeDispatch>,
}

/// The runtime context used by every concurrent `tools/call` request.
struct BridgeDispatch {
    driver: Arc<Driver>,
    state: Arc<std::sync::Mutex<SessionState>>,
    execution: Arc<SessionExecutionContext>,
    emitter: Arc<EventEmitter>,
    minter: Arc<IdMinter>,
    cancel: Cancellation,
    turn: TurnId,
    deadline: Deadline,
    active: Arc<AtomicBool>,
}

struct BridgeToolResult {
    text: String,
    is_error: bool,
}

impl BridgeDispatch {
    async fn call(&self, name: String, arguments: Value) -> BridgeToolResult {
        if !self.active.load(Ordering::Acquire) {
            return BridgeToolResult::error("runtime tool bridge is no longer active");
        }

        let call = ToolCall {
            id: self.minter.tool_call(),
            name,
            arguments,
        };
        let mut machine = TurnMachine::new(
            self.driver.as_ref(),
            TurnMachineContext {
                state: self.state.clone(),
                execution: self.execution.clone(),
                emitter: self.emitter.clone(),
                minter: self.minter.clone(),
                cancel: self.cancel.clone(),
                inbox: Arc::new(std::sync::Mutex::new(
                    crate::runtime::inject::InjectionQueue::new(0),
                )),
                steer_mailbox: None,
                turn_id: self.turn.clone(),
                acceptance: None,
            },
        );

        match machine
            .run_external_bridge_action(call, self.deadline)
            .await
        {
            Ok(result) => BridgeToolResult::from_block(&result),
            Err(error) => BridgeToolResult::error(error.message),
        }
    }
}

impl BridgeToolResult {
    fn error(message: impl Into<String>) -> Self {
        Self {
            text: message.into(),
            is_error: true,
        }
    }

    fn from_block(block: &ToolResultBlock) -> Self {
        let text = block
            .content
            .iter()
            .filter_map(ContentPart::as_text)
            .collect::<Vec<_>>()
            .join("\n");
        let text = if text.is_empty() {
            serde_json::to_string(&block.content).unwrap_or_else(|_| "tool completed".into())
        } else {
            text
        };
        Self {
            text,
            is_error: block.is_error,
        }
    }
}

/// A live bridge endpoint and its invalidation handle.
pub(super) struct ExternalToolBridgeServer {
    bridge: ExternalToolBridge,
    active: Arc<AtomicBool>,
    cancel: Cancellation,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl ExternalToolBridgeServer {
    /// Binds a fresh loopback listener before the backend starts its turn.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn start(
        driver: &Driver,
        state: Arc<std::sync::Mutex<SessionState>>,
        execution: Arc<SessionExecutionContext>,
        emitter: Arc<EventEmitter>,
        minter: Arc<IdMinter>,
        turn_cancel: Cancellation,
        turn: TurnId,
        deadline: Deadline,
    ) -> Result<Self, RuntimeError> {
        let listener = TcpListener::bind("127.0.0.1:0").await.map_err(|error| {
            RuntimeError::config(format!("could not bind runtime tool bridge: {error}"))
        })?;
        let address = listener.local_addr().map_err(|error| {
            RuntimeError::internal(format!("could not inspect runtime tool bridge: {error}"))
        })?;
        let token = format!("runtime-{}", Uuid::new_v4().simple());
        let session_id = format!("runtime-{}", Uuid::new_v4().simple());
        let active = Arc::new(AtomicBool::new(true));
        let bridge_cancel = turn_cancel.child();
        let dispatch = Arc::new(BridgeDispatch {
            driver: Arc::new(driver.clone()),
            state,
            execution,
            emitter,
            minter,
            cancel: bridge_cancel.clone(),
            turn,
            deadline,
            active: active.clone(),
        });
        let tools = driver
            .registry
            .specs()
            .into_iter()
            .map(|spec| McpTool {
                name: spec.name,
                description: spec.description,
                input_schema: spec.input_schema,
            })
            .collect::<Vec<_>>();
        let state = Arc::new(BridgeState {
            token: token.clone(),
            session_id: session_id.clone(),
            tools: tools.clone(),
            active: active.clone(),
            calls: Arc::new(Semaphore::new(MAX_CONCURRENT_CALLS)),
            dispatch,
        });
        let task_state = state.clone();
        let task_cancel = bridge_cancel.clone();
        let task = tokio::spawn(async move {
            serve(listener, task_state, task_cancel).await;
        });

        Ok(Self {
            bridge: ExternalToolBridge {
                url: format!("http://{address}/mcp"),
                bearer_token: token,
                tools: tools.into_iter().map(|tool| tool.name).collect(),
            },
            active,
            cancel: bridge_cancel.clone(),
            task: Some(task),
        })
    }

    /// The neutral description handed to the external backend.
    pub(super) fn description(&self) -> ExternalToolBridge {
        self.bridge.clone()
    }

    /// Invalidates the credential and stops accepting new calls.
    pub(super) async fn stop(&mut self) {
        self.active.store(false, Ordering::Release);
        self.cancel.cancel(CancelReason::Shutdown);
        if let Some(task) = self.task.take() {
            task.abort();
            let _ = task.await;
        }
    }
}

impl Drop for ExternalToolBridgeServer {
    fn drop(&mut self) {
        self.active.store(false, Ordering::Release);
        self.cancel.cancel(CancelReason::Shutdown);
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

async fn serve(listener: TcpListener, state: Arc<BridgeState>, cancel: Cancellation) {
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
            accepted = listener.accept() => {
                let Ok((stream, _)) = accepted else { break; };
                let state = state.clone();
                connections.spawn(async move { handle_connection(stream, state).await });
            }
        }
    }
    connections.abort_all();
    while connections.join_next().await.is_some() {}
}

async fn handle_connection(mut stream: TcpStream, state: Arc<BridgeState>) {
    loop {
        let read = tokio::time::timeout(REQUEST_READ_TIMEOUT, read_request(&mut stream)).await;
        let Ok(read) = read else {
            return;
        };
        let request = match read {
            Ok(Some(request)) => request,
            Ok(None) | Err(ReadError::Closed) => return,
            Err(ReadError::TooLarge) => {
                let _ = write_response(&mut stream, 413, b"request too large", None).await;
                return;
            }
            Err(ReadError::Malformed) => {
                let _ = write_response(&mut stream, 400, b"malformed HTTP request", None).await;
                return;
            }
            Err(ReadError::Io) => return,
        };

        if !authorized(&request, &state) {
            let _ = write_response_with_header(
                &mut stream,
                401,
                b"unauthorized",
                Some(("WWW-Authenticate", "Bearer")),
                None,
            )
            .await;
            return;
        }
        if request.method != "POST" {
            let _ = write_response(
                &mut stream,
                405,
                b"method not allowed",
                Some(&state.session_id),
            )
            .await;
            return;
        }

        let response = match serde_json::from_slice::<JsonRpcRequest>(&request.body) {
            Ok(request) => handle_rpc(request, state.clone()).await,
            Err(error) => Some(json_rpc_error(Value::Null, -32700, error.to_string())),
        };
        match response {
            Some(response) => {
                let body = match serde_json::to_vec(&response) {
                    Ok(body) => body,
                    Err(_) => return,
                };
                if write_json_response(&mut stream, &body, &state.session_id)
                    .await
                    .is_err()
                {
                    return;
                }
            }
            None => {
                if write_response(&mut stream, 202, &[], Some(&state.session_id))
                    .await
                    .is_err()
                {
                    return;
                }
            }
        }
    }
}

#[derive(Debug, Deserialize)]
struct JsonRpcRequest {
    #[serde(default)]
    jsonrpc: Option<String>,
    #[serde(default)]
    id: Option<Value>,
    method: String,
    #[serde(default)]
    params: Option<Value>,
}

async fn handle_rpc(request: JsonRpcRequest, state: Arc<BridgeState>) -> Option<Value> {
    let id = request.id.clone().unwrap_or(Value::Null);
    if request.jsonrpc.as_deref() != Some("2.0") {
        return response_or_none(
            request.id,
            json_rpc_error(id, -32600, "jsonrpc must be 2.0"),
        );
    }
    if request.method == "notifications/initialized" {
        return None;
    }
    let result = match request.method.as_str() {
        "initialize" => Ok(json!({
            "protocolVersion": MCP_PROTOCOL_VERSION,
            "capabilities": {"tools": {}},
            "serverInfo": {"name": "agent-runtime", "version": "0.1.0"}
        })),
        "ping" => Ok(json!({})),
        "tools/list" => Ok(json!({"tools": state.tools})),
        "tools/call" => match parse_tool_call(request.params) {
            Ok((name, arguments)) => {
                let permit = match state.calls.clone().try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(TryAcquireError::NoPermits) => {
                        return response_or_none(
                            request.id,
                            json_rpc_result(
                                id,
                                json!({
                                    "content": [{"type": "text", "text": "runtime tool bridge is busy"}],
                                    "isError": true
                                }),
                            ),
                        );
                    }
                    Err(TryAcquireError::Closed) => {
                        return response_or_none(
                            request.id,
                            json_rpc_result(
                                id,
                                json!({
                                    "content": [{"type": "text", "text": "runtime tool bridge is closed"}],
                                    "isError": true
                                }),
                            ),
                        );
                    }
                };
                let outcome = state.dispatch.call(name, arguments).await;
                drop(permit);
                Ok(json!({
                    "content": [{"type": "text", "text": outcome.text}],
                    "isError": outcome.is_error
                }))
            }
            Err(message) => Ok(json!({
                "content": [{"type": "text", "text": message}],
                "isError": true
            })),
        },
        _ if request.method.starts_with("notifications/") => return None,
        _ => Err(json_rpc_error(id.clone(), -32601, "method not found")),
    };
    let response = match result {
        Ok(result) => json_rpc_result(id, result),
        Err(error) => error,
    };
    response_or_none(request.id, response)
}

fn parse_tool_call(params: Option<Value>) -> Result<(String, Value), String> {
    let params = params
        .and_then(|params| params.as_object().cloned())
        .ok_or_else(|| "tools/call params must be an object".to_owned())?;
    let name = params
        .get("name")
        .and_then(Value::as_str)
        .filter(|name| !name.is_empty() && name.len() <= 256)
        .ok_or_else(|| "tools/call requires a bounded name".to_owned())?
        .to_owned();
    let arguments = params
        .get("arguments")
        .cloned()
        .unwrap_or_else(|| json!({}));
    Ok((name, arguments))
}

fn response_or_none(id: Option<Value>, response: Value) -> Option<Value> {
    id.is_some().then_some(response)
}

fn json_rpc_result(id: Value, result: Value) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "result": result})
}

fn json_rpc_error(id: Value, code: i64, message: impl Into<String>) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": message.into()}})
}

fn authorized(request: &HttpRequest, state: &BridgeState) -> bool {
    state.active.load(Ordering::Acquire)
        && request
            .headers
            .get("authorization")
            .and_then(|value| value.strip_prefix("Bearer "))
            .is_some_and(|presented| constant_time_eq(presented.as_bytes(), state.token.as_bytes()))
}

/// Compares without an early exit, so response timing does not reveal how
/// much of a guessed token matched.
fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0_u8, |difference, (a, b)| difference | (a ^ b))
        == 0
}

struct HttpRequest {
    method: String,
    headers: std::collections::BTreeMap<String, String>,
    body: Vec<u8>,
}

enum ReadError {
    Closed,
    TooLarge,
    Malformed,
    Io,
}

async fn read_request(stream: &mut TcpStream) -> Result<Option<HttpRequest>, ReadError> {
    let mut header = Vec::new();
    loop {
        let mut byte = [0u8; 1];
        let count = stream.read(&mut byte).await.map_err(|_| ReadError::Io)?;
        if count == 0 {
            return if header.is_empty() {
                Ok(None)
            } else {
                Err(ReadError::Closed)
            };
        }
        header.push(byte[0]);
        if header.len() > MAX_HEADER_BYTES {
            return Err(ReadError::TooLarge);
        }
        if header.ends_with(b"\r\n\r\n") {
            break;
        }
    }
    let header = std::str::from_utf8(&header).map_err(|_| ReadError::Malformed)?;
    let mut lines = header.split("\r\n");
    let request_line = lines.next().ok_or(ReadError::Malformed)?;
    let mut request_parts = request_line.split_whitespace();
    let method = request_parts.next().ok_or(ReadError::Malformed)?.to_owned();
    let _path = request_parts.next().ok_or(ReadError::Malformed)?;
    let _version = request_parts.next().ok_or(ReadError::Malformed)?;
    let mut headers = std::collections::BTreeMap::new();
    for line in lines.filter(|line| !line.is_empty()) {
        let (name, value) = line.split_once(':').ok_or(ReadError::Malformed)?;
        headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_owned());
    }
    let length = headers
        .get("content-length")
        .map(|value| value.parse::<usize>().map_err(|_| ReadError::Malformed))
        .transpose()?
        .unwrap_or(0);
    if length > MAX_BODY_BYTES {
        return Err(ReadError::TooLarge);
    }
    let mut body = vec![0u8; length];
    stream
        .read_exact(&mut body)
        .await
        .map_err(|_| ReadError::Io)?;
    Ok(Some(HttpRequest {
        method,
        headers,
        body,
    }))
}

async fn write_json_response(
    stream: &mut TcpStream,
    body: &[u8],
    session_id: &str,
) -> io::Result<()> {
    write_response_with_header(
        stream,
        200,
        body,
        Some(("Content-Type", "application/json")),
        Some(session_id),
    )
    .await
}

async fn write_response(
    stream: &mut TcpStream,
    status: u16,
    body: &[u8],
    session_id: Option<&str>,
) -> io::Result<()> {
    write_response_with_header(stream, status, body, None, session_id).await
}

async fn write_response_with_header(
    stream: &mut TcpStream,
    status: u16,
    body: &[u8],
    extra: Option<(&str, &str)>,
    session_id: Option<&str>,
) -> io::Result<()> {
    let reason = match status {
        200 => "OK",
        202 => "Accepted",
        400 => "Bad Request",
        401 => "Unauthorized",
        405 => "Method Not Allowed",
        413 => "Payload Too Large",
        _ => "Error",
    };
    let mut response = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Length: {}\r\nConnection: keep-alive\r\n",
        body.len()
    );
    if let Some((name, value)) = extra {
        response.push_str(name);
        response.push_str(": ");
        response.push_str(value);
        response.push_str("\r\n");
    }
    if let Some(session_id) = session_id {
        response.push_str("Mcp-Session-Id: ");
        response.push_str(session_id);
        response.push_str("\r\n");
    }
    response.push_str("\r\n");
    stream.write_all(response.as_bytes()).await?;
    stream.write_all(body).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn structured_result_is_rendered_as_json_text() {
        let block = ToolResultBlock {
            call_id: agent_runtime_core::ids::ToolCallId::new("call"),
            name: "tool".into(),
            content: vec![ContentPart::text(r#"{"ok":true}"#)],
            is_error: false,
        };
        let result = BridgeToolResult::from_block(&block);
        assert_eq!(result.text, r#"{"ok":true}"#);
        assert!(!result.is_error);
    }
}
