//! Minimal MCP stdio server used by nyx-tools integration tests.
//!
//! Speaks newline-delimited JSON-RPC 2.0 on stdin/stdout: `initialize`,
//! `tools/list`, `tools/call`, and `ping`. Deliberately hand-rolled so the
//! `mcp` feature's regular dependency set stays client-only.

use std::io::{BufRead, Write};

use serde_json::{Value, json};

fn main() {
    let stdin = std::io::stdin();
    let stdout = std::io::stdout();
    let mut out = stdout.lock();

    for line in stdin.lock().lines() {
        let Ok(line) = line else { break };
        if line.trim().is_empty() {
            continue;
        }
        let Ok(message) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        let Some(id) = message.get("id").cloned() else {
            continue; // notification
        };
        let method = message
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let params = message.get("params").cloned().unwrap_or(Value::Null);

        let reply = match method {
            "initialize" => {
                let protocol_version = params
                    .get("protocolVersion")
                    .cloned()
                    .unwrap_or_else(|| json!("2025-06-18"));
                json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "result": {
                        "protocolVersion": protocol_version,
                        "capabilities": { "tools": {} },
                        "serverInfo": { "name": "mcp-fixture-server", "version": "0.1.0" }
                    }
                })
            }
            "tools/list" => json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": { "tools": tool_list() }
            }),
            "tools/call" => {
                let name = params
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let arguments = params.get("arguments").cloned().unwrap_or(Value::Null);
                match call_tool(name, &arguments) {
                    Ok(result) => json!({ "jsonrpc": "2.0", "id": id, "result": result }),
                    Err(message) => json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "error": { "code": -32602, "message": message }
                    }),
                }
            }
            _ => json!({ "jsonrpc": "2.0", "id": id, "result": {} }),
        };

        let Ok(encoded) = serde_json::to_string(&reply) else {
            continue;
        };
        if writeln!(out, "{encoded}")
            .and_then(|_| out.flush())
            .is_err()
        {
            break;
        }
    }
}

fn tool_list() -> Value {
    let object_schema = json!({ "type": "object", "properties": {} });
    json!([
        {
            "name": "echo",
            "description": "Echo the message argument back as text",
            "inputSchema": {
                "type": "object",
                "properties": { "message": { "type": "string" } },
                "required": ["message"]
            }
        },
        { "name": "fail", "description": "Always reports a tool error", "inputSchema": object_schema },
        { "name": "search", "description": "Pretend search", "inputSchema": object_schema },
        { "name": "admin", "description": "Pretend admin action", "inputSchema": object_schema },
        { "name": "env_probe", "description": "Return the FIXTURE_TAG environment variable", "inputSchema": object_schema },
        { "name": "structured", "description": "Return structured content only", "inputSchema": object_schema }
    ])
}

fn call_tool(name: &str, arguments: &Value) -> Result<Value, String> {
    match name {
        "echo" => {
            let message = arguments
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or_default();
            Ok(json!({
                "content": [{ "type": "text", "text": message }],
                "isError": false
            }))
        }
        "fail" => Ok(json!({
            "content": [{ "type": "text", "text": "boom" }],
            "isError": true
        })),
        "search" => Ok(json!({
            "content": [{ "type": "text", "text": "search ok" }],
            "isError": false
        })),
        "admin" => Ok(json!({
            "content": [{ "type": "text", "text": "admin ok" }],
            "isError": false
        })),
        "env_probe" => Ok(json!({
            "content": [{
                "type": "text",
                "text": std::env::var("FIXTURE_TAG").unwrap_or_default()
            }],
            "isError": false
        })),
        "structured" => Ok(json!({
            "content": [],
            "structuredContent": { "answer": 42 },
            "isError": false
        })),
        other => Err(format!("unknown tool: {other}")),
    }
}
