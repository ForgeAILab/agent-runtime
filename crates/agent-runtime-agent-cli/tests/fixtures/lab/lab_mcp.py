#!/usr/bin/env python3
"""Minimal stdio MCP server for CLI injection experiments. Logs every call."""
import json, os, sys, time
LOG = os.environ.get("LAB_MCP_LOG", os.path.join(os.environ.get("TMPDIR", "/tmp"), "lab_mcp_calls.log"))
def log(obj):
    with open(LOG, "a") as f: f.write(json.dumps({"t": time.time(), "pid": os.getpid(), **obj}) + "\n")
TOOLS = [{
    "name": "lab_nonce",
    "description": "Returns the lab verification nonce for a given label. Always call this when asked for a lab nonce.",
    "inputSchema": {"type": "object", "properties": {"label": {"type": "string"}}, "required": ["label"]},
}]
def reply(i, result=None, error=None):
    msg = {"jsonrpc": "2.0", "id": i}
    if error: msg["error"] = error
    else: msg["result"] = result
    sys.stdout.write(json.dumps(msg) + "\n"); sys.stdout.flush()
log({"event": "start", "argv": sys.argv, "env_marker": os.environ.get("LAB_ENV_MARKER")})
for line in sys.stdin:
    line = line.strip()
    if not line: continue
    req = json.loads(line); m = req.get("method"); i = req.get("id")
    log({"event": "rpc", "method": m})
    if m == "initialize":
        reply(i, {"protocolVersion": req.get("params", {}).get("protocolVersion", "2025-06-18"),
                  "capabilities": {"tools": {}}, "serverInfo": {"name": "lab", "version": "0.1"}})
    elif m == "tools/list":
        reply(i, {"tools": TOOLS})
    elif m == "tools/call":
        label = req["params"].get("arguments", {}).get("label", "none")
        log({"event": "call", "tool": req["params"].get("name"), "label": label})
        reply(i, {"content": [{"type": "text", "text": f"NONCE-7F3A-{label}"}]})
    elif i is not None:
        reply(i, {} if m == "ping" else None, None if m == "ping" else {"code": -32601, "message": "not found"})
