# Command-backed model providers

Agent Runtime can host a trusted model CLI through the same `Provider` trait as
a native HTTP adapter. Enable the mechanism explicitly:

```toml
[dependencies]
agent-runtime = { git = "https://github.com/ForgeAILab/agent-runtime.git", rev = "<reviewed-commit-sha>", features = ["command-provider"] }
```

This is a provider transport, not an autonomous-agent bridge. Runtime remains
the authority for canonical history, context planning, tools and MCP, approval,
retry accounting, cancellation, usage, and events. A CLI that keeps a hidden
conversation, performs its own tool loop, or retries model work internally
needs a separate executor integration.

## Consumer-owned adapter

The host implements `CommandAdapter` for one named CLI protocol/version. It:

1. Advertises exact `ModelDescriptor` and `Capabilities` values.
2. Validates every accepted `ProviderRequest` before process I/O.
3. Produces adapter-specific argv and bounded stdin for one provider attempt.
4. Creates an attempt-local `CommandOutputDecoder` that maps machine stdout
   frames to `ProviderStreamEvent`s.
5. Optionally defines an explicit version probe and parses it into bounded,
   redaction-safe `CommandPreflight` metadata.

See
[`crates/agent-runtime-provider/examples/command_provider.rs`](../crates/agent-runtime-provider/examples/command_provider.rs)
for a compile-checked JSONL example.

The framework rejects standard capability mismatches before calling the
adapter. The adapter must reject stricter protocol mismatches before returning
its `CommandAttempt`. Settings must never be silently dropped. In particular,
a text-only CLI advertises `tools = false`; it cannot accept a runtime request
containing MCP or native tool schemas.

## Process authority

`CommandProcessConfig` accepts an already-authorized absolute executable and
working directory. It canonicalizes both, executes direct argv without a
shell, clears the ambient environment, and passes only values explicitly added
with `with_env`. The host—not this crate—resolves executable names, layered
configuration, credential references, or logged-in CLI homes.

Prompt and secret-bearing request data belongs on stdin, not argv. Process
configuration debug output shows environment names but not values;
`CommandAttempt` debug output shows the stdin byte count but not its contents.
Raw stderr is drained so the child cannot block, then discarded. A trusted
adapter should report errors through its machine stdout protocol and convert
them to bounded `ProviderError` values.

The default limits bound stdin, each newline-delimited stdout frame, aggregate
stdout, aggregate stderr, probe output, probe time, and cleanup time. Builders can
narrow or increase them only within hard framework ceilings.

## Attempts and lifecycle

One `Provider::stream` call starts at most one process and represents exactly
one visible `AttemptId`. The decoder may stream text, reasoning, tool-call
fragments, usage, and other normalized events. It must produce exactly one
`Finish` or `Error` terminal. EOF without a terminal, malformed frames, a
second terminal, post-terminal data, an unsuccessful exit after `Finish`, or
an output-limit violation fails the attempt without committing a successful
terminal.

The provider supervises a process group (a job object on Windows). Attempt
cancellation, deadline expiry, decoder/input failure, early stream drop, probe
timeout, and normal completion all close or terminate the process tree under a
bounded cleanup policy. A runtime retry therefore creates a new process and a
new visible attempt; an adapter must not retry invisibly.

`CommandProvider::preflight` is the only framework operation that runs an
adapter's optional availability/version probe. Construction, config loading,
`Debug`, and static inspection never spawn or access CLI authentication state.

## Portable and adapter-specific settings

Portable settings stay above the provider boundary:

- model identity and context/output limits;
- reasoning request and declared reasoning capabilities;
- structured-output and tool capabilities;
- runtime MCP/tools, approvals, retries, and time limits.

Adapter-specific settings stay in a consumer-owned namespace:

- named adapter kind and supported CLI versions;
- executable selection and trusted fixed arguments;
- CLI home/authentication environment mapping;
- CLI protocol flags and response normalization.

## Smith/TUI integration

Smith implements `command-jsonl`, a consumer-owned adapter for version 1 of the
`smith-command-provider` protocol. It authorizes user-owned executable settings,
runs an explicit compatibility probe, and constructs `CommandProvider` through
its ordinary runtime factory. The same provider serves TUI and headless turns.

The executable must implement Smith's probe and request/frame protocol. Pointing
this configuration at `claude`, `codex`, or another autonomous coding agent does
not make that CLI a compatible model provider. This framework and the reference
`agent-runtime run` command do not launch Claude Code or Codex as external agents.
Such support needs a separate backend contract for history, tool execution,
approvals, cancellation, and agent events.

Smith should not reuse the reference `agent-runtime-cli` TOML parser: its
layered configuration and trust model are already richer. This repository does
not modify `../tui` or ship a vendor CLI adapter as part of the framework.
