#![cfg(all(feature = "command-provider", unix))]

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use agent_runtime_core::cancel::{CancelReason, Cancellation};
use agent_runtime_core::clock::{Deadline, SystemClock};
use agent_runtime_core::ids::{AttemptId, RequestId, SessionId};
use agent_runtime_core::provider::{
    AuthKind, Capabilities, FinishReason, ModelDescriptor, ModelId, Provider,
    ProviderAttemptPurpose, ProviderCallContext, ProviderError, ProviderErrorKind, ProviderRequest,
    ProviderStreamEvent, ReasoningSupport, ToolSchema,
};
use agent_runtime_core::usage::CounterKind;
use agent_runtime_provider::command::{
    CommandAdapter, CommandAttempt, CommandConfigError, CommandLimits, CommandOutputDecoder,
    CommandPreflight, CommandPreflightError, CommandProbe, CommandProcessConfig, CommandProvider,
};
use futures_util::StreamExt;

const MODEL: &str = "fixture-command-model";

#[derive(Debug)]
struct JsonLinesDecoder;

impl CommandOutputDecoder for JsonLinesDecoder {
    fn decode_frame(&mut self, frame: &[u8]) -> Result<Vec<ProviderStreamEvent>, ProviderError> {
        if frame.is_empty() {
            return Ok(Vec::new());
        }
        serde_json::from_slice(frame)
            .map(|event| vec![event])
            .map_err(|_| {
                ProviderError::new(
                    ProviderErrorKind::MalformedStream,
                    "fixture machine frame is malformed",
                )
            })
    }
}

#[derive(Debug)]
struct FixtureAdapter {
    capabilities: Capabilities,
    stdin: Vec<u8>,
    prepares: Arc<AtomicUsize>,
    probe: bool,
    vendor_extensions: bool,
}

impl FixtureAdapter {
    fn new(capabilities: Capabilities) -> Self {
        Self {
            capabilities,
            stdin: Vec::new(),
            prepares: Arc::new(AtomicUsize::new(0)),
            probe: false,
            vendor_extensions: false,
        }
    }

    fn with_stdin(mut self, stdin: impl Into<Vec<u8>>) -> Self {
        self.stdin = stdin.into();
        self
    }

    fn with_probe(mut self) -> Self {
        self.probe = true;
        self
    }
}

impl CommandAdapter for FixtureAdapter {
    fn describe(&self) -> Vec<ModelDescriptor> {
        vec![ModelDescriptor {
            id: ModelId::new(MODEL),
            display_name: "Fixture command model".to_owned(),
            vendor: "fixture".to_owned(),
            capabilities: self.capabilities.clone(),
        }]
    }

    fn prepare(
        &self,
        _request: &ProviderRequest,
        _context: &ProviderCallContext,
    ) -> Result<CommandAttempt, ProviderError> {
        self.prepares.fetch_add(1, Ordering::SeqCst);
        Ok(CommandAttempt::new(
            Vec::new(),
            self.stdin.clone(),
            Box::new(JsonLinesDecoder),
        ))
    }

    fn accepts_vendor_extensions(&self) -> bool {
        self.vendor_extensions
    }

    fn probe(&self) -> Option<CommandProbe> {
        self.probe
            .then(|| CommandProbe::new(Vec::new()).expect("valid probe"))
    }

    fn parse_probe(&self, stdout: &[u8]) -> Result<CommandPreflight, CommandPreflightError> {
        let text = std::str::from_utf8(stdout)
            .map_err(|_| CommandPreflightError::MalformedOutput)?
            .trim();
        let version = text
            .strip_prefix("fixture ")
            .ok_or(CommandPreflightError::MalformedOutput)?;
        CommandPreflight::compatible(Some(version.to_owned()))
    }
}

fn capabilities(tools: bool) -> Capabilities {
    Capabilities {
        streaming: true,
        tools,
        reasoning: ReasoningSupport::Controllable,
        structured_output: true,
        usage: true,
        cache: false,
        prompt_cache: Default::default(),
        cache_contract: None,
        auth: AuthKind::Custom("command".to_owned()),
        continuation: false,
        max_output_tokens: Some(8_192),
    }
}

fn context() -> ProviderCallContext {
    ProviderCallContext {
        session: SessionId::new("session-command"),
        request_id: RequestId::new("request-command"),
        attempt_id: AttemptId::new("attempt-command"),
        cache_identity: None,
        purpose: ProviderAttemptPurpose::Ordinary,
        cancel: Cancellation::new(),
        deadline: Deadline::never(),
    }
}

fn request() -> ProviderRequest {
    ProviderRequest::new(ModelId::new(MODEL), Vec::new())
}

fn shell_script(lines: &[&str]) -> String {
    let args = lines
        .iter()
        .map(|line| format!("'{line}'"))
        .collect::<Vec<_>>()
        .join(" ");
    format!("printf '%s\\n' {args}")
}

fn process_config(script: String) -> CommandProcessConfig {
    process_config_with(script, CommandLimits::default())
}

fn process_config_with(script: String, limits: CommandLimits) -> CommandProcessConfig {
    CommandProcessConfig::new(
        "/bin/sh",
        std::env::current_dir().expect("current directory"),
    )
    .expect("valid shell fixture config")
    .with_fixed_args(vec!["-c".to_owned(), script])
    .expect("valid fixture args")
    .with_limits(limits)
}

fn command_provider(
    script: String,
    adapter: Arc<FixtureAdapter>,
    limits: CommandLimits,
) -> CommandProvider {
    CommandProvider::new(process_config_with(script, limits), adapter)
        .expect("valid command provider")
}

async fn collect(provider: &CommandProvider, request: ProviderRequest) -> Vec<ProviderStreamEvent> {
    let mut stream = provider
        .stream(request, context())
        .await
        .expect("command stream starts");
    let mut events = Vec::new();
    while let Some(event) = stream.next().await {
        events.push(event);
    }
    events
}

#[test]
fn config_and_attempt_debug_redact_environment_and_stdin() {
    let config = process_config("exit 0".to_owned())
        .with_env("TOKEN", "environment-secret")
        .expect("valid environment");
    let attempt = CommandAttempt::new(
        Vec::new(),
        b"stdin-secret".to_vec(),
        Box::new(JsonLinesDecoder),
    );

    let debug = format!("{config:?} {attempt:?}");
    assert!(debug.contains("TOKEN"));
    assert!(debug.contains("stdin_bytes"));
    assert!(!debug.contains("environment-secret"));
    assert!(!debug.contains("stdin-secret"));
}

#[test]
fn config_rejects_relative_paths_and_invalid_bounds() {
    assert!(matches!(
        CommandProcessConfig::new("sh", PathBuf::from(".")),
        Err(CommandConfigError::RelativePath {
            field: "executable"
        })
    ));
    assert!(matches!(
        CommandLimits::default().with_max_stdout_bytes(0),
        Err(CommandConfigError::InvalidLimit {
            field: "max_stdout_bytes"
        })
    ));
}

#[tokio::test]
async fn normalizes_text_reasoning_tool_usage_and_terminal_events() {
    let script = shell_script(&[
        r#"{"type":"text_delta","text":"hello"}"#,
        r#"{"type":"reasoning_delta","text":"think","redacted":false}"#,
        r#"{"type":"tool_call_delta","index":0,"id":"call-1","name":"search","arguments_fragment":"{}"}"#,
        r#"{"type":"usage","delta":{"input_uncached":3,"output":2}}"#,
        r#"{"type":"finish","reason":"tool_calls"}"#,
    ]);
    let provider = command_provider(
        script,
        Arc::new(FixtureAdapter::new(capabilities(true))),
        CommandLimits::default(),
    );
    let events = collect(&provider, request()).await;

    assert!(matches!(
        &events[0],
        ProviderStreamEvent::TextDelta { text } if text == "hello"
    ));
    assert!(matches!(
        &events[1],
        ProviderStreamEvent::ReasoningDelta { text, redacted: false, .. } if text == "think"
    ));
    assert!(matches!(
        &events[2],
        ProviderStreamEvent::ToolCallDelta {
            index: 0,
            id: Some(id),
            name: Some(name),
            arguments_fragment,
        } if id == "call-1" && name == "search" && arguments_fragment == "{}"
    ));
    assert!(matches!(
        &events[3],
        ProviderStreamEvent::Usage { delta }
            if delta.get(CounterKind::InputUncached) == 3
                && delta.get(CounterKind::Output) == 2
    ));
    assert!(matches!(
        events[4],
        ProviderStreamEvent::Finish {
            reason: FinishReason::ToolCalls
        }
    ));
}

#[tokio::test]
async fn clears_ambient_environment_and_passes_only_explicit_values() {
    let script = concat!(
        "printf '{\"type\":\"text_delta\",\"text\":\"%s:%s\"}\\n' ",
        "\"$EXPLICIT\" \"${HOME-unset}\"; ",
        "printf '%s\\n' '{\"type\":\"finish\",\"reason\":\"stop\"}'"
    );
    let config = process_config(script.to_owned())
        .with_env("EXPLICIT", "present")
        .expect("valid environment");
    let provider = CommandProvider::new(config, Arc::new(FixtureAdapter::new(capabilities(false))))
        .expect("valid provider");
    let events = collect(&provider, request()).await;

    assert!(matches!(
        &events[0],
        ProviderStreamEvent::TextDelta { text } if text == "present:unset"
    ));
}

#[tokio::test]
async fn rejects_unsupported_tools_and_vendor_extensions_before_prepare() {
    let adapter = Arc::new(FixtureAdapter::new(capabilities(false)));
    let prepares = adapter.prepares.clone();
    let provider = command_provider("exit 99".to_owned(), adapter, CommandLimits::default());
    let mut tool_request = request();
    tool_request.tools.push(ToolSchema {
        name: "search".to_owned(),
        description: "search".to_owned(),
        input_schema: serde_json::json!({"type": "object"}),
    });
    let tool_error = match provider.stream(tool_request, context()).await {
        Err(error) => error,
        Ok(_) => panic!("tools must fail before spawn"),
    };
    assert_eq!(tool_error.kind, ProviderErrorKind::Unsupported);

    let mut extension_request = request();
    extension_request.vendor_extensions = serde_json::json!({"hidden": true});
    let extension_error = match provider.stream(extension_request, context()).await {
        Err(error) => error,
        Ok(_) => panic!("extensions must fail before spawn"),
    };
    assert_eq!(extension_error.kind, ProviderErrorKind::Unsupported);
    assert_eq!(prepares.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn rejects_oversized_stdin_before_spawn() {
    let limits = CommandLimits::default()
        .with_max_stdin_bytes(4)
        .expect("valid bound");
    let marker = temporary_path("oversized-stdin");
    let script = format!("printf touched > '{}'", marker.display());
    let provider = command_provider(
        script,
        Arc::new(FixtureAdapter::new(capabilities(false)).with_stdin(b"too-large".to_vec())),
        limits,
    );

    let error = match provider.stream(request(), context()).await {
        Err(error) => error,
        Ok(_) => panic!("stdin bound must fail before spawn"),
    };
    assert_eq!(error.kind, ProviderErrorKind::BadRequest);
    assert!(!marker.exists());
}

#[tokio::test]
async fn malformed_missing_and_post_terminal_output_fail_closed() {
    let cases = [
        shell_script(&["not-json"]),
        shell_script(&[r#"{"type":"text_delta","text":"partial"}"#]),
        shell_script(&[
            r#"{"type":"finish","reason":"stop"}"#,
            r#"{"type":"text_delta","text":"late"}"#,
        ]),
    ];

    for script in cases {
        let provider = command_provider(
            script,
            Arc::new(FixtureAdapter::new(capabilities(false))),
            CommandLimits::default(),
        );
        let events = collect(&provider, request()).await;
        assert!(matches!(
            events.last(),
            Some(ProviderStreamEvent::Error { error })
                if error.kind == ProviderErrorKind::MalformedStream
        ));
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, ProviderStreamEvent::Finish { .. }))
        );
    }
}

#[tokio::test]
async fn output_bounds_nonzero_exit_and_stderr_are_safe() {
    let limits = CommandLimits::default()
        .with_max_stdout_bytes(64)
        .expect("valid output bound");
    let oversized = format!(
        "printf '%s\\n' '{}'",
        serde_json::json!({"type": "text_delta", "text": "x".repeat(128)})
    );
    let oversized_provider = command_provider(
        oversized,
        Arc::new(FixtureAdapter::new(capabilities(false))),
        limits,
    );
    let oversized_events = collect(&oversized_provider, request()).await;
    assert!(matches!(
        oversized_events.last(),
        Some(ProviderStreamEvent::Error { error })
            if error.kind == ProviderErrorKind::MalformedStream
    ));

    let failed = format!(
        "printf '%s' 'stderr-secret' >&2; {}; exit 7",
        shell_script(&[r#"{"type":"finish","reason":"stop"}"#])
    );
    let failed_provider = command_provider(
        failed,
        Arc::new(FixtureAdapter::new(capabilities(false))),
        CommandLimits::default(),
    );
    let failed_events = collect(&failed_provider, request()).await;
    let debug = format!("{failed_events:?}");
    assert!(matches!(
        failed_events.last(),
        Some(ProviderStreamEvent::Error { error })
            if error.kind == ProviderErrorKind::Server
    ));
    assert!(!debug.contains("stderr-secret"));

    let stderr_bounded = format!(
        "printf '%s' '{}' >&2; {}",
        "s".repeat(128),
        shell_script(&[r#"{"type":"finish","reason":"stop"}"#])
    );
    let stderr_provider = command_provider(
        stderr_bounded,
        Arc::new(FixtureAdapter::new(capabilities(false))),
        CommandLimits::default()
            .with_max_stderr_bytes(16)
            .expect("valid stderr bound"),
    );
    let stderr_events = collect(&stderr_provider, request()).await;
    assert!(matches!(
        stderr_events.last(),
        Some(ProviderStreamEvent::Error { error })
            if error.kind == ProviderErrorKind::MalformedStream
    ));

    let retryable = serde_json::to_string(&ProviderStreamEvent::Error {
        error: ProviderError::new(ProviderErrorKind::RateLimited, "fixture rate limit").retryable(),
    })
    .expect("serialize fixture error");
    let retryable_provider = command_provider(
        shell_script(&[&retryable]),
        Arc::new(FixtureAdapter::new(capabilities(false))),
        CommandLimits::default(),
    );
    let retryable_events = collect(&retryable_provider, request()).await;
    assert!(matches!(
        retryable_events.last(),
        Some(ProviderStreamEvent::Error { error })
            if error.kind == ProviderErrorKind::RateLimited && error.retryable
    ));
}

#[tokio::test]
async fn cancellation_and_deadline_terminate_attempts() {
    let provider = command_provider(
        "/bin/sleep 30".to_owned(),
        Arc::new(FixtureAdapter::new(capabilities(false))),
        CommandLimits::default(),
    );
    let mut cancelled_context = context();
    let cancellation = cancelled_context.cancel.clone();
    let mut cancelled_stream = provider
        .stream(request(), cancelled_context.clone())
        .await
        .expect("stream starts");
    cancellation.cancel(CancelReason::UserRequested);
    let cancelled = cancelled_stream.next().await.expect("cancel terminal");
    assert!(matches!(
        cancelled,
        ProviderStreamEvent::Error { error }
            if error.kind == ProviderErrorKind::Cancelled
    ));

    cancelled_context.cancel = Cancellation::new();
    cancelled_context.deadline = Deadline::after(&SystemClock, 25);
    let mut timed_stream = provider
        .stream(request(), cancelled_context)
        .await
        .expect("timed stream starts");
    let timed = timed_stream.next().await.expect("timeout terminal");
    assert!(matches!(
        timed,
        ProviderStreamEvent::Error { error }
            if error.kind == ProviderErrorKind::Timeout
    ));
}

#[tokio::test]
async fn early_stream_drop_and_normal_terminal_cleanup_descendants() {
    let early_pid = temporary_path("early-descendant");
    let early_script = format!(
        "/bin/sleep 30 >/dev/null 2>&1 & child=$!; printf '%s' \"$child\" > '{}'; {}; wait",
        early_pid.display(),
        shell_script(&[r#"{"type":"text_delta","text":"started"}"#])
    );
    let provider = command_provider(
        early_script,
        Arc::new(FixtureAdapter::new(capabilities(false))),
        CommandLimits::default()
            .with_cleanup_timeout(Duration::from_millis(250))
            .expect("valid cleanup bound"),
    );
    let mut stream = provider
        .stream(request(), context())
        .await
        .expect("stream starts");
    assert!(matches!(
        stream.next().await,
        Some(ProviderStreamEvent::TextDelta { .. })
    ));
    let early_child = read_pid(&early_pid).await;
    drop(stream);
    wait_until_dead(early_child).await;

    let normal_pid = temporary_path("normal-descendant");
    let normal_script = format!(
        "/bin/sleep 30 >/dev/null 2>&1 & child=$!; printf '%s' \"$child\" > '{}'; {}",
        normal_pid.display(),
        shell_script(&[r#"{"type":"finish","reason":"stop"}"#])
    );
    let provider = command_provider(
        normal_script,
        Arc::new(FixtureAdapter::new(capabilities(false))),
        CommandLimits::default()
            .with_cleanup_timeout(Duration::from_millis(250))
            .expect("valid cleanup bound"),
    );
    let events = collect(&provider, request()).await;
    let normal_child = read_pid(&normal_pid).await;
    wait_until_dead(normal_child).await;
    assert!(matches!(
        events.last(),
        Some(ProviderStreamEvent::Finish {
            reason: FinishReason::Stop
        })
    ));
}

#[tokio::test]
async fn preflight_is_explicit_bounded_and_versioned() {
    let marker = temporary_path("probe-marker");
    let script = format!(
        "printf 'fixture 1.2.3\\n'; printf touched > '{}'",
        marker.display()
    );
    let provider = command_provider(
        script,
        Arc::new(FixtureAdapter::new(capabilities(false)).with_probe()),
        CommandLimits::default(),
    );
    assert!(!marker.exists(), "construction must not run the probe");

    let preflight = provider
        .preflight()
        .await
        .expect("probe succeeds")
        .expect("probe configured");
    assert!(preflight.is_compatible());
    assert_eq!(preflight.version(), Some("1.2.3"));
    assert!(marker.exists());
}

#[tokio::test]
async fn preflight_supports_absence_timeout_and_output_bounds() {
    let absent = command_provider(
        "exit 99".to_owned(),
        Arc::new(FixtureAdapter::new(capabilities(false))),
        CommandLimits::default(),
    );
    assert_eq!(absent.preflight().await.expect("absence is valid"), None);

    let timed = command_provider(
        "/bin/sleep 30".to_owned(),
        Arc::new(FixtureAdapter::new(capabilities(false)).with_probe()),
        CommandLimits::default()
            .with_probe_timeout(Duration::from_millis(25))
            .expect("valid timeout"),
    );
    assert_eq!(
        timed.preflight().await.expect_err("probe times out"),
        CommandPreflightError::Timeout
    );

    let bounded = command_provider(
        "printf 'fixture 1234567890'".to_owned(),
        Arc::new(FixtureAdapter::new(capabilities(false)).with_probe()),
        CommandLimits::default()
            .with_max_probe_stdout_bytes(4)
            .expect("valid output bound"),
    );
    assert_eq!(
        bounded
            .preflight()
            .await
            .expect_err("probe output is bounded"),
        CommandPreflightError::OutputLimit
    );
}

fn temporary_path(label: &str) -> PathBuf {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    std::env::temp_dir().join(format!(
        "agent-runtime-command-{label}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ))
}

async fn read_pid(path: &PathBuf) -> u32 {
    for _ in 0..40 {
        if let Ok(text) = std::fs::read_to_string(path) {
            if let Ok(pid) = text.parse() {
                let _ = std::fs::remove_file(path);
                return pid;
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("fixture did not publish a child pid")
}

async fn wait_until_dead(pid: u32) {
    for _ in 0..80 {
        let alive = std::process::Command::new("/bin/kill")
            .args(["-0", &pid.to_string()])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .is_ok_and(|status| status.success());
        let zombie = alive
            && std::process::Command::new("/bin/ps")
                .args(["-o", "stat=", "-p", &pid.to_string()])
                .output()
                .ok()
                .and_then(|output| String::from_utf8(output.stdout).ok())
                .is_some_and(|state| state.trim_start().starts_with('Z'));
        if !alive || zombie {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("descendant process {pid} survived bounded cleanup")
}
