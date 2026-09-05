//! Process-bounded provider support for trusted model CLI protocol adapters.
//!
//! This module deliberately supplies mechanism, not a generic shell template
//! or a second agent loop. A consumer-owned [`CommandAdapter`] declares exact
//! model capabilities, converts one canonical provider request into argv plus
//! stdin, and decodes machine stdout back into canonical provider events.
//! [`CommandProvider`] owns the child process, environment isolation, output
//! bounds, cancellation, deadline, and process-tree cleanup.
//!
//! The adapter is trusted code. User-authored configuration must be resolved
//! and authorized by the embedding host before it constructs these types.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::sync::Arc;
use std::time::Duration;

use agent_runtime_core::clock::SystemClock;
use agent_runtime_core::provider::{
    Capabilities, ModelDescriptor, ModelId, Provider, ProviderCallContext, ProviderError,
    ProviderErrorKind, ProviderRequest, ProviderStream, ProviderStreamEvent,
};
use async_trait::async_trait;
use command_group::{AsyncCommandGroup, AsyncGroupChild};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::process::{ChildStderr, ChildStdin, ChildStdout, Command};
use tokio::sync::{Notify, mpsc, oneshot};
use zeroize::Zeroizing;

const MAX_ARGUMENTS: usize = 256;
const MAX_ARGUMENT_BYTES: usize = 64 * 1024;
const MAX_ENVIRONMENT_ENTRIES: usize = 256;
const MAX_ENVIRONMENT_BYTES: usize = 1024 * 1024;
const HARD_MAX_STDIN_BYTES: usize = 16 * 1024 * 1024;
const HARD_MAX_FRAME_BYTES: usize = 4 * 1024 * 1024;
const HARD_MAX_STDOUT_BYTES: usize = 64 * 1024 * 1024;
const HARD_MAX_STDERR_BYTES: usize = 1024 * 1024;
const HARD_MAX_PROBE_BYTES: usize = 1024 * 1024;
const HARD_MAX_CLEANUP_MS: u64 = 30_000;
const HARD_MAX_PROBE_MS: u64 = 60_000;
const EVENT_CHANNEL_CAPACITY: usize = 64;

/// Resource limits applied to every command-provider process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandLimits {
    max_stdin_bytes: usize,
    max_frame_bytes: usize,
    max_stdout_bytes: usize,
    max_stderr_bytes: usize,
    cleanup_timeout: Duration,
    probe_timeout: Duration,
    max_probe_stdout_bytes: usize,
}

impl Default for CommandLimits {
    fn default() -> Self {
        Self {
            max_stdin_bytes: 4 * 1024 * 1024,
            max_frame_bytes: 1024 * 1024,
            max_stdout_bytes: 8 * 1024 * 1024,
            max_stderr_bytes: 64 * 1024,
            cleanup_timeout: Duration::from_secs(5),
            probe_timeout: Duration::from_secs(5),
            max_probe_stdout_bytes: 64 * 1024,
        }
    }
}

impl CommandLimits {
    /// Replaces the maximum stdin payload size.
    pub fn with_max_stdin_bytes(mut self, value: usize) -> Result<Self, CommandConfigError> {
        validate_limit("max_stdin_bytes", value, HARD_MAX_STDIN_BYTES)?;
        self.max_stdin_bytes = value;
        Ok(self)
    }

    /// Replaces the maximum size of one newline-delimited stdout frame.
    pub fn with_max_frame_bytes(mut self, value: usize) -> Result<Self, CommandConfigError> {
        validate_limit("max_frame_bytes", value, HARD_MAX_FRAME_BYTES)?;
        self.max_frame_bytes = value;
        Ok(self)
    }

    /// Replaces the aggregate stdout limit for one provider attempt.
    pub fn with_max_stdout_bytes(mut self, value: usize) -> Result<Self, CommandConfigError> {
        validate_limit("max_stdout_bytes", value, HARD_MAX_STDOUT_BYTES)?;
        self.max_stdout_bytes = value;
        Ok(self)
    }

    /// Replaces the aggregate stderr limit. The drain continues discarding
    /// after the limit so the child cannot block, but the attempt fails.
    pub fn with_max_stderr_bytes(mut self, value: usize) -> Result<Self, CommandConfigError> {
        validate_limit("max_stderr_bytes", value, HARD_MAX_STDERR_BYTES)?;
        self.max_stderr_bytes = value;
        Ok(self)
    }

    /// Replaces the bounded process-tree cleanup grace period.
    pub fn with_cleanup_timeout(mut self, value: Duration) -> Result<Self, CommandConfigError> {
        validate_duration("cleanup_timeout", value, HARD_MAX_CLEANUP_MS)?;
        self.cleanup_timeout = value;
        Ok(self)
    }

    /// Replaces the explicit preflight probe timeout.
    pub fn with_probe_timeout(mut self, value: Duration) -> Result<Self, CommandConfigError> {
        validate_duration("probe_timeout", value, HARD_MAX_PROBE_MS)?;
        self.probe_timeout = value;
        Ok(self)
    }

    /// Replaces the aggregate stdout limit for one explicit preflight probe.
    pub fn with_max_probe_stdout_bytes(mut self, value: usize) -> Result<Self, CommandConfigError> {
        validate_limit("max_probe_stdout_bytes", value, HARD_MAX_PROBE_BYTES)?;
        self.max_probe_stdout_bytes = value;
        Ok(self)
    }

    /// Maximum stdin bytes accepted from an adapter.
    pub fn max_stdin_bytes(&self) -> usize {
        self.max_stdin_bytes
    }

    /// Maximum bytes accepted in one stdout frame.
    pub fn max_frame_bytes(&self) -> usize {
        self.max_frame_bytes
    }

    /// Maximum aggregate stdout bytes accepted for an attempt.
    pub fn max_stdout_bytes(&self) -> usize {
        self.max_stdout_bytes
    }

    /// Maximum aggregate stderr bytes accepted for an attempt.
    pub fn max_stderr_bytes(&self) -> usize {
        self.max_stderr_bytes
    }

    /// Process-tree cleanup grace period.
    pub fn cleanup_timeout(&self) -> Duration {
        self.cleanup_timeout
    }

    /// Explicit preflight probe timeout.
    pub fn probe_timeout(&self) -> Duration {
        self.probe_timeout
    }

    /// Maximum stdout bytes accepted from a preflight probe.
    pub fn max_probe_stdout_bytes(&self) -> usize {
        self.max_probe_stdout_bytes
    }
}

/// Immutable host-authorized process configuration.
#[derive(Clone)]
pub struct CommandProcessConfig {
    executable: PathBuf,
    fixed_args: Vec<String>,
    cwd: PathBuf,
    env: BTreeMap<String, Zeroizing<String>>,
    limits: CommandLimits,
}

impl CommandProcessConfig {
    /// Creates a config from an exact executable and working directory.
    ///
    /// Both inputs must already be absolute. They are canonicalized once so a
    /// later diagnostic and spawn refer to the same authorized target.
    pub fn new(
        executable: impl Into<PathBuf>,
        cwd: impl Into<PathBuf>,
    ) -> Result<Self, CommandConfigError> {
        let executable = canonical_file(executable.into(), "executable")?;
        validate_executable(&executable)?;
        let cwd = canonical_directory(cwd.into(), "cwd")?;
        Ok(Self {
            executable,
            fixed_args: Vec::new(),
            cwd,
            env: BTreeMap::new(),
            limits: CommandLimits::default(),
        })
    }

    /// Replaces the fixed, non-secret argument prefix.
    pub fn with_fixed_args(mut self, args: Vec<String>) -> Result<Self, CommandConfigError> {
        validate_args(&args)?;
        self.fixed_args = args;
        Ok(self)
    }

    /// Adds one explicitly authorized child environment value.
    pub fn with_env(
        mut self,
        name: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<Self, CommandConfigError> {
        let name = name.into();
        let value = value.into();
        validate_environment_entry(&name, &value)?;
        if !self.env.contains_key(&name) && self.env.len() >= MAX_ENVIRONMENT_ENTRIES {
            return Err(CommandConfigError::TooManyEnvironmentEntries);
        }
        let existing_bytes = environment_bytes(&self.env);
        let replaced_bytes = self
            .env
            .get(&name)
            .map_or(0, |existing| name.len() + existing.len());
        let next_bytes = existing_bytes
            .saturating_sub(replaced_bytes)
            .saturating_add(name.len())
            .saturating_add(value.len());
        if next_bytes > MAX_ENVIRONMENT_BYTES {
            return Err(CommandConfigError::EnvironmentTooLarge);
        }
        self.env.insert(name, Zeroizing::new(value));
        Ok(self)
    }

    /// Replaces the validated resource limits.
    pub fn with_limits(mut self, limits: CommandLimits) -> Self {
        self.limits = limits;
        self
    }

    /// Canonical executable path used directly without a shell.
    pub fn executable(&self) -> &Path {
        &self.executable
    }

    /// Fixed argument prefix.
    pub fn fixed_args(&self) -> &[String] {
        &self.fixed_args
    }

    /// Canonical child working directory.
    pub fn cwd(&self) -> &Path {
        &self.cwd
    }

    /// Names of explicitly authorized child environment entries.
    pub fn environment_names(&self) -> impl Iterator<Item = &str> {
        self.env.keys().map(String::as_str)
    }

    /// Process resource bounds.
    pub fn limits(&self) -> &CommandLimits {
        &self.limits
    }
}

impl fmt::Debug for CommandProcessConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CommandProcessConfig")
            .field("executable", &self.executable)
            .field("fixed_args", &self.fixed_args)
            .field("cwd", &self.cwd)
            .field("environment_names", &self.env.keys().collect::<Vec<_>>())
            .field("limits", &self.limits)
            .finish()
    }
}

/// Invalid command process configuration.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum CommandConfigError {
    /// A path was not absolute.
    #[error("command {field} must be an absolute path")]
    RelativePath {
        /// Configuration field.
        field: &'static str,
    },
    /// A path could not be resolved.
    #[error("command {field} could not be resolved")]
    UnresolvablePath {
        /// Configuration field.
        field: &'static str,
    },
    /// The executable target is not a regular file.
    #[error("command executable must be a regular file")]
    ExecutableNotFile,
    /// The executable target lacks execute permission.
    #[error("command executable is not executable")]
    ExecutableNotExecutable,
    /// The working directory is not a directory.
    #[error("command cwd must be a directory")]
    CwdNotDirectory,
    /// Too many arguments were supplied.
    #[error("command argument count exceeds the supported bound")]
    TooManyArguments,
    /// An argument is too large or contains NUL.
    #[error("command argument is invalid")]
    InvalidArgument,
    /// An environment name is invalid.
    #[error("command environment name is invalid")]
    InvalidEnvironmentName,
    /// An environment value contains NUL.
    #[error("command environment value is invalid")]
    InvalidEnvironmentValue,
    /// Too many explicit environment entries were supplied.
    #[error("command environment entry count exceeds the supported bound")]
    TooManyEnvironmentEntries,
    /// The explicit environment exceeds its aggregate size bound.
    #[error("command environment exceeds the supported size bound")]
    EnvironmentTooLarge,
    /// A byte limit was zero or exceeded its hard ceiling.
    #[error("command limit `{field}` is outside the supported range")]
    InvalidLimit {
        /// Limit field.
        field: &'static str,
    },
}

/// One adapter-prepared provider attempt.
pub struct CommandAttempt {
    args: Vec<String>,
    stdin: Zeroizing<Vec<u8>>,
    decoder: Box<dyn CommandOutputDecoder>,
}

impl CommandAttempt {
    /// Creates an attempt from adapter-specific arguments, stdin, and decoder.
    /// Prompt and secret-bearing data should travel through `stdin`, never
    /// argv, because argv is process metadata on common operating systems.
    pub fn new(
        args: Vec<String>,
        stdin: impl Into<Vec<u8>>,
        decoder: Box<dyn CommandOutputDecoder>,
    ) -> Self {
        Self {
            args,
            stdin: Zeroizing::new(stdin.into()),
            decoder,
        }
    }
}

impl fmt::Debug for CommandAttempt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CommandAttempt")
            .field("args", &self.args)
            .field("stdin_bytes", &self.stdin.len())
            .field("decoder", &self.decoder)
            .finish()
    }
}

/// Attempt-local machine-stdout decoder supplied by a trusted adapter.
pub trait CommandOutputDecoder: Send + fmt::Debug {
    /// Decodes one stdout frame without its trailing newline.
    fn decode_frame(&mut self, frame: &[u8]) -> Result<Vec<ProviderStreamEvent>, ProviderError>;

    /// Flushes any bounded decoder state at stdout EOF.
    fn finish(&mut self) -> Result<Vec<ProviderStreamEvent>, ProviderError> {
        Ok(Vec::new())
    }
}

/// Optional adapter-supplied explicit compatibility probe.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CommandProbe {
    args: Vec<String>,
}

impl CommandProbe {
    /// Creates a probe argument suffix. It is appended after fixed arguments.
    pub fn new(args: Vec<String>) -> Result<Self, CommandConfigError> {
        validate_args(&args)?;
        Ok(Self { args })
    }

    /// Probe argument suffix.
    pub fn args(&self) -> &[String] {
        &self.args
    }
}

/// Redaction-safe result of one explicit compatibility probe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandPreflight {
    version: Option<String>,
    compatible: bool,
    detail: Option<String>,
}

impl CommandPreflight {
    /// Reports a compatible executable and optional bounded version label.
    pub fn compatible(version: Option<String>) -> Result<Self, CommandPreflightError> {
        validate_probe_text(version.as_deref(), "version", 128)?;
        Ok(Self {
            version,
            compatible: true,
            detail: None,
        })
    }

    /// Reports an incompatible executable with bounded redaction-safe detail.
    pub fn incompatible(
        version: Option<String>,
        detail: impl Into<String>,
    ) -> Result<Self, CommandPreflightError> {
        let detail = detail.into();
        validate_probe_text(version.as_deref(), "version", 128)?;
        validate_probe_text(Some(&detail), "detail", 512)?;
        Ok(Self {
            version,
            compatible: false,
            detail: Some(detail),
        })
    }

    /// Parsed version label, if the adapter exposed one.
    pub fn version(&self) -> Option<&str> {
        self.version.as_deref()
    }

    /// Whether the adapter accepts the executable's version.
    pub fn is_compatible(&self) -> bool {
        self.compatible
    }

    /// Bounded redaction-safe incompatibility detail.
    pub fn detail(&self) -> Option<&str> {
        self.detail.as_deref()
    }
}

/// Explicit compatibility probe failure.
#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum CommandPreflightError {
    /// The executable could not be started or supervised.
    #[error("command compatibility probe is unavailable")]
    Unavailable,
    /// The probe exceeded its independent time bound.
    #[error("command compatibility probe timed out")]
    Timeout,
    /// Probe stdout exceeded its byte bound.
    #[error("command compatibility probe exceeded its output bound")]
    OutputLimit,
    /// The process returned a non-success status.
    #[error("command compatibility probe exited unsuccessfully")]
    UnsuccessfulExit,
    /// The adapter could not parse or validate machine probe output.
    #[error("command compatibility probe output is malformed")]
    MalformedOutput,
    /// The adapter has no probe parser because it did not declare a probe.
    #[error("command adapter does not define a compatibility probe")]
    NotConfigured,
    /// Parsed metadata exceeded its redaction-safe contract.
    #[error("command compatibility {field} is not bounded redaction-safe text")]
    InvalidMetadata {
        /// Metadata field.
        field: &'static str,
    },
}

/// Trusted consumer-owned protocol adapter for one model CLI family.
pub trait CommandAdapter: Send + Sync + fmt::Debug + 'static {
    /// Stable model descriptors and exact capabilities served by this adapter.
    fn describe(&self) -> Vec<ModelDescriptor>;

    /// Validates and prepares one visible provider attempt before process I/O.
    fn prepare(
        &self,
        request: &ProviderRequest,
        context: &ProviderCallContext,
    ) -> Result<CommandAttempt, ProviderError>;

    /// Whether this adapter explicitly validates and consumes non-null vendor
    /// extensions. The conservative default rejects them before `prepare`.
    fn accepts_vendor_extensions(&self) -> bool {
        false
    }

    /// Optional explicit version/availability probe.
    fn probe(&self) -> Option<CommandProbe> {
        None
    }

    /// Parses bounded machine stdout for a declared probe.
    fn parse_probe(&self, _stdout: &[u8]) -> Result<CommandPreflight, CommandPreflightError> {
        Err(CommandPreflightError::NotConfigured)
    }
}

/// Invalid provider construction from an adapter declaration.
#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum CommandProviderBuildError {
    /// The adapter advertised no models.
    #[error("command adapter must advertise at least one model")]
    NoModels,
    /// One model id was empty or unbounded.
    #[error("command adapter advertised an invalid model id")]
    InvalidModelId,
    /// The adapter advertised the same model more than once.
    #[error("command adapter advertised duplicate model `{model}`")]
    DuplicateModel {
        /// Duplicate model id.
        model: String,
    },
    /// A model's cache capability declaration was contradictory.
    #[error("command adapter advertised an invalid capability contract for `{model}`")]
    InvalidCapabilities {
        /// Model id.
        model: String,
    },
}

/// A process-backed implementation of the canonical provider contract.
pub struct CommandProvider {
    process: Arc<CommandProcessConfig>,
    adapter: Arc<dyn CommandAdapter>,
    models: Vec<ModelDescriptor>,
}

impl CommandProvider {
    /// Validates a trusted adapter declaration without spawning a process.
    pub fn new(
        process: CommandProcessConfig,
        adapter: Arc<dyn CommandAdapter>,
    ) -> Result<Self, CommandProviderBuildError> {
        let models = adapter.describe();
        validate_models(&models)?;
        Ok(Self {
            process: Arc::new(process),
            adapter,
            models,
        })
    }

    /// Static process metadata. Reading it never probes or spawns.
    pub fn process_config(&self) -> &CommandProcessConfig {
        &self.process
    }

    /// Runs the optional adapter-declared compatibility probe explicitly.
    pub async fn preflight(&self) -> Result<Option<CommandPreflight>, CommandPreflightError> {
        let Some(probe) = self.adapter.probe() else {
            return Ok(None);
        };
        let stdout = run_probe(self.process.clone(), probe).await?;
        self.adapter.parse_probe(&stdout).map(Some)
    }
}

impl fmt::Debug for CommandProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CommandProvider")
            .field("process", &self.process)
            .field("adapter", &self.adapter)
            .field("models", &self.models)
            .finish()
    }
}

#[async_trait]
impl Provider for CommandProvider {
    fn describe(&self) -> Vec<ModelDescriptor> {
        self.models.clone()
    }

    fn capabilities(&self, model: &ModelId) -> Option<Capabilities> {
        self.models
            .iter()
            .find(|descriptor| &descriptor.id == model)
            .map(|descriptor| descriptor.capabilities.clone())
    }

    async fn stream(
        &self,
        request: ProviderRequest,
        context: ProviderCallContext,
    ) -> Result<ProviderStream, ProviderError> {
        if context.cancel.is_cancelled() {
            return Err(provider_error(
                ProviderErrorKind::Cancelled,
                "command provider attempt was cancelled before spawn",
            ));
        }
        if context.deadline.is_expired(&SystemClock) {
            return Err(provider_error(
                ProviderErrorKind::Timeout,
                "command provider deadline elapsed before spawn",
            ));
        }

        let Some(capabilities) = self.capabilities(&request.model) else {
            return Err(provider_error(
                ProviderErrorKind::BadRequest,
                "command adapter does not serve the requested model",
            ));
        };
        let unsupported = capabilities.unsupported_for(&request);
        if !unsupported.is_empty() {
            return Err(ProviderError::unsupported(&unsupported));
        }
        if !request.vendor_extensions.is_null() && !self.adapter.accepts_vendor_extensions() {
            return Err(provider_error(
                ProviderErrorKind::Unsupported,
                "command adapter does not accept vendor extensions",
            ));
        }

        let attempt = self.adapter.prepare(&request, &context)?;
        validate_attempt(&attempt, &self.process.limits)?;
        start_attempt(self.process.clone(), attempt, context).await
    }
}

struct DropSignal(Option<oneshot::Sender<()>>);

impl Drop for DropSignal {
    fn drop(&mut self) {
        if let Some(signal) = self.0.take() {
            let _ = signal.send(());
        }
    }
}

async fn start_attempt(
    process: Arc<CommandProcessConfig>,
    attempt: CommandAttempt,
    context: ProviderCallContext,
) -> Result<ProviderStream, ProviderError> {
    let CommandAttempt {
        args,
        stdin,
        decoder,
    } = attempt;
    let mut command = build_command(&process, &args, true);
    let mut group = command.group();
    group.kill_on_drop(true);
    let mut child = group.spawn().map_err(|_| {
        provider_error(
            ProviderErrorKind::Network,
            "command provider process could not be started",
        )
    })?;
    let stdin_handle = child.inner().stdin.take().ok_or_else(|| {
        provider_error(
            ProviderErrorKind::Network,
            "command provider stdin pipe is unavailable",
        )
    })?;
    let stdout = child.inner().stdout.take().ok_or_else(|| {
        provider_error(
            ProviderErrorKind::Network,
            "command provider stdout pipe is unavailable",
        )
    })?;
    let stderr = child.inner().stderr.take().ok_or_else(|| {
        provider_error(
            ProviderErrorKind::Network,
            "command provider stderr pipe is unavailable",
        )
    })?;

    let (events_tx, mut events_rx) = mpsc::channel(EVENT_CHANNEL_CAPACITY);
    let (drop_tx, drop_rx) = oneshot::channel();
    let limits = process.limits.clone();

    // The task is intentionally detached from the caller's JoinHandle: the
    // returned stream owns `drop_tx`, and the supervisor owns and always
    // cleans the process group even when that stream is abandoned.
    drop(tokio::spawn(supervise_attempt(
        SpawnedAttempt {
            child,
            stdin_handle,
            stdout,
            stderr,
            stdin,
        },
        decoder,
        context,
        limits,
        events_tx,
        drop_rx,
    )));

    let stream = async_stream::stream! {
        let _drop_signal = DropSignal(Some(drop_tx));
        while let Some(event) = events_rx.recv().await {
            yield event;
        }
    };
    Ok(Box::pin(stream))
}

#[derive(Debug)]
enum OutputFailure {
    Provider(ProviderError),
    ReceiverClosed,
}

enum FirstPhase {
    Output(Result<ProviderStreamEvent, OutputFailure>),
    InputFailed,
    StderrLimit,
    Cancelled,
    TimedOut,
    Dropped,
}

struct SpawnedAttempt {
    child: AsyncGroupChild,
    stdin_handle: ChildStdin,
    stdout: ChildStdout,
    stderr: ChildStderr,
    stdin: Zeroizing<Vec<u8>>,
}

async fn supervise_attempt(
    attempt: SpawnedAttempt,
    decoder: Box<dyn CommandOutputDecoder>,
    context: ProviderCallContext,
    limits: CommandLimits,
    events: mpsc::Sender<ProviderStreamEvent>,
    mut dropped: oneshot::Receiver<()>,
) {
    let SpawnedAttempt {
        mut child,
        mut stdin_handle,
        stdout,
        stderr,
        stdin,
    } = attempt;
    let mut writer = tokio::spawn(async move {
        stdin_handle.write_all(&stdin).await?;
        stdin_handle.shutdown().await
    });
    let stderr_limit = Arc::new(Notify::new());
    let mut stderr_drain = tokio::spawn(drain_stderr(
        stderr,
        limits.max_stderr_bytes,
        stderr_limit.clone(),
    ));
    let output = decode_stdout(stdout, decoder, &limits, events.clone());
    tokio::pin!(output);
    let deadline = wait_for_deadline(context.deadline);
    tokio::pin!(deadline);
    let mut writer_done = false;

    let first = loop {
        tokio::select! {
            biased;
            _ = &mut dropped => break FirstPhase::Dropped,
            _ = context.cancel.cancelled() => break FirstPhase::Cancelled,
            _ = &mut deadline => break FirstPhase::TimedOut,
            _ = stderr_limit.notified() => break FirstPhase::StderrLimit,
            write = &mut writer, if !writer_done => {
                writer_done = true;
                match write {
                    Ok(Ok(())) => {}
                    Ok(Err(_)) | Err(_) => break FirstPhase::InputFailed,
                }
            }
            decoded = &mut output => break FirstPhase::Output(decoded),
        }
    };

    let terminal = match first {
        FirstPhase::Dropped => {
            terminate_group(&mut child, limits.cleanup_timeout).await;
            writer.abort();
            finish_drain(&mut stderr_drain, limits.cleanup_timeout).await;
            return;
        }
        FirstPhase::Cancelled => {
            terminate_group(&mut child, limits.cleanup_timeout).await;
            writer.abort();
            finish_drain(&mut stderr_drain, limits.cleanup_timeout).await;
            send_error(
                &events,
                provider_error(
                    ProviderErrorKind::Cancelled,
                    "command provider attempt was cancelled",
                ),
            )
            .await;
            return;
        }
        FirstPhase::TimedOut => {
            terminate_group(&mut child, limits.cleanup_timeout).await;
            writer.abort();
            finish_drain(&mut stderr_drain, limits.cleanup_timeout).await;
            send_error(
                &events,
                provider_error(
                    ProviderErrorKind::Timeout,
                    "command provider attempt exceeded its deadline",
                ),
            )
            .await;
            return;
        }
        FirstPhase::InputFailed => {
            terminate_group(&mut child, limits.cleanup_timeout).await;
            finish_drain(&mut stderr_drain, limits.cleanup_timeout).await;
            send_error(
                &events,
                provider_error(
                    ProviderErrorKind::Network,
                    "command provider stdin write failed",
                ),
            )
            .await;
            return;
        }
        FirstPhase::StderrLimit => {
            terminate_group(&mut child, limits.cleanup_timeout).await;
            writer.abort();
            finish_drain(&mut stderr_drain, limits.cleanup_timeout).await;
            send_error(
                &events,
                provider_error(
                    ProviderErrorKind::MalformedStream,
                    "command provider stderr exceeded its configured bound",
                ),
            )
            .await;
            return;
        }
        FirstPhase::Output(Err(OutputFailure::ReceiverClosed)) => {
            terminate_group(&mut child, limits.cleanup_timeout).await;
            writer.abort();
            finish_drain(&mut stderr_drain, limits.cleanup_timeout).await;
            return;
        }
        FirstPhase::Output(Err(OutputFailure::Provider(error))) => {
            terminate_group(&mut child, limits.cleanup_timeout).await;
            writer.abort();
            finish_drain(&mut stderr_drain, limits.cleanup_timeout).await;
            send_error(&events, error).await;
            return;
        }
        FirstPhase::Output(Ok(terminal)) => terminal,
    };

    if !writer_done {
        let input_phase = tokio::select! {
            biased;
            _ = &mut dropped => FirstPhase::Dropped,
            _ = context.cancel.cancelled() => FirstPhase::Cancelled,
            _ = &mut deadline => FirstPhase::TimedOut,
            _ = stderr_limit.notified() => FirstPhase::StderrLimit,
            write = &mut writer => match write {
                Ok(Ok(())) => FirstPhase::Output(Ok(terminal.clone())),
                Ok(Err(_)) | Err(_) => FirstPhase::InputFailed,
            },
        };
        if !matches!(input_phase, FirstPhase::Output(Ok(_))) {
            terminate_group(&mut child, limits.cleanup_timeout).await;
            finish_drain(&mut stderr_drain, limits.cleanup_timeout).await;
            match input_phase {
                FirstPhase::Dropped => {}
                FirstPhase::Cancelled => {
                    send_error(
                        &events,
                        provider_error(
                            ProviderErrorKind::Cancelled,
                            "command provider attempt was cancelled",
                        ),
                    )
                    .await;
                }
                FirstPhase::TimedOut => {
                    send_error(
                        &events,
                        provider_error(
                            ProviderErrorKind::Timeout,
                            "command provider attempt exceeded its deadline",
                        ),
                    )
                    .await;
                }
                FirstPhase::InputFailed => {
                    send_error(
                        &events,
                        provider_error(
                            ProviderErrorKind::Network,
                            "command provider stdin write failed",
                        ),
                    )
                    .await;
                }
                FirstPhase::StderrLimit => {
                    send_error(
                        &events,
                        provider_error(
                            ProviderErrorKind::MalformedStream,
                            "command provider stderr exceeded its configured bound",
                        ),
                    )
                    .await;
                }
                FirstPhase::Output(_) => {}
            }
            return;
        }
    }

    let wait_phase = {
        let wait = child.wait();
        tokio::pin!(wait);
        let cleanup = tokio::time::sleep(limits.cleanup_timeout);
        tokio::pin!(cleanup);
        tokio::select! {
            biased;
            _ = &mut dropped => WaitPhase::Dropped,
            _ = context.cancel.cancelled() => WaitPhase::Cancelled,
            _ = &mut deadline => WaitPhase::TimedOut,
            _ = stderr_limit.notified() => WaitPhase::StderrLimit,
            _ = &mut cleanup => WaitPhase::CleanupTimedOut,
            status = &mut wait => WaitPhase::Exited(status),
        }
    };

    let status = match wait_phase {
        WaitPhase::Exited(Ok(status)) => status,
        WaitPhase::Exited(Err(_)) => {
            terminate_group(&mut child, limits.cleanup_timeout).await;
            finish_drain(&mut stderr_drain, limits.cleanup_timeout).await;
            send_error(
                &events,
                provider_error(
                    ProviderErrorKind::Network,
                    "command provider process could not be reaped",
                ),
            )
            .await;
            return;
        }
        WaitPhase::Dropped => {
            terminate_group(&mut child, limits.cleanup_timeout).await;
            finish_drain(&mut stderr_drain, limits.cleanup_timeout).await;
            return;
        }
        WaitPhase::Cancelled => {
            terminate_group(&mut child, limits.cleanup_timeout).await;
            finish_drain(&mut stderr_drain, limits.cleanup_timeout).await;
            send_error(
                &events,
                provider_error(
                    ProviderErrorKind::Cancelled,
                    "command provider attempt was cancelled",
                ),
            )
            .await;
            return;
        }
        WaitPhase::TimedOut => {
            terminate_group(&mut child, limits.cleanup_timeout).await;
            finish_drain(&mut stderr_drain, limits.cleanup_timeout).await;
            send_error(
                &events,
                provider_error(
                    ProviderErrorKind::Timeout,
                    "command provider attempt exceeded its deadline",
                ),
            )
            .await;
            return;
        }
        WaitPhase::CleanupTimedOut => {
            terminate_group(&mut child, limits.cleanup_timeout).await;
            finish_drain(&mut stderr_drain, limits.cleanup_timeout).await;
            send_error(
                &events,
                provider_error(
                    ProviderErrorKind::Timeout,
                    "command provider process did not exit after its terminal",
                ),
            )
            .await;
            return;
        }
        WaitPhase::StderrLimit => {
            terminate_group(&mut child, limits.cleanup_timeout).await;
            finish_drain(&mut stderr_drain, limits.cleanup_timeout).await;
            send_error(
                &events,
                provider_error(
                    ProviderErrorKind::MalformedStream,
                    "command provider stderr exceeded its configured bound",
                ),
            )
            .await;
            return;
        }
    };

    // `wait` reaps the leader, but a grandchild can outlive it and is not
    // waitable by this process. Signal the retained group/job identity once
    // more before publishing the terminal so normal completion cannot orphan
    // adapter-owned work.
    terminate_group(&mut child, limits.cleanup_timeout).await;
    finish_drain(&mut stderr_drain, limits.cleanup_timeout).await;
    match terminal {
        ProviderStreamEvent::Error { error } => send_error(&events, error).await,
        terminal if status.success() => {
            let _ = events.send(terminal).await;
        }
        _ => {
            send_error(
                &events,
                provider_error(
                    ProviderErrorKind::Server,
                    "command provider process exited unsuccessfully",
                ),
            )
            .await;
        }
    }
}

enum WaitPhase {
    Exited(std::io::Result<ExitStatus>),
    Cancelled,
    TimedOut,
    CleanupTimedOut,
    StderrLimit,
    Dropped,
}

async fn decode_stdout(
    mut stdout: ChildStdout,
    mut decoder: Box<dyn CommandOutputDecoder>,
    limits: &CommandLimits,
    events: mpsc::Sender<ProviderStreamEvent>,
) -> Result<ProviderStreamEvent, OutputFailure> {
    let mut chunk = [0_u8; 8192];
    let mut frame = Vec::new();
    let mut total = 0_usize;
    let mut terminal = None;

    loop {
        let read = stdout.read(&mut chunk).await.map_err(|_| {
            OutputFailure::Provider(provider_error(
                ProviderErrorKind::Network,
                "command provider stdout read failed",
            ))
        })?;
        if read == 0 {
            break;
        }
        total = total.saturating_add(read);
        if total > limits.max_stdout_bytes {
            return Err(OutputFailure::Provider(provider_error(
                ProviderErrorKind::MalformedStream,
                "command provider stdout exceeded its aggregate bound",
            )));
        }
        let mut start = 0;
        for (offset, byte) in chunk[..read].iter().enumerate() {
            if *byte == b'\n' {
                append_frame(&mut frame, &chunk[start..offset], limits.max_frame_bytes)?;
                if frame.last() == Some(&b'\r') {
                    frame.pop();
                }
                decode_frame(&mut *decoder, &frame, &events, &mut terminal).await?;
                frame.clear();
                start = offset + 1;
            }
        }
        append_frame(&mut frame, &chunk[start..read], limits.max_frame_bytes)?;
    }

    if !frame.is_empty() {
        if frame.last() == Some(&b'\r') {
            frame.pop();
        }
        decode_frame(&mut *decoder, &frame, &events, &mut terminal).await?;
    }
    let final_events = decoder.finish().map_err(OutputFailure::Provider)?;
    accept_events(final_events, &events, &mut terminal).await?;
    terminal.ok_or_else(|| {
        OutputFailure::Provider(provider_error(
            ProviderErrorKind::MalformedStream,
            "command provider stdout ended without a terminal event",
        ))
    })
}

fn append_frame(
    frame: &mut Vec<u8>,
    bytes: &[u8],
    max_frame_bytes: usize,
) -> Result<(), OutputFailure> {
    if frame.len().saturating_add(bytes.len()) > max_frame_bytes {
        return Err(OutputFailure::Provider(provider_error(
            ProviderErrorKind::MalformedStream,
            "command provider stdout frame exceeded its bound",
        )));
    }
    frame.extend_from_slice(bytes);
    Ok(())
}

async fn decode_frame(
    decoder: &mut dyn CommandOutputDecoder,
    frame: &[u8],
    events: &mpsc::Sender<ProviderStreamEvent>,
    terminal: &mut Option<ProviderStreamEvent>,
) -> Result<(), OutputFailure> {
    let decoded = decoder
        .decode_frame(frame)
        .map_err(OutputFailure::Provider)?;
    accept_events(decoded, events, terminal).await
}

async fn accept_events(
    decoded: Vec<ProviderStreamEvent>,
    events: &mpsc::Sender<ProviderStreamEvent>,
    terminal: &mut Option<ProviderStreamEvent>,
) -> Result<(), OutputFailure> {
    for event in decoded {
        if terminal.is_some() {
            return Err(OutputFailure::Provider(provider_error(
                ProviderErrorKind::MalformedStream,
                "command provider emitted data after its terminal",
            )));
        }
        if is_terminal(&event) {
            *terminal = Some(event);
        } else {
            events
                .send(event)
                .await
                .map_err(|_| OutputFailure::ReceiverClosed)?;
        }
    }
    Ok(())
}

fn is_terminal(event: &ProviderStreamEvent) -> bool {
    matches!(
        event,
        ProviderStreamEvent::Finish { .. } | ProviderStreamEvent::Error { .. }
    )
}

async fn drain_stderr(mut stderr: ChildStderr, bound: usize, exceeded: Arc<Notify>) {
    let mut chunk = [0_u8; 8192];
    let mut total = 0_usize;
    let mut notified = false;
    loop {
        match stderr.read(&mut chunk).await {
            Ok(0) | Err(_) => return,
            Ok(read) => {
                total = total.saturating_add(read);
                if total > bound && !notified {
                    notified = true;
                    exceeded.notify_one();
                }
            }
        }
    }
}

async fn finish_drain(task: &mut tokio::task::JoinHandle<()>, timeout: Duration) {
    if tokio::time::timeout(timeout, &mut *task).await.is_err() {
        task.abort();
    }
}

async fn terminate_group(child: &mut AsyncGroupChild, timeout: Duration) {
    // Signal the group even when the leader has already exited: descendants
    // may still be alive under the original process-group/job identity.
    let _ = child.start_kill();
    let _ = tokio::time::timeout(timeout, child.wait()).await;
}

async fn send_error(events: &mpsc::Sender<ProviderStreamEvent>, error: ProviderError) {
    let _ = events.send(ProviderStreamEvent::Error { error }).await;
}

async fn wait_for_deadline(deadline: agent_runtime_core::clock::Deadline) {
    match deadline.remaining_millis(&SystemClock) {
        Some(remaining) => tokio::time::sleep(Duration::from_millis(remaining)).await,
        None => std::future::pending::<()>().await,
    }
}

async fn run_probe(
    process: Arc<CommandProcessConfig>,
    probe: CommandProbe,
) -> Result<Vec<u8>, CommandPreflightError> {
    let mut command = build_command(&process, &probe.args, false);
    let mut group = command.group();
    group.kill_on_drop(true);
    let mut child = group
        .spawn()
        .map_err(|_| CommandPreflightError::Unavailable)?;
    let stdout = child
        .inner()
        .stdout
        .take()
        .ok_or(CommandPreflightError::Unavailable)?;
    let stderr = child
        .inner()
        .stderr
        .take()
        .ok_or(CommandPreflightError::Unavailable)?;
    let stderr_limit = Arc::new(Notify::new());
    let mut stderr_drain = tokio::spawn(drain_stderr(
        stderr,
        process.limits.max_stderr_bytes,
        stderr_limit.clone(),
    ));
    let started = tokio::time::Instant::now();
    let output = collect_bounded(stdout, process.limits.max_probe_stdout_bytes);
    tokio::pin!(output);
    let timer = tokio::time::sleep(process.limits.probe_timeout);
    tokio::pin!(timer);

    let stdout = tokio::select! {
        result = &mut output => match result {
            Ok(stdout) => stdout,
            Err(error) => {
                terminate_group(&mut child, process.limits.cleanup_timeout).await;
                finish_drain(&mut stderr_drain, process.limits.cleanup_timeout).await;
                return Err(error);
            }
        },
        _ = &mut timer => {
            terminate_group(&mut child, process.limits.cleanup_timeout).await;
            finish_drain(&mut stderr_drain, process.limits.cleanup_timeout).await;
            return Err(CommandPreflightError::Timeout);
        }
        _ = stderr_limit.notified() => {
            terminate_group(&mut child, process.limits.cleanup_timeout).await;
            finish_drain(&mut stderr_drain, process.limits.cleanup_timeout).await;
            return Err(CommandPreflightError::OutputLimit);
        }
    };

    let remaining = process
        .limits
        .probe_timeout
        .saturating_sub(started.elapsed());
    let probe_wait = {
        let wait = child.wait();
        tokio::pin!(wait);
        let remaining = tokio::time::sleep(remaining);
        tokio::pin!(remaining);
        tokio::select! {
            status = &mut wait => ProbeWait::Exited(status),
            _ = &mut remaining => ProbeWait::TimedOut,
            _ = stderr_limit.notified() => ProbeWait::StderrLimit,
        }
    };
    let status = match probe_wait {
        ProbeWait::Exited(Ok(status)) => status,
        ProbeWait::Exited(Err(_)) => {
            terminate_group(&mut child, process.limits.cleanup_timeout).await;
            finish_drain(&mut stderr_drain, process.limits.cleanup_timeout).await;
            return Err(CommandPreflightError::Unavailable);
        }
        ProbeWait::TimedOut => {
            terminate_group(&mut child, process.limits.cleanup_timeout).await;
            finish_drain(&mut stderr_drain, process.limits.cleanup_timeout).await;
            return Err(CommandPreflightError::Timeout);
        }
        ProbeWait::StderrLimit => {
            terminate_group(&mut child, process.limits.cleanup_timeout).await;
            finish_drain(&mut stderr_drain, process.limits.cleanup_timeout).await;
            return Err(CommandPreflightError::OutputLimit);
        }
    };
    terminate_group(&mut child, process.limits.cleanup_timeout).await;
    finish_drain(&mut stderr_drain, process.limits.cleanup_timeout).await;
    if !status.success() {
        return Err(CommandPreflightError::UnsuccessfulExit);
    }
    Ok(stdout)
}

enum ProbeWait {
    Exited(std::io::Result<ExitStatus>),
    TimedOut,
    StderrLimit,
}

async fn collect_bounded<R: AsyncRead + Unpin>(
    mut reader: R,
    max_bytes: usize,
) -> Result<Vec<u8>, CommandPreflightError> {
    let mut output = Vec::new();
    let mut chunk = [0_u8; 8192];
    loop {
        let read = reader
            .read(&mut chunk)
            .await
            .map_err(|_| CommandPreflightError::Unavailable)?;
        if read == 0 {
            return Ok(output);
        }
        if output.len().saturating_add(read) > max_bytes {
            return Err(CommandPreflightError::OutputLimit);
        }
        output.extend_from_slice(&chunk[..read]);
    }
}

fn build_command(
    process: &CommandProcessConfig,
    attempt_args: &[String],
    piped_stdin: bool,
) -> Command {
    let mut command = Command::new(&process.executable);
    command
        .args(&process.fixed_args)
        .args(attempt_args)
        .current_dir(&process.cwd)
        .env_clear()
        .envs(
            process
                .env
                .iter()
                .map(|(name, value)| (name.as_str(), value.as_str())),
        )
        .stdin(if piped_stdin {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    command
}

fn validate_attempt(attempt: &CommandAttempt, limits: &CommandLimits) -> Result<(), ProviderError> {
    validate_args(&attempt.args).map_err(|_| {
        provider_error(
            ProviderErrorKind::BadRequest,
            "command adapter produced invalid arguments",
        )
    })?;
    if attempt.stdin.len() > limits.max_stdin_bytes {
        return Err(provider_error(
            ProviderErrorKind::BadRequest,
            "command adapter stdin exceeded its configured bound",
        ));
    }
    Ok(())
}

fn validate_models(models: &[ModelDescriptor]) -> Result<(), CommandProviderBuildError> {
    if models.is_empty() {
        return Err(CommandProviderBuildError::NoModels);
    }
    let mut seen = BTreeSet::new();
    for descriptor in models {
        let id = descriptor.id.as_str();
        if id.is_empty() || id.len() > 256 || id.chars().any(char::is_control) {
            return Err(CommandProviderBuildError::InvalidModelId);
        }
        if !seen.insert(id.to_owned()) {
            return Err(CommandProviderBuildError::DuplicateModel {
                model: id.to_owned(),
            });
        }
        if descriptor.capabilities.validate_cache_contract().is_err() {
            return Err(CommandProviderBuildError::InvalidCapabilities {
                model: id.to_owned(),
            });
        }
    }
    Ok(())
}

fn canonical_file(path: PathBuf, field: &'static str) -> Result<PathBuf, CommandConfigError> {
    if !path.is_absolute() {
        return Err(CommandConfigError::RelativePath { field });
    }
    let path =
        std::fs::canonicalize(path).map_err(|_| CommandConfigError::UnresolvablePath { field })?;
    if !path.is_file() {
        return Err(CommandConfigError::ExecutableNotFile);
    }
    Ok(path)
}

fn canonical_directory(path: PathBuf, field: &'static str) -> Result<PathBuf, CommandConfigError> {
    if !path.is_absolute() {
        return Err(CommandConfigError::RelativePath { field });
    }
    let path =
        std::fs::canonicalize(path).map_err(|_| CommandConfigError::UnresolvablePath { field })?;
    if !path.is_dir() {
        return Err(CommandConfigError::CwdNotDirectory);
    }
    Ok(path)
}

#[cfg(unix)]
fn validate_executable(path: &Path) -> Result<(), CommandConfigError> {
    use std::os::unix::fs::PermissionsExt;
    let mode = std::fs::metadata(path)
        .map_err(|_| CommandConfigError::ExecutableNotFile)?
        .permissions()
        .mode();
    if mode & 0o111 == 0 {
        return Err(CommandConfigError::ExecutableNotExecutable);
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_executable(_path: &Path) -> Result<(), CommandConfigError> {
    Ok(())
}

fn validate_args(args: &[String]) -> Result<(), CommandConfigError> {
    if args.len() > MAX_ARGUMENTS {
        return Err(CommandConfigError::TooManyArguments);
    }
    if args
        .iter()
        .any(|argument| argument.len() > MAX_ARGUMENT_BYTES || argument.contains('\0'))
    {
        return Err(CommandConfigError::InvalidArgument);
    }
    Ok(())
}

fn validate_environment_entry(name: &str, value: &str) -> Result<(), CommandConfigError> {
    if name.is_empty() || name.contains('=') || name.contains('\0') {
        return Err(CommandConfigError::InvalidEnvironmentName);
    }
    if value.contains('\0') {
        return Err(CommandConfigError::InvalidEnvironmentValue);
    }
    Ok(())
}

fn environment_bytes(env: &BTreeMap<String, Zeroizing<String>>) -> usize {
    env.iter()
        .map(|(name, value)| name.len().saturating_add(value.len()))
        .sum()
}

fn validate_limit(
    field: &'static str,
    value: usize,
    maximum: usize,
) -> Result<(), CommandConfigError> {
    if value == 0 || value > maximum {
        return Err(CommandConfigError::InvalidLimit { field });
    }
    Ok(())
}

fn validate_duration(
    field: &'static str,
    value: Duration,
    maximum_ms: u64,
) -> Result<(), CommandConfigError> {
    if value.is_zero() || value > Duration::from_millis(maximum_ms) {
        return Err(CommandConfigError::InvalidLimit { field });
    }
    Ok(())
}

fn validate_probe_text(
    value: Option<&str>,
    field: &'static str,
    maximum: usize,
) -> Result<(), CommandPreflightError> {
    if value.is_some_and(|value| {
        value.is_empty() || value.len() > maximum || value.chars().any(char::is_control)
    }) {
        return Err(CommandPreflightError::InvalidMetadata { field });
    }
    Ok(())
}

fn provider_error(kind: ProviderErrorKind, message: &'static str) -> ProviderError {
    ProviderError::new(kind, message)
}
