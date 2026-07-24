# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this project uses
pre-1.0 semantic versioning: while the major version is `0`, minor releases may
contain breaking changes and are coordinated with consumer proposals.

## [Unreleased]

### Added
- Rust 2024 workspace with `agent-runtime-core`, `agent-runtime`, and
  `agent-runtime-testkit` (minimum supported Rust version 1.86).
- Host-neutral core contracts: neutral IDs, messages/content, structured
  errors, cancellation, deadlines, redaction-safe metadata, versioned events,
  and disjoint usage counters with per-counter provenance.
- Host adapter traits: `Provider`, `Tool`, `ApprovalPolicy`, `Workspace`,
  `SessionStore`, `SecretStore`, `EventObserver`, and `Clock`.
- Provider runtime: capability/model descriptors, normalized requests, typed
  streaming events, a deterministic fake adapter, a configurable
  OpenAI-compatible adapter over an injectable HTTP transport, and an
  attempt-recording retry wrapper.
- Tool + agent execution: deterministic tool registry with name-conflict
  validation, fail-closed approval and workspace enforcement, side-effect-aware
  scheduling, and one canonical direct provider/tool loop with configured
  limits.
- Embeddable runtime facade: `RuntimeBuilder`, `Runtime`, and `SessionHandle`
  with injected host services, versioned commands/events, concurrent
  subscribers, cancellation propagation, and bounded shutdown.
- `agent-runtime-testkit`: fake clock, event recorder, temporary workspace, and
  reusable conformance suites (provider, tool, runtime, cancellation,
  event-schema, shutdown) plus neutral consumer adapter fixtures.

### Provenance
- Reusable provider, agent-loop, and tool mechanisms were adapted from the Nyx
  project. See `PROVENANCE.md` for the donor revision and path mappings.
