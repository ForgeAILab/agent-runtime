## ADDED Requirements

### Requirement: Runtime failures retain neutral classification

RuntimeError SHALL carry a defaultable FailureClass that distinguishes known
deterministic rejection and policy denial, transient provider failure, rate
limiting, quota exhaustion, authentication, context overflow, state conflict,
host component failure, limits, cancellation, and internal failure. Classified
errors MUST distinguish originating pre-provider, provider, tool, or unknown
stages without inferring cost, history commitment, or retry admission.

#### Scenario: Planner rejects input before provider execution

- **WHEN** the planner reports a typed model-input-budget overflow
- **THEN** the existing Error event retains a pre-provider ContextOverflow class and the known budget counts
- **AND** no provider request is dispatched and the existing terminal outcome is preserved

#### Scenario: Policy denial is deterministic

- **WHEN** an approval, workspace, or LCM-view denial is exposed as RuntimeError
- **THEN** it carries PolicyDenied with its known stage
- **AND** neither the class nor its stage grants retry or lookup authority

#### Scenario: Harness boundary preserves classified failure

- **WHEN** a contributor or LCM coordinator returns a classified runtime failure
- **THEN** the host receives its class and safe origin metadata through the existing error channel while retaining that path's historical coarse kind/retryability projection
- **AND** no classification is reconstructed from message substrings or raw store text

#### Scenario: Legacy error has no evidence

- **WHEN** an error lacks classification or a generic constructor has no typed origin evidence
- **THEN** classification or stage remains unknown
- **AND** unknown values do not imply that retry will succeed

### Requirement: Runtime provider conversion preserves recovery evidence

Conversion from ProviderError SHALL retain retry_after_ms,
limit_resets_at_ms, and the fixed credential_recovery classification as
optional evidence alongside the existing fields. It MUST preserve absent and
zero values and MUST NOT treat provider timing as an admitted retry decision.

#### Scenario: Rate-limit error includes a retry hint

- **WHEN** a provider error with retry_after_ms is converted
- **THEN** the runtime error preserves that exact duration and RateLimited classification
- **AND** ProviderAttemptFinished.retry_delay_ms continues to describe only the existing loop's actual admission

#### Scenario: Exhausted quota reports reset time

- **WHEN** a LimitExhausted error reports limit_resets_at_ms
- **THEN** conversion retains the absolute Unix-millisecond timestamp and QuotaExhausted classification
- **AND** it preserves the original retryable flag without treating a reset as ordinary backoff permission

#### Scenario: Authentication recovery remains fenced

- **WHEN** a classified authentication error is converted
- **THEN** its fixed recovery evidence remains readable without a credential lease or secret
- **AND** the existing credential replay fence remains the only runtime recovery admission path
