# Verification

Verified on 2026-10-23 against the v0.7.0 Smith compatibility baseline.

- The five-package runtime/core/provider/obs/testkit suite passed: 1,670
  tests, one existing ignored doctest. Both `external-agent-bridge` and
  `command-provider` features were enabled.
- The final `model_fallback` target passed all 21 tests, including the added
  regression that skips a text-only candidate while preserving image input.
- Runtime unit tests and the Smith consumer-conformance target were rerun
  after the image-compatibility fix.
- Clippy for all five packages and all targets passed with warnings denied.
- Formatting, whitespace checks, and strict change validation passed.

Commands:

```sh
cargo test -p agent-runtime -p agent-runtime-core -p agent-runtime-provider -p agent-runtime-obs -p agent-runtime-testkit --features agent-runtime/external-agent-bridge,agent-runtime/command-provider --quiet
cargo test -p agent-runtime --test model_fallback --quiet
cargo test -p agent-runtime --lib --quiet
cargo test -p agent-runtime-testkit --test consumer_smith --quiet
cargo clippy -p agent-runtime -p agent-runtime-core -p agent-runtime-provider -p agent-runtime-obs -p agent-runtime-testkit --all-targets --features agent-runtime/external-agent-bridge,agent-runtime/command-provider -- -D warnings
cargo fmt --all --check
git diff --check
python3 ~/.codex/skills/spec-toolkit/scripts/spec_toolkit.py validate add-model-fallback-2026-10-23 --type change --strict
```

The route tests cover opening/empty/metadata-only streams, retries and
backoff under one startup budget, per-candidate budgets, progress disarming,
late-output retirement, unsafe hosted effects, cancellation and turn
deadlines, local preparation, finite failure summaries, history/tool
continuation, smaller windows, images, host interceptors, and checkpoint
recovery. A unit regression retains the session cache registry across route
changes. Frozen transition-table-4 tests remain unchanged; fallback turns
explicitly select table 5, and unknown revisions remain rejected.

No live-provider requests were needed.
