# Validation

Implemented from Smith's pinned runtime revision
`70c4f96ce8060c00d6176bc0724ee6d252b1426d` in the
`fix/malformed-tool-recovery` worktree.

- Runtime conformance: 95 tests passed, including nine correction regressions.
- Strict public assembler: three tests passed.
- Existing tool-schema correction: one integration test passed.
- Existing provider cache admission: three integration tests passed.
- Smith OpenAI-compatible adapter: 24 tests passed.
- Smith built-in tools through the runtime: 13 tests passed. The new shell
  regression also passed for both tool-call and output-length finishes, using
  an unfinished argument string larger than 85 KB.
- Local Smith binary built successfully; `target/debug/smith --version`
  reports `smith 0.7.7`. The development linker emitted debug-object
  and unwind-table warnings; the binary starts successfully.
- The optimized release binary was built and installed locally as Smith 0.7.7;
  its recovery message and installed SHA-256 were verified.
- Formatting, scoped diff whitespace checks, and strict Clippy passed for the
  modified runtime/testkit and Smith runtime/tools targets. Runtime Clippy uses
  the `external-agent` feature that Smith enables; the pinned baseline's default
  all-targets build has an unrelated cfg-dependent `unused_mut` test warning.

The regressions verify bounded attempts, correction replacement, absence of raw
argument canaries from retries/history, failed usage attribution, stable request
and distinct attempt IDs, discarded output before retry, no sibling execution,
terminal cancellation/filtering/errors, output-limit recovery and exhaustion,
input admission, cancellation/deadlines at the correction boundary, and cache
identity/baseline retirement.

## Smith integration

Development validation used Smith's git-ignored local Cargo override. The
consumer can adopt the pushed fix through its exact Git revision, replacing
the local override without a runtime version upgrade.

This is a compatibility fix for Smith's pinned 0.7 runtime. Runtime main has
advanced to 0.9; adopting that unrelated upgrade is outside this patch. Local
path overrides must not be committed as release dependencies.
