//! Compaction conformance: the invariants that keep a compacted context
//! trustworthy rather than merely smaller.
//!
//! [`validate_compacted`] is the structural gate every compaction result
//! passes through, whether produced by [`StructuralCompactor`] or a host's own
//! [`agent_runtime::context::Compactor`] implementation — so most of these
//! assertions are written directly against it: a host authoring a custom
//! compactor can run them against its own candidate outputs the same way this
//! suite runs them against the shipped one. Required content is preserved by
//! construction (every stage only ever touches optional fragments) and the
//! gate rejects a candidate anyway if that is somehow violated. A tool call
//! and its result must both survive or both be removed together — never one
//! without the other, since an orphaned half is unusable to a provider. A
//! `Sensitivity::Secret` fragment must never be covered by host-supplied
//! semantic-summary provenance. The shipped structural compactor never
//! creates a summary itself. Repeating compaction once no safe structural
//! operation remains must change nothing.

use agent_runtime::context::{
    CharRatioSizer, CompactionErrorKind, CompactionOutcome, CompactionPolicy, ContextFragment,
    FragmentContent, FragmentId, FragmentKind, FragmentSource, RequestSizer, Sensitivity,
    StructuralCompactor, SummaryProvenance, validate_compacted,
};
use agent_runtime::core::ids::ToolCallId;
use agent_runtime::registry::RegistryRevision;

/// Builds a required text fragment for suite fixtures.
pub fn conformance_required_fragment(id: &str, kind: FragmentKind, body: &str) -> ContextFragment {
    ContextFragment::new(
        id,
        kind,
        FragmentSource::Host,
        RegistryRevision::from_content(body),
        FragmentContent::Text(body.to_owned()),
    )
}

/// Builds an optional text fragment at `priority`, for suite fixtures.
pub fn conformance_optional_fragment(
    id: &str,
    kind: FragmentKind,
    priority: i32,
    body: &str,
) -> ContextFragment {
    conformance_required_fragment(id, kind, body)
        .optional()
        .with_priority(priority)
}

/// Asserts `validate_compacted` rejects a candidate that dropped a required
/// fragment, naming the fragment that went missing.
pub fn assert_required_content_dropped_is_rejected(
    original: &[ContextFragment],
    candidate: &[ContextFragment],
) {
    let outcome = CompactionOutcome::default();
    let err = validate_compacted(original, candidate, &outcome)
        .expect_err("a candidate missing required content must be rejected");
    assert_eq!(err.kind, CompactionErrorKind::RequiredContentDropped);
    assert!(
        original
            .iter()
            .any(|f| Some(&f.id) == err.fragment.as_ref() && f.is_required()),
        "the rejection must name a fragment that was actually required"
    );
}

/// Asserts `validate_compacted` accepts a candidate that preserves every
/// required fragment, keeps pairing complete, and summarizes no secret.
pub fn assert_preserved_required_content_is_accepted(
    original: &[ContextFragment],
    candidate: &[ContextFragment],
    outcome: &CompactionOutcome,
) {
    assert!(
        validate_compacted(original, candidate, outcome).is_ok(),
        "a candidate keeping every required fragment, valid pairing, and no secret summary must be accepted"
    );
}

/// Asserts a candidate leaving a tool call or result unmatched is rejected —
/// a call and its result must both survive or both be removed, never one
/// without the other.
pub fn assert_broken_pairing_is_rejected(
    original: &[ContextFragment],
    candidate: &[ContextFragment],
) {
    let outcome = CompactionOutcome::default();
    let err = validate_compacted(original, candidate, &outcome)
        .expect_err("a candidate leaving a tool call/result pair incomplete must be rejected");
    assert_eq!(err.kind, CompactionErrorKind::InvalidPairing);
}

/// Asserts an outcome claiming to have summarized a `Sensitivity::Secret`
/// fragment is rejected, regardless of what the candidate fragment list
/// itself contains.
pub fn assert_secret_summarization_is_rejected(
    original: &[ContextFragment],
    candidate: &[ContextFragment],
    outcome: &CompactionOutcome,
) {
    let err = validate_compacted(original, candidate, outcome)
        .expect_err("an outcome claiming to summarize a secret fragment must be rejected");
    assert_eq!(err.kind, CompactionErrorKind::SecretSummarized);
}

/// Asserts every summary a compaction pass created records exactly the
/// fragment ids it replaced: the covered ids existed in the original set and
/// no longer remain in the result.
pub fn assert_summary_records_exactly_what_it_replaced(
    original: &[ContextFragment],
    result: &[ContextFragment],
    outcome: &CompactionOutcome,
) {
    assert!(
        !outcome.summarized.is_empty(),
        "the fixture must actually produce a summary for this assertion to be meaningful"
    );
    for summary in &outcome.summarized {
        assert!(
            result.iter().any(|f| f.id == summary.summary),
            "the recorded summary fragment `{}` must actually be present in the result",
            summary.summary
        );
        assert!(
            !summary.covers.is_empty(),
            "a summary must record at least one covered fragment"
        );
        for covered in &summary.covers {
            assert!(
                original.iter().any(|f| &f.id == covered),
                "a summary must only claim to cover fragments that existed originally"
            );
            assert!(
                !result.iter().any(|f| &f.id == covered),
                "a fragment a summary claims to cover must not also still be present in the result"
            );
        }
    }
}

/// Asserts compacting `fragments` preserves every fragment it marked
/// [`agent_runtime::context::Requirement::Required`].
pub fn assert_compaction_preserves_required_content(
    compactor: &StructuralCompactor,
    sizer: &dyn RequestSizer,
    fragments: &[ContextFragment],
) {
    let compacted = compactor
        .maybe_compact(fragments, sizer)
        .expect("compaction must succeed for this assertion to be meaningful");
    for fragment in fragments.iter().filter(|f| f.is_required()) {
        assert!(
            compacted.fragments.iter().any(|r| r.id == fragment.id),
            "required fragment `{}` must survive compaction",
            fragment.id
        );
    }
    assert!(validate_compacted(fragments, &compacted.fragments, &compacted.outcome).is_ok());
}

/// Asserts compacting an already structurally compacted fragment set again
/// changes nothing: an empty outcome and an identical fragment list.
pub fn assert_repeated_structural_compaction_is_a_noop(
    compactor: &StructuralCompactor,
    sizer: &dyn RequestSizer,
    fragments: &[ContextFragment],
) {
    let once = compactor
        .maybe_compact(fragments, sizer)
        .expect("first compaction must succeed for this assertion to be meaningful");
    assert!(
        !once.outcome.is_noop(),
        "the fixture must actually need compaction for a second pass to prove anything"
    );
    let twice = compactor
        .maybe_compact(&once.fragments, sizer)
        .expect("second compaction must succeed");
    assert!(
        twice.outcome.is_noop(),
        "compacting an already-compacted fragment set again must change nothing"
    );
    assert_eq!(
        once.fragments, twice.fragments,
        "a no-op compaction pass must return the same fragments"
    );
}

/// Runs every compaction assertion over a standard fixture set: a required
/// system instruction, a `Sensitivity::Secret` optional history fragment that
/// must survive as-is, and enough old optional history to exercise structural
/// bounding.
pub fn assert_compaction_conformance() {
    // `validate_compacted` gate: dropped required content, broken pairing,
    // and secret summarization must each be rejected; a clean candidate must
    // be accepted.
    let sys = conformance_required_fragment("sys", FragmentKind::SystemInstruction, "be helpful");
    assert_required_content_dropped_is_rejected(std::slice::from_ref(&sys), &[]);
    assert_preserved_required_content_is_accepted(
        std::slice::from_ref(&sys),
        std::slice::from_ref(&sys),
        &CompactionOutcome::default(),
    );

    let call = conformance_optional_fragment("call", FragmentKind::History, 1, "call the tool")
        .paired_with(ToolCallId::new("conformance-call-1"));
    let result =
        conformance_optional_fragment("result", FragmentKind::ToolResult, 1, "tool result")
            .paired_with(ToolCallId::new("conformance-call-1"));
    assert_broken_pairing_is_rejected(&[call.clone(), result.clone()], std::slice::from_ref(&call));

    let secret = conformance_optional_fragment("secret", FragmentKind::History, 1, "shh")
        .with_sensitivity(Sensitivity::Secret);
    let claimed_outcome = CompactionOutcome {
        summarized: vec![SummaryProvenance {
            summary: FragmentId::new("summary-1"),
            covers: vec![FragmentId::new("secret")],
            policy_revision: RegistryRevision::new("conformance-1"),
            source_artifact: None,
            model_purpose: None,
            model_revision: None,
            sensitivity: None,
        }],
        ..CompactionOutcome::default()
    };
    assert_secret_summarization_is_rejected(&[secret], &[], &claimed_outcome);

    // `StructuralCompactor`: required content survives, old optional history
    // is bounded without a fabricated semantic summary, and repeating the
    // exhausted structural pass is a no-op.
    let sizer = CharRatioSizer::default();
    let policy = CompactionPolicy::new(RegistryRevision::new("conformance-compaction-1"), 200, 80);
    let compactor = StructuralCompactor::new(policy);

    let old_a = conformance_optional_fragment("old-a", FragmentKind::History, 1, &"x".repeat(400));
    let old_b = conformance_optional_fragment("old-b", FragmentKind::History, 1, &"x".repeat(400));
    let recent = conformance_optional_fragment("recent", FragmentKind::History, 5, "recent turn");
    let fragments = vec![sys, old_a, old_b, recent];

    assert_compaction_preserves_required_content(&compactor, &sizer, &fragments);
    let compacted = compactor.maybe_compact(&fragments, &sizer).unwrap();
    assert!(
        compacted.outcome.summarized.is_empty(),
        "structural compaction must never claim to summarize meaning"
    );
    assert!(
        !compacted
            .fragments
            .iter()
            .any(|fragment| fragment.kind == FragmentKind::Summary),
        "structural compaction must not fabricate summary fragments"
    );
    assert_repeated_structural_compaction_is_a_noop(&compactor, &sizer, &fragments);
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_runtime::core::content::{ContentPart, Message, ToolCall, ToolResultBlock};

    #[test]
    fn structural_compactor_and_validate_compacted_satisfy_the_conformance_suite() {
        assert_compaction_conformance();
    }

    /// A call and result declared at different priorities remain present
    /// together because structural compaction never folds either group into
    /// a fabricated summary.
    #[test]
    fn structural_compaction_cannot_split_a_pair_across_priorities() {
        let call_id = ToolCallId::new("conformance-split-1");
        let call_message = Message::assistant(vec![ContentPart::ToolCall(ToolCall {
            id: call_id.clone(),
            name: "search".into(),
            arguments: serde_json::json!({}),
        })]);
        let call_fragment = ContextFragment::new(
            "call",
            FragmentKind::History,
            FragmentSource::History,
            RegistryRevision::new("h1"),
            FragmentContent::Message(call_message),
        )
        .optional()
        .with_priority(1)
        .paired_with(call_id.clone());

        let result_message = Message::tool_result(ToolResultBlock {
            call_id: call_id.clone(),
            name: "search".into(),
            content: vec![ContentPart::text("done")],
            is_error: false,
        });
        let result_fragment = ContextFragment::new(
            "result",
            FragmentKind::ToolResult,
            FragmentSource::Tool,
            RegistryRevision::new("r1"),
            FragmentContent::Message(result_message),
        )
        .optional()
        .with_priority(2)
        .paired_with(call_id);

        let padding =
            conformance_optional_fragment("padding", FragmentKind::History, 1, &"y".repeat(400));

        let sizer = CharRatioSizer::default();
        let policy = CompactionPolicy::new(RegistryRevision::new("conformance-split-1"), 50, 35);
        let compactor = StructuralCompactor::new(policy);
        let fragments = vec![call_fragment, result_fragment, padding];

        let compacted = compactor.maybe_compact(&fragments, &sizer).unwrap();
        assert!(
            compacted
                .fragments
                .iter()
                .any(|fragment| fragment.id == FragmentId::new("call"))
        );
        assert!(
            compacted
                .fragments
                .iter()
                .any(|fragment| fragment.id == FragmentId::new("result"))
        );
        assert!(compacted.outcome.summarized.is_empty());
        assert!(validate_compacted(&fragments, &compacted.fragments, &compacted.outcome).is_ok());
    }

    #[test]
    fn structural_compactor_never_folds_secret_history_into_a_summary() {
        let sizer = CharRatioSizer::default();
        let policy = CompactionPolicy::new(RegistryRevision::new("conformance-secret-1"), 100, 40);
        let compactor = StructuralCompactor::new(policy);

        let secret =
            conformance_optional_fragment("secret", FragmentKind::History, 1, &"x".repeat(320))
                .with_sensitivity(Sensitivity::Secret);
        let other =
            conformance_optional_fragment("other", FragmentKind::History, 2, &"x".repeat(160));
        let fragments = vec![secret.clone(), other];

        let compacted = compactor.maybe_compact(&fragments, &sizer).unwrap();
        assert!(compacted.fragments.iter().any(|f| f.id == secret.id));
        assert!(compacted.outcome.summarized.is_empty());
        assert!(validate_compacted(&fragments, &compacted.fragments, &compacted.outcome).is_ok());
    }

    #[test]
    #[should_panic(expected = "must be accepted")]
    fn required_content_conformance_is_not_trivially_satisfied() {
        // A candidate that dropped required content must not be accepted:
        // proves `assert_preserved_required_content_is_accepted` is a real
        // check, not one that passes no matter what.
        let sys =
            conformance_required_fragment("sys", FragmentKind::SystemInstruction, "be helpful");
        assert_preserved_required_content_is_accepted(&[sys], &[], &CompactionOutcome::default());
    }

    #[test]
    fn structural_compactor_never_emits_a_summary_fragment_or_provenance() {
        let sizer = CharRatioSizer::default();
        let policy = CompactionPolicy::new(RegistryRevision::new("conformance-cache-1"), 200, 80);
        let compactor = StructuralCompactor::new(policy);
        let old_a =
            conformance_optional_fragment("old-a", FragmentKind::History, 1, &"x".repeat(400));
        let old_b =
            conformance_optional_fragment("old-b", FragmentKind::History, 1, &"x".repeat(400));
        let fragments = vec![old_a, old_b];

        let compacted = compactor.maybe_compact(&fragments, &sizer).unwrap();
        assert!(compacted.outcome.summarized.is_empty());
        assert!(
            !compacted
                .fragments
                .iter()
                .any(|fragment| fragment.kind == FragmentKind::Summary)
        );
    }
}
