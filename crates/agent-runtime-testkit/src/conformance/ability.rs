//! Ability descriptor and activation conformance: the invariants that keep
//! discovery cheap and activation safe no matter what a host registers.
//!
//! Two of these matter most. **Discovery must cost nothing**: a descriptor is
//! bounded metadata, and indexing, searching, or inspecting it must never
//! reach the executable content (or the I/O) behind it — otherwise a large
//! catalog is only cheap to list, not to search. **Activation must authorize
//! before it acts**: [`activate`] runs the policy to completion first, so a
//! denied, conflicting, unready, or stale-revision attempt can never cause a
//! side effect, and a readiness failure may only ever name the credential or
//! configuration *names* that are missing — never a value, since this crate
//! never sees one to begin with.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use agent_runtime::ability::activation::{
    Activated, ActivationContext, ActivationError, ActivationHandle, ActivationPolicy,
    FailClosedPolicy, activate,
};
use agent_runtime::ability::descriptor::{AbilityDescriptor, ReadinessRequirement};
use agent_runtime::ability::{Ability, AbilityKind, Named};
use agent_runtime::registry::{EntryProvenance, RegistryId, RegistryRevision, RegistrySource};

/// Records whether a paired handle's [`ActivationHandle::activate`] was ever
/// actually called, so a descriptor-only operation can be proven not to have
/// touched it.
#[derive(Debug, Default)]
pub struct MaterializationTripwire(AtomicBool);

impl MaterializationTripwire {
    /// A tripwire that has not fired.
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether the paired handle's `activate` was ever called.
    pub fn was_materialized(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }
}

/// A minimal descriptor-first ability whose activation is instrumented by a
/// [`MaterializationTripwire`] — a fixture, not a real capability, but one
/// real enough that its descriptor and its activation are the same object,
/// which is what makes "searching it never activates it" a meaningful check
/// rather than two unrelated values that happen to agree.
#[derive(Debug)]
pub struct TripwireAbility {
    name: String,
    description: String,
    kind: AbilityKind,
    tripwire: Arc<MaterializationTripwire>,
    payload: Activated,
}

impl TripwireAbility {
    /// Builds a fixture ability named `name` of `kind`, wired to `tripwire`,
    /// returning `payload` if it is ever actually activated.
    pub fn new(
        name: impl Into<String>,
        description: impl Into<String>,
        kind: AbilityKind,
        tripwire: Arc<MaterializationTripwire>,
        payload: Activated,
    ) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            kind,
            tripwire,
            payload,
        }
    }

    /// The tripwire this ability's activation is wired to.
    pub fn tripwire(&self) -> &Arc<MaterializationTripwire> {
        &self.tripwire
    }
}

impl Named for TripwireAbility {
    fn name(&self) -> &str {
        &self.name
    }
}

impl Ability for TripwireAbility {
    fn description(&self) -> &str {
        &self.description
    }

    fn kind(&self) -> AbilityKind {
        self.kind.clone()
    }
}

impl ActivationHandle for TripwireAbility {
    fn activate(&self) -> Result<Activated, ActivationError> {
        self.tripwire.0.store(true, Ordering::SeqCst);
        Ok(self.payload.clone())
    }
}

/// Builds a descriptor for `kind`/`name` with `summary` as both title and
/// card summary, at revision `"1"`, for suite fixtures.
pub fn conformance_descriptor(kind: AbilityKind, name: &str, summary: &str) -> AbilityDescriptor {
    AbilityDescriptor::new(
        kind,
        name,
        EntryProvenance::new(RegistrySource::BuiltIn, RegistryRevision::new("1")),
        name,
        summary,
        RegistryRevision::new("1"),
    )
}

/// Asserts that building and searching a descriptor — its card match,
/// affordances, dependencies, readiness, and context cost — never activates
/// the ability behind it. Discovery must be possible with zero I/O; only
/// [`activate`] may ever materialize content.
pub fn assert_building_and_searching_a_descriptor_never_activates(
    ability: &TripwireAbility,
    terms: &[String],
) {
    let descriptor = ability.descriptor();
    let _ = descriptor.card().matches_any(terms);
    let _ = descriptor.affordances();
    let _ = descriptor.dependencies();
    let _ = descriptor.conflicts();
    let _ = descriptor.permissions();
    let _ = descriptor.risk();
    let _ = descriptor.readiness();
    let _ = descriptor.context_cost();
    let _ = descriptor.content_revision();
    let _ = descriptor.fingerprint();
    assert!(
        !ability.tripwire().was_materialized(),
        "building and searching a descriptor must never activate the ability behind it"
    );
}

/// Asserts [`activate`] runs `policy` to completion before ever calling
/// `handle`: a denying context never trips the handle, and an approving
/// context both trips it and returns its payload.
pub fn assert_activation_authorizes_before_materializing(
    descriptor: &AbilityDescriptor,
    handle: &TripwireAbility,
    policy: &dyn ActivationPolicy,
    denying_context: &ActivationContext,
    approving_context: &ActivationContext,
) {
    activate(descriptor, handle, policy, denying_context).expect_err(
        "the denying context must not authorize activation, or this assertion proves nothing",
    );
    assert!(
        !handle.tripwire().was_materialized(),
        "a denied, conflicting, unready, or stale-revision activation must never call the handle"
    );

    let payload = activate(descriptor, handle, policy, approving_context)
        .expect("the approving context must authorize activation");
    assert!(
        handle.tripwire().was_materialized(),
        "an authorized activation must actually call the handle"
    );
    assert_eq!(
        payload,
        handle
            .activate()
            .expect("the handle must still materialize once authorized"),
        "activation must return exactly the handle's materialized payload"
    );
}

/// Asserts a readiness failure names only the credential/configuration
/// *names* the descriptor itself declared — never anything else, and
/// structurally never a value, since [`ReadinessRequirement`] never stores
/// one.
pub fn assert_readiness_failure_names_only_declared_names(
    descriptor: &AbilityDescriptor,
    context: &ActivationContext,
) {
    let err = FailClosedPolicy
        .authorize(descriptor, context)
        .expect_err("this assertion requires a descriptor/context pair with unmet readiness");
    let ActivationError::ReadinessUnmet { missing } = err else {
        panic!(
            "expected `ActivationError::ReadinessUnmet`, got a different structural failure: {err:?}"
        );
    };
    assert!(
        !missing.is_empty(),
        "a readiness failure must name at least one missing requirement"
    );
    assert!(
        missing
            .iter()
            .all(|name| descriptor.readiness().credentials.contains(name)
                || descriptor.readiness().config_keys.contains(name)),
        "a readiness failure must only ever name a declared credential or configuration key, never a derived value"
    );
}

/// Asserts a conflicting activation fails with a structural
/// `ActivationError::Conflict` naming the id it conflicts with, and a denied
/// activation fails with a structural `ActivationError::Denied` — both
/// determined by policy alone, before any handle would ever run.
pub fn assert_conflict_and_denial_fail_structurally(
    conflicting: &AbilityDescriptor,
    conflict_context: &ActivationContext,
    conflicts_with: &RegistryId,
    denied: &AbilityDescriptor,
    denied_context: &ActivationContext,
) {
    let err = FailClosedPolicy
        .authorize(conflicting, conflict_context)
        .expect_err("a conflicting activation must be rejected");
    assert_eq!(
        err,
        ActivationError::Conflict {
            with: conflicts_with.clone()
        }
    );

    let err = FailClosedPolicy
        .authorize(denied, denied_context)
        .expect_err("a denied activation must be rejected");
    assert!(
        matches!(err, ActivationError::Denied { .. }),
        "a host-denied id must fail with `ActivationError::Denied`, got {err:?}"
    );
}

/// Runs every ability assertion over a standard fixture set.
pub fn assert_ability_conformance() {
    let tripwire = Arc::new(MaterializationTripwire::new());
    let ability = TripwireAbility::new(
        "conformance-skill",
        "Searches and summarizes conformance fixtures",
        AbilityKind::Skill,
        tripwire,
        Activated::SkillInstructions("do the thing".to_string()),
    );
    assert_building_and_searching_a_descriptor_never_activates(&ability, &["searches".to_string()]);

    let descriptor = ability
        .descriptor()
        .with_readiness(ReadinessRequirement::none().with_credentials(["CONFORMANCE_API_KEY"]));
    let unready_context = ActivationContext::new();
    let ready_context = ActivationContext::new().with_ready_credentials(["CONFORMANCE_API_KEY"]);

    assert_readiness_failure_names_only_declared_names(&descriptor, &unready_context);
    assert_activation_authorizes_before_materializing(
        &descriptor,
        &ability,
        &FailClosedPolicy,
        &unready_context,
        &ready_context,
    );

    let conflicting =
        conformance_descriptor(AbilityKind::Tool, "aggressive-edit", "edits aggressively")
            .with_conflicts([RegistryId::tool("safe-edit")]);
    let conflict_context = ActivationContext::new().with_active([RegistryId::tool("safe-edit")]);

    let denied =
        conformance_descriptor(AbilityKind::Mcp, "paid-search", "a metered search provider");
    let denied_context = ActivationContext::new().with_denied([RegistryId::mcp("paid-search")]);

    assert_conflict_and_denial_fail_structurally(
        &conflicting,
        &conflict_context,
        &RegistryId::tool("safe-edit"),
        &denied,
        &denied_context,
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_ability_crate_satisfies_the_conformance_suite() {
        assert_ability_conformance();
    }

    #[test]
    fn a_stale_expected_revision_also_fails_structurally_before_any_handle_runs() {
        let tripwire = Arc::new(MaterializationTripwire::new());
        let ability = TripwireAbility::new(
            "conformance-skill",
            "d",
            AbilityKind::Skill,
            tripwire,
            Activated::SkillInstructions("body".to_string()),
        );
        let descriptor = ability.descriptor();
        let stale_context = ActivationContext::new()
            .expecting_revision(RegistryRevision::new("not-the-current-one"));

        let err = activate(&descriptor, &ability, &FailClosedPolicy, &stale_context).unwrap_err();
        assert!(matches!(err, ActivationError::RevisionMismatch { .. }));
        assert!(!ability.tripwire().was_materialized());
    }
}
