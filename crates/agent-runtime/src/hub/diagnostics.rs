//! Redaction-safe filter and snapshot diagnostics.
//!
//! Per `design.md` Decision 4, discovery must never let a caller infer that a
//! specific unauthorized entry exists. Every type in this module is built so
//! that guarantee holds *structurally*: [`ScopeDiagnostics`] and everything it
//! contains is made of `usize` counters only. There is no field capable of
//! holding a [`RegistryId`](agent_runtime_registry::RegistryId), a name, or a
//! card, so no future edit to this module's `Debug` output, `Display`, or
//! serialization can regress the guarantee by accident — it would need a new
//! field of a new type to do it, not a formatting mistake.
//!
//! [`classify_reason`] explains an exclusion; it never decides one. It
//! re-checks the same [`crate::hub::scope::ScopeInputs`] facts already baked
//! into the [`agent_runtime_registry::ViewFilter`] a scope built, so the
//! reported reason can never diverge from what actually happened at view
//! construction.

use agent_runtime_registry::{RegistryId, RegistrySource};

use crate::hub::scope::ScopeInputs;

/// Why one entry was excluded from a scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExclusionReason {
    /// An explicit deny (by id, domain, or source) or an unsatisfied
    /// allow-list, including a compatibility mismatch such as an
    /// unsupported model modality.
    Denied,
    /// The entry is not confirmed ready (credentials, configuration, health,
    /// or availability).
    NotReady,
    /// The entry's domain is not authorized for the querying surface (an
    /// internal domain, or a domain requiring model-routing authority that
    /// was not granted).
    DomainNotAuthorized,
    /// The entry exceeds a configured risk or quota budget.
    RiskOrQuota,
}

/// Aggregate exclusion counts for one domain, by reason. Never carries an id,
/// a name, or a card — only counts.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ExclusionReasons {
    /// Entries denied by explicit policy or an unsatisfied allow-list.
    pub denied: usize,
    /// Entries not confirmed ready.
    pub not_ready: usize,
    /// Entries whose domain is not authorized for this surface.
    pub domain_not_authorized: usize,
    /// Entries excluded by a risk or quota budget.
    pub risk_or_quota: usize,
}

impl ExclusionReasons {
    fn record(&mut self, reason: ExclusionReason) {
        match reason {
            ExclusionReason::Denied => self.denied += 1,
            ExclusionReason::NotReady => self.not_ready += 1,
            ExclusionReason::DomainNotAuthorized => self.domain_not_authorized += 1,
            ExclusionReason::RiskOrQuota => self.risk_or_quota += 1,
        }
    }

    /// The total number of excluded entries across every reason.
    pub fn total(&self) -> usize {
        self.denied + self.not_ready + self.domain_not_authorized + self.risk_or_quota
    }
}

/// Aggregate visibility counts for one domain. Never carries an id, a name,
/// or a card — only counts.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DomainDiagnostics {
    /// The number of entries this domain contributed to the hub.
    pub total: usize,
    /// The number of entries visible on the surface these diagnostics
    /// describe.
    pub visible: usize,
    /// The number of entries excluded (`total - visible`).
    pub excluded: usize,
    /// Why each excluded entry was excluded, aggregated by reason.
    pub reasons: ExclusionReasons,
}

/// Aggregate filter and snapshot diagnostics across every domain. Never
/// carries an id, a name, or a card — only counts, so it is safe to log,
/// return to a host, or render in an error report without disclosing which
/// specific entry a scope excluded.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ScopeDiagnostics {
    /// Ability domain diagnostics.
    pub abilities: DomainDiagnostics,
    /// Provider domain diagnostics.
    pub providers: DomainDiagnostics,
    /// Model domain diagnostics.
    pub models: DomainDiagnostics,
    /// Tokenizer domain diagnostics.
    pub tokenizers: DomainDiagnostics,
    /// Context-policy domain diagnostics.
    pub context_policies: DomainDiagnostics,
}

/// Classifies why one entry would be excluded from a scope, or returns `None`
/// if it would be visible.
///
/// `extra_policy_denied` and `extra_risk_or_quota` let a caller fold
/// domain-specific exclusions (a risk budget, a quota, a modality mismatch)
/// into the same report without this function needing to know about them.
/// Priority among simultaneous causes is fixed (denied, then unauthorized,
/// then unready, then risk/quota) only so one entry reports exactly one
/// reason; every branch here corresponds to a condition the same scope
/// already applied when it built its `ViewFilter`, so the total exclusion
/// this function reports can never diverge from what the view actually
/// excluded.
pub(crate) fn classify_reason(
    id: &RegistryId,
    source: RegistrySource,
    inputs: &ScopeInputs,
    extra_policy_denied: bool,
    domain_authorized: bool,
    extra_risk_or_quota: bool,
) -> Option<ExclusionReason> {
    let policy_denied = extra_policy_denied
        || inputs.denies_id(id)
        || inputs.denies_domain(&id.domain)
        || inputs.denies_source(source)
        || inputs.violates_allowed_ids(id)
        || inputs.violates_allowed_domains(&id.domain)
        || inputs.violates_allowed_sources(source);
    if policy_denied {
        return Some(ExclusionReason::Denied);
    }
    if !domain_authorized {
        return Some(ExclusionReason::DomainNotAuthorized);
    }
    if inputs.requires_readiness() && !inputs.is_ready(id) {
        return Some(ExclusionReason::NotReady);
    }
    if extra_risk_or_quota {
        return Some(ExclusionReason::RiskOrQuota);
    }
    None
}

/// Computes one domain's diagnostics from its full id/source sequence.
pub(crate) fn domain_diagnostics<'a>(
    entries: impl Iterator<Item = (&'a RegistryId, RegistrySource)>,
    inputs: &ScopeInputs,
    extra_policy_denied: impl Fn(&RegistryId) -> bool,
    domain_authorized: bool,
    extra_risk_or_quota: impl Fn(&RegistryId) -> bool,
) -> DomainDiagnostics {
    let mut total = 0usize;
    let mut reasons = ExclusionReasons::default();
    for (id, source) in entries {
        total += 1;
        if let Some(reason) = classify_reason(
            id,
            source,
            inputs,
            extra_policy_denied(id),
            domain_authorized,
            extra_risk_or_quota(id),
        ) {
            reasons.record(reason);
        }
    }
    let excluded = reasons.total();
    DomainDiagnostics {
        total,
        visible: total.saturating_sub(excluded),
        excluded,
        reasons,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exclusion_reasons_never_expose_anything_but_counts() {
        let mut reasons = ExclusionReasons::default();
        reasons.record(ExclusionReason::Denied);
        reasons.record(ExclusionReason::RiskOrQuota);
        reasons.record(ExclusionReason::RiskOrQuota);
        assert_eq!(reasons.total(), 3);
        assert_eq!(reasons.denied, 1);
        assert_eq!(reasons.risk_or_quota, 2);
    }

    #[test]
    fn domain_diagnostics_accounts_for_every_entry() {
        let inputs = ScopeInputs::new().deny_id(RegistryId::tool("denied"));
        let ids = [
            (RegistryId::tool("denied"), RegistrySource::BuiltIn),
            (RegistryId::tool("visible"), RegistrySource::BuiltIn),
        ];
        let diagnostics = domain_diagnostics(
            ids.iter().map(|(id, source)| (id, *source)),
            &inputs,
            |_| false,
            true,
            |_| false,
        );
        assert_eq!(diagnostics.total, 2);
        assert_eq!(diagnostics.visible, 1);
        assert_eq!(diagnostics.excluded, 1);
        assert_eq!(diagnostics.reasons.denied, 1);
    }
}
