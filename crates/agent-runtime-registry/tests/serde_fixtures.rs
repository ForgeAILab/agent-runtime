//! Serialization fixtures for the registry kernel's public data types.
//!
//! These run only with the optional `serde` feature enabled and never touch
//! registry semantics: a snapshot's fingerprint and resolution behavior must
//! be identical whether or not this feature is compiled in. What is tested
//! here is that the *shape* of the wire format for structured errors is
//! stable (a literal fixture, so accidental schema drift fails loudly) and
//! that every other serde-enabled type round-trips through JSON without loss.
#![cfg(feature = "serde")]

use agent_runtime_registry::{
    EntryProvenance, RegistryCard, RegistryDomain, RegistryError, RegistryId, RegistryRevision,
    RegistrySource, ViewFilter,
};

#[test]
fn a_registry_id_round_trips_through_json() {
    let id = RegistryId::tool("browser");
    let json = serde_json::to_string(&id).unwrap();
    let back: RegistryId = serde_json::from_str(&json).unwrap();
    assert_eq!(id, back);
}

#[test]
fn a_registry_domain_serializes_as_a_snake_case_string() {
    assert_eq!(
        serde_json::to_string(&RegistryDomain::ContextPolicy).unwrap(),
        "\"context_policy\""
    );
    assert_eq!(
        serde_json::to_string(&RegistryDomain::other("custom")).unwrap(),
        "{\"other\":\"custom\"}"
    );
}

#[test]
fn a_registry_source_serializes_as_a_snake_case_string() {
    assert_eq!(
        serde_json::to_string(&RegistrySource::BuiltIn).unwrap(),
        "\"built_in\""
    );
}

#[test]
fn entry_provenance_round_trips_and_omits_a_missing_override() {
    let provenance = EntryProvenance::new(RegistrySource::BuiltIn, RegistryRevision::new("1"));
    let json = serde_json::to_string(&provenance).unwrap();
    assert!(!json.contains("overrides"));
    let back: EntryProvenance = serde_json::from_str(&json).unwrap();
    assert_eq!(provenance, back);

    let overriding = provenance.overriding(RegistrySource::Plugin);
    let json = serde_json::to_string(&overriding).unwrap();
    assert!(json.contains("overrides"));
    let back: EntryProvenance = serde_json::from_str(&json).unwrap();
    assert_eq!(overriding, back);
}

#[test]
fn a_registry_card_round_trips_through_json() {
    let card = RegistryCard::new(
        RegistryId::skill("web-research"),
        EntryProvenance::new(RegistrySource::BuiltIn, RegistryRevision::new("1")),
        "Web research",
        "Searches the web",
    )
    .with_tags(["research"])
    .with_keywords(["search", "browse"]);

    let json = serde_json::to_string(&card).unwrap();
    let back: RegistryCard = serde_json::from_str(&json).unwrap();
    assert_eq!(card, back);
}

/// A literal JSON fixture for one `RegistryError` variant. `Vec<RegistryId>`
/// has a fixed, deterministic order (unlike the `HashSet`-backed
/// [`ViewFilter`] below), so asserting on the exact rendered string is safe
/// and catches accidental field renames or shape changes.
#[test]
fn a_duplicate_in_layer_error_has_a_stable_wire_shape() {
    let err = RegistryError::DuplicateInLayer {
        id: RegistryId::tool("browser"),
        source: RegistrySource::Plugin,
    };
    let json = serde_json::to_value(&err).unwrap();
    assert_eq!(
        json,
        serde_json::json!({
            "duplicate_in_layer": {
                "id": { "domain": "tool", "name": "browser" },
                "source": "plugin",
            }
        })
    );
}

#[test]
fn an_alias_cycle_error_round_trips_its_full_path() {
    let err = RegistryError::AliasCycle {
        path: vec![
            RegistryId::tool("a"),
            RegistryId::tool("b"),
            RegistryId::tool("a"),
        ],
    };
    let json = serde_json::to_string(&err).unwrap();
    let back: RegistryError = serde_json::from_str(&json).unwrap();
    assert_eq!(err, back);
}

/// `ViewFilter`'s allow/deny sets are `HashSet`s, whose JSON array order is
/// not guaranteed stable across process runs. A literal string fixture would
/// be flaky; a round-trip-to-equal check is not, since `HashSet` equality is
/// order-independent.
#[test]
fn a_view_filter_round_trips_through_json_regardless_of_hash_set_iteration_order() {
    let filter = ViewFilter::new()
        .allow_domain(RegistryDomain::Tool)
        .allow_domain(RegistryDomain::Skill)
        .deny_id(RegistryId::mcp("browser"))
        .deny_source(RegistrySource::Remote)
        .require_readiness()
        .ready(RegistryId::skill("web-research"))
        .agent_facing(true);

    let json = serde_json::to_string(&filter).unwrap();
    let back: ViewFilter = serde_json::from_str(&json).unwrap();
    assert_eq!(filter, back);
}

#[test]
fn an_empty_view_filter_round_trips_through_json() {
    let filter = ViewFilter::new();
    let json = serde_json::to_string(&filter).unwrap();
    let back: ViewFilter = serde_json::from_str(&json).unwrap();
    assert_eq!(filter, back);
}
