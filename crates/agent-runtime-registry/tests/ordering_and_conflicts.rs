//! Property tests: sealing must be a function of *content*, never of
//! *registration order*.
//!
//! There is no `proptest`-style dependency available here — the crate is
//! std-only by default and this test exercises the default feature set — so
//! these tests instead exhaustively permute a small number of declarations
//! and assert every permutation seals to the same observable result. That is
//! a strictly stronger check than a handful of hand-picked orderings for the
//! same reason property tests generally are: it does not rely on the author
//! having guessed the order that would expose a bug.

use agent_runtime_registry::{
    EntryProvenance, RegistryBuilder, RegistryCard, RegistryEntry, RegistryId, RegistryRevision,
    RegistrySource,
};

/// All permutations of `items`, via Heap's algorithm. `items.len()` is kept
/// small (<= 5) everywhere this is used, so the factorial blowup is cheap.
fn permutations<T: Clone>(items: &[T]) -> Vec<Vec<T>> {
    let mut items = items.to_vec();
    let n = items.len();
    let mut result = Vec::new();
    let mut c = vec![0usize; n];
    result.push(items.clone());
    let mut i = 0;
    while i < n {
        if c[i] < i {
            if i % 2 == 0 {
                items.swap(0, i);
            } else {
                items.swap(c[i], i);
            }
            result.push(items.clone());
            c[i] += 1;
            i = 0;
        } else {
            c[i] = 0;
            i += 1;
        }
    }
    result
}

fn entry(id: RegistryId, source: RegistrySource) -> RegistryEntry<&'static str> {
    RegistryEntry::new(
        RegistryCard::new(
            id,
            EntryProvenance::new(source, RegistryRevision::new("1")),
            "t",
            "s",
        ),
        "payload",
    )
}

fn entry_overriding(
    id: RegistryId,
    source: RegistrySource,
    overrides: RegistrySource,
) -> RegistryEntry<&'static str> {
    RegistryEntry::new(
        RegistryCard::new(
            id,
            EntryProvenance::new(source, RegistryRevision::new("1")).overriding(overrides),
            "t",
            "s",
        ),
        "payload",
    )
}

#[test]
fn every_registration_order_of_five_independent_entries_seals_identically() {
    let entries = vec![
        entry(RegistryId::tool("zebra"), RegistrySource::BuiltIn),
        entry(RegistryId::tool("apple"), RegistrySource::BuiltIn),
        entry(RegistryId::skill("web-research"), RegistrySource::BuiltIn),
        entry(RegistryId::agent("planner"), RegistrySource::BuiltIn),
        entry(RegistryId::model("gpt"), RegistrySource::BuiltIn),
    ];

    let mut fingerprints = Vec::new();
    let mut orderings = Vec::new();
    for permutation in permutations(&entries) {
        let mut builder = RegistryBuilder::new();
        for e in permutation {
            builder.declare(e);
        }
        let snapshot = builder.seal().unwrap();
        fingerprints.push(snapshot.fingerprint());
        orderings.push(
            snapshot
                .iter()
                .map(|e| e.id().qualified())
                .collect::<Vec<_>>(),
        );
    }

    assert!(fingerprints.windows(2).all(|w| w[0] == w[1]));
    assert!(orderings.windows(2).all(|w| w[0] == w[1]));
}

#[test]
fn every_registration_order_of_an_authorized_three_layer_override_resolves_to_the_same_winner() {
    let base = vec![
        entry(RegistryId::tool("browser"), RegistrySource::BuiltIn),
        entry(RegistryId::tool("browser"), RegistrySource::Remote),
        entry_overriding(
            RegistryId::tool("browser"),
            RegistrySource::Plugin,
            RegistrySource::Remote,
        ),
    ];

    let mut fingerprints = Vec::new();
    for permutation in permutations(&base) {
        let mut builder = RegistryBuilder::new();
        for e in permutation {
            builder.declare(e);
        }
        let snapshot = builder.seal().unwrap();
        assert_eq!(snapshot.len(), 1);
        let winner = snapshot.get(&RegistryId::tool("browser")).unwrap();
        assert_eq!(winner.provenance().source, RegistrySource::Plugin);
        fingerprints.push(snapshot.fingerprint());
    }

    assert!(fingerprints.windows(2).all(|w| w[0] == w[1]));
}

#[test]
fn every_registration_order_of_an_unauthorized_conflict_fails_with_the_same_structured_error() {
    let conflicting = vec![
        entry(RegistryId::tool("browser"), RegistrySource::BuiltIn),
        entry(RegistryId::tool("browser"), RegistrySource::Plugin),
    ];

    let mut errors = Vec::new();
    for permutation in permutations(&conflicting) {
        let mut builder = RegistryBuilder::new();
        for e in permutation {
            builder.declare(e);
        }
        errors.push(builder.seal().unwrap_err());
    }

    assert!(errors.windows(2).all(|w| w[0] == w[1]));
}

#[test]
fn every_declaration_order_of_duplicate_same_layer_entries_fails_with_the_same_error() {
    let duplicates = vec![
        entry(RegistryId::tool("browser"), RegistrySource::BuiltIn),
        entry(RegistryId::tool("browser"), RegistrySource::BuiltIn),
        entry(RegistryId::tool("browser"), RegistrySource::BuiltIn),
    ];

    for permutation in permutations(&duplicates) {
        let mut builder = RegistryBuilder::new();
        for e in permutation {
            builder.declare(e);
        }
        let err = builder.seal().unwrap_err();
        assert_eq!(
            err,
            agent_runtime_registry::RegistryError::DuplicateInLayer {
                id: RegistryId::tool("browser"),
                source: RegistrySource::BuiltIn,
            }
        );
    }
}

#[test]
fn every_declaration_order_of_a_multi_hop_alias_chain_resolves_identically() {
    // Aliases and their target entry, registered in every order: the alias
    // graph itself has no notion of "layer", so nothing here should ever
    // fail; only the resolution result and fingerprint must stay constant.
    #[derive(Clone)]
    enum Declaration {
        Entry(RegistryEntry<&'static str>),
        Alias(RegistryId, RegistryId),
    }

    let declarations = vec![
        Declaration::Entry(entry(RegistryId::tool("browser"), RegistrySource::BuiltIn)),
        Declaration::Alias(
            RegistryId::tool("legacy-browser"),
            RegistryId::tool("browser"),
        ),
        Declaration::Alias(RegistryId::tool("web"), RegistryId::tool("legacy-browser")),
    ];

    let mut fingerprints = Vec::new();
    for permutation in permutations(&declarations) {
        let mut builder = RegistryBuilder::new();
        for declaration in permutation {
            match declaration {
                Declaration::Entry(e) => {
                    builder.declare(e);
                }
                Declaration::Alias(from, to) => {
                    builder.alias(from, to);
                }
            }
        }
        let snapshot = builder.seal().unwrap();
        assert_eq!(
            snapshot.resolve_alias(&RegistryId::tool("web")),
            Some(&RegistryId::tool("browser"))
        );
        fingerprints.push(snapshot.fingerprint());
    }

    assert!(fingerprints.windows(2).all(|w| w[0] == w[1]));
}
