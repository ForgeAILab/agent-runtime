use agent_runtime_core::content::{ContentPart, Message};
use agent_runtime_core::journal::*;
use agent_runtime_core::store::VersionedSessionState;
use serde_json::Value;

fn frozen(name: &str) -> (Vec<u8>, Value) {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/journal");
    (
        std::fs::read(root.join(format!("{name}.json"))).unwrap(),
        serde_json::from_slice(&std::fs::read(root.join(format!("{name}.digests.json"))).unwrap())
            .unwrap(),
    )
}

#[test]
fn journal_json_1_bytes_and_domain_type_version_digests_are_frozen() {
    assert_eq!(JOURNAL_ENCODING, "journal-json-1");
    assert_eq!(JOURNAL_FANOUT, 64);
    assert_eq!(
        JOURNAL_OBJECT_DOMAIN,
        b"agent-runtime/session-journal/object\0"
    );
    for (name, kind) in [
        ("signed-message", JournalObjectKind::Message),
        ("numbers-extension", JournalObjectKind::Extension),
        ("raw-message", JournalObjectKind::Message),
        ("projected-message", JournalObjectKind::Message),
        ("sequence-leaf", JournalObjectKind::Sequence),
        ("map-leaf", JournalObjectKind::Map),
    ] {
        let (bytes, digests) = frozen(name);
        let envelope: Value = serde_json::from_slice(&bytes).unwrap();
        let object = JournalObject::encode(kind, &envelope["value"]).unwrap();
        assert_eq!(object.bytes, bytes, "{name} canonical bytes changed");
        assert_eq!(object.id.as_str(), digests["schema_4"].as_str().unwrap());
        assert_eq!(
            journal_object_id(kind, 3, &bytes).as_str(),
            digests["schema_3"].as_str().unwrap()
        );
        assert_eq!(
            journal_object_id(JournalObjectKind::StateValue, 4, &bytes).as_str(),
            digests["other_type"].as_str().unwrap()
        );
        assert_ne!(digests["schema_4"], digests["schema_3"]);
        assert_ne!(digests["schema_4"], digests["other_type"]);
        assert_eq!(object.decode::<Value>(kind).unwrap(), envelope["value"]);
    }
}

#[test]
fn signed_reasoning_and_numeric_bits_round_trip_without_normalization() {
    let (bytes, digests) = frozen("signed-message");
    let object = JournalObject {
        id: JournalObjectId::parse(digests["schema_4"].as_str().unwrap()).unwrap(),
        bytes,
    };
    let message: Message = object.decode(JournalObjectKind::Message).unwrap();
    let ContentPart::Reasoning {
        text,
        signature,
        redacted,
    } = &message.content[0]
    else {
        panic!("not reasoning");
    };
    assert_eq!(text.as_bytes(), "thought\né\u{2028}".as_bytes());
    assert_eq!(
        signature.as_ref().unwrap().as_bytes(),
        b"sig+/=\0\n\xc3\xa9"
    );
    assert!(!redacted);
    assert_eq!(
        JournalObject::encode(JournalObjectKind::Message, &message).unwrap(),
        object
    );

    let (bytes, digests) = frozen("numbers-extension");
    let object = JournalObject {
        id: JournalObjectId::parse(digests["schema_4"].as_str().unwrap()).unwrap(),
        bytes,
    };
    let state: VersionedSessionState = object.decode(JournalObjectKind::Extension).unwrap();
    assert_eq!(
        state.value["negative_zero"].as_f64().unwrap().to_bits(),
        (-0.0f64).to_bits()
    );
    assert_eq!(
        state.value["positive_zero"].as_f64().unwrap().to_bits(),
        0.0f64.to_bits()
    );
    assert_eq!(state.value["nested"]["a"].as_u64(), Some(u64::MAX));
    assert_eq!(state.value["min"].as_i64(), Some(i64::MIN));
    assert_eq!(
        JournalObject::encode(JournalObjectKind::Extension, &state).unwrap(),
        object
    );

    // Values on which the default serde_json fast float parser can round at an
    // adjacent binary value: the explicit float_roundtrip feature is required.
    for value in [
        f64::from_bits(0x3ff0000000000001),
        f64::MIN_POSITIVE,
        f64::MAX,
        1.2345678901234567e-100,
    ] {
        let object = JournalObject::encode(JournalObjectKind::StateValue, &value).unwrap();
        assert_eq!(
            object
                .decode::<f64>(JournalObjectKind::StateValue)
                .unwrap()
                .to_bits(),
            value.to_bits()
        );
    }
}

#[test]
fn history_chain_is_frozen_and_order_sensitive() {
    let digests: Value =
        serde_json::from_str(include_str!("fixtures/journal/history.digests.json")).unwrap();
    assert_eq!(
        journal_history_chain([]).as_str(),
        digests["empty"].as_str().unwrap()
    );
    let (_, signed) = frozen("signed-message");
    let signed = JournalObjectId::parse(signed["schema_4"].as_str().unwrap()).unwrap();
    assert_eq!(
        journal_history_chain([&signed]).as_str(),
        digests["signed_message"].as_str().unwrap()
    );
    let other = JournalObject::encode(JournalObjectKind::Message, &Message::user("other"))
        .unwrap()
        .id;
    assert_ne!(
        journal_history_chain([&signed, &other]),
        journal_history_chain([&other, &signed])
    );
}

#[test]
fn private_commit_input_digest_is_frozen_and_binds_drafts_delta_fence_and_operation() {
    let mut commit: JournalCommit =
        serde_json::from_str(include_str!("fixtures/journal/commit-input.json")).unwrap();
    let expected = include_str!("fixtures/journal/commit-input.sha256").trim();
    assert_eq!(commit.input_digest().unwrap().as_str(), expected);
    commit.digest = commit.input_digest().unwrap();
    assert_eq!(
        commit.input_digest().unwrap().as_str(),
        expected,
        "digest excludes itself"
    );
    assert_eq!(
        commit.clone().input_digest().unwrap(),
        commit.digest,
        "identical retry remains exact"
    );
    for changed in 0..4 {
        let mut changed_commit = commit.clone();
        match changed {
            0 => changed_commit.batch.operation = JournalOperationId::new("different-operation"),
            1 => changed_commit.batch.writer_epoch += 1,
            2 => changed_commit.objects[0].bytes[0] ^= 1,
            _ => changed_commit
                .delta
                .bootstrap
                .as_mut()
                .unwrap()
                .extension_state
                .clear(),
        }
        assert_ne!(changed_commit.input_digest().unwrap(), commit.digest);
    }
    // Host projection changes stored content IDs, while exact input evidence
    // remains private and stable. It is not copied into projected JournalHead.
    let bootstrap = commit.delta.bootstrap.as_ref().unwrap();
    assert!(
        bootstrap
            .extension_state
            .values()
            .all(|state| state.sensitivity
                == agent_runtime_core::store::SessionStateSensitivity::Sensitive)
    );
    let (_, projection) = frozen("projected-message");
    let (_, raw) = frozen("raw-message");
    assert_ne!(projection["schema_4"], raw["schema_4"]);
    assert_eq!(commit.input_digest().unwrap().as_str(), expected);
}

#[test]
fn execution_relevant_json_object_order_is_frozen_without_changing_canonical_key_order() {
    let (bytes, digests) = frozen("ordered-extension");
    assert_eq!(
        journal_object_id(JournalObjectKind::Extension, 4, &bytes).as_str(),
        digests["schema_4"].as_str().unwrap()
    );
    assert_eq!(
        journal_object_id(JournalObjectKind::Extension, 3, &bytes).as_str(),
        digests["schema_3"].as_str().unwrap()
    );
    assert_eq!(
        journal_object_id(JournalObjectKind::StateValue, 4, &bytes).as_str(),
        digests["other_type"].as_str().unwrap()
    );
    let mut map = serde_json::Map::new();
    map.insert("z".to_owned(), serde_json::json!(1.5));
    map.insert("a".to_owned(), serde_json::json!(-0.0));
    let preserves_order = map.keys().map(String::as_str).eq(["z", "a"]);
    let object = JournalObject {
        id: JournalObjectId::parse(digests["schema_4"].as_str().unwrap()).unwrap(),
        bytes,
    };
    if preserves_order {
        let state = VersionedSessionState::new(
            agent_runtime_registry::RegistryRevision::new("fixture-v1"),
            Value::Object(map),
        );
        assert_eq!(
            JournalObject::encode(JournalObjectKind::Extension, &state).unwrap(),
            object
        );
        let restored: VersionedSessionState = object.decode(JournalObjectKind::Extension).unwrap();
        assert!(
            restored
                .value
                .as_object()
                .unwrap()
                .keys()
                .map(String::as_str)
                .eq(["z", "a"])
        );
        assert_eq!(
            JournalObject::encode(JournalObjectKind::Extension, &restored).unwrap(),
            object
        );
    } else {
        // Existing byte-order-dependent fingerprints cannot be reinterpreted
        // under a different map representation. Fail before any execution.
        assert!(
            object
                .decode::<VersionedSessionState>(JournalObjectKind::Extension)
                .is_err()
        );
    }
}

#[test]
fn committed_noncanonical_corrupt_or_wrong_typed_objects_fail_closed() {
    let original = JournalObject::encode(
        JournalObjectKind::StateValue,
        &serde_json::json!({"a":1,"b":-0.0}),
    )
    .unwrap();
    assert!(
        original
            .decode::<Value>(JournalObjectKind::Message)
            .is_err()
    );
    for bytes in [
        br#"{ "encoding":"journal-json-1","kind":"state_value","schema_version":4,"value":{"a":1,"b":-0.0}}"#.to_vec(),
        br#"{"encoding":"journal-json-1","kind":"state_value","schema_version":4,"value":{"b":-0.0,"a":1}}"#.to_vec(),
        br#"{"encoding":"journal-json-1","kind":"state_value","schema_version":4,"value":{"a":0,"a":1,"b":-0.0}}"#.to_vec(),
        br#"{"encoding":"journal-json-1","kind":"state_value","schema_version":4,"value":{"a":1,"b":-0}}"#.to_vec(),
    ] {
        // Even an attacker recomputing the hash cannot make a noncanonical
        // published frame valid; recovery cannot roll back to older work.
        let object = JournalObject { id: journal_object_id(JournalObjectKind::StateValue, 4, &bytes), bytes };
        let error = object.decode::<Value>(JournalObjectKind::StateValue).unwrap_err();
        assert!(matches!(error.class, agent_runtime_core::error::FailureClass::StateConflict { .. }));
        assert!(!format!("{error:?}").contains(object.id.as_str()));
    }
    let mut corrupt = original.clone();
    corrupt.bytes[0] ^= 1;
    assert!(
        corrupt
            .decode::<Value>(JournalObjectKind::StateValue)
            .is_err()
    );
    assert!(JournalObjectId::parse("F".repeat(64)).is_err());
    assert!(JournalObjectId::parse("0".repeat(63)).is_err());
    assert!(serde_json::from_str::<JournalObjectId>(r#""not-a-digest""#).is_err());
    assert!(!format!("{original:?}").contains(original.id.as_str()));
    assert!(!format!("{:?}", original.id).contains(original.id.as_str()));

    let mut unknown = serde_json::to_value(Message::user("exact")).unwrap();
    unknown["future_field"] = serde_json::json!("must not be silently dropped");
    let object = JournalObject::encode(JournalObjectKind::Message, &unknown).unwrap();
    assert!(
        object
            .decode::<Message>(JournalObjectKind::Message)
            .is_err()
    );
}
