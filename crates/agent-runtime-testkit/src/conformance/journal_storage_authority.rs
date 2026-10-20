//! Direct storage authority probes, including warm paths and real digest collisions.
use super::*;

fn put(session: &str, object: &JournalObject) -> JournalWrite {
    let mut write = raw_write(session, "authority", None);
    write.objects.objects = vec![JournalStoredObject {
        id: object.id.clone(),
        kind: object.kind().unwrap(),
        bytes: object.bytes.clone().into(),
    }];
    write
}

pub(super) async fn body_cache_membership<S, F, Fut>(factory: &F)
where
    S: JournalStorage,
    F: Fn() -> Fut,
    Fut: Future<Output = Arc<S>>,
{
    let storage = factory().await;
    let object = JournalObject::encode(JournalObjectKind::JsonInline, &"warm body").unwrap();
    let owner = put("cache-owner", &object);
    let other = put("cache-other", &object);
    for write in [&owner, &other] {
        storage.apply(write.clone()).await.unwrap();
    }
    let ids = [object.id.clone()];
    // Warm the exact membership being revoked, rather than only another holder.
    for _ in 0..2 {
        assert_eq!(
            storage
                .objects(&owner.domain, &owner.session, &ids)
                .await
                .unwrap(),
            vec![Some(Arc::from(object.bytes.clone()))]
        );
    }
    let mut release = edits(&owner);
    release.release = ids.to_vec();
    storage.apply(release).await.unwrap();
    for session in [&owner.session, &SessionId::new("cache-stranger")] {
        for _ in 0..2 {
            assert_eq!(
                storage.objects(&owner.domain, session, &ids).await.unwrap(),
                vec![None],
                "a cache hit must recheck current membership"
            );
        }
    }
}

pub(super) async fn full_object_id_lookup<S, F, Fut>(factory: &F)
where
    S: JournalStorage,
    F: Fn() -> Fut,
    Fut: Future<Output = Arc<S>>,
{
    let storage = factory().await;
    // Precomputed valid schema-4 SHA-256 collision on the first FOUR bytes.
    // These bodies are deliberately different; no synthetic/inconsistent ids.
    let objects = [2426, 83180].map(|n| {
        JournalObject::encode(
            JournalObjectKind::JsonInline,
            &format!("prefix-collision-{n}"),
        )
        .unwrap()
    });
    assert_eq!(objects[0].id.to_bytes()[..4], objects[1].id.to_bytes()[..4]);
    assert_ne!(objects[0].id, objects[1].id);
    let mut write = put("prefix", &objects[0]);
    write.objects.objects.push(JournalStoredObject {
        id: objects[1].id.clone(),
        kind: objects[1].kind().unwrap(),
        bytes: objects[1].bytes.clone().into(),
    });
    write.objects.objects.sort_by(|a, b| a.id.cmp(&b.id));
    storage.apply(write.clone()).await.unwrap();
    for order in [[0, 1, 0], [1, 0, 1]] {
        let ids: Vec<_> = order.iter().map(|&i| objects[i].id.clone()).collect();
        let expected: Vec<_> = order
            .iter()
            .map(|&i| Some(Arc::from(objects[i].bytes.clone())))
            .collect();
        assert_eq!(
            storage
                .objects(&write.domain, &write.session, &ids)
                .await
                .unwrap(),
            expected,
            "lookup must compare all 32 id bytes and preserve request order"
        );
    }
    let mut absent = objects[0].id.to_bytes();
    absent[31] ^= 1;
    assert_eq!(
        storage
            .objects(
                &write.domain,
                &write.session,
                &[JournalObjectId::from_bytes(absent)]
            )
            .await
            .unwrap(),
        vec![None],
        "same-prefix non-member must remain absent"
    );
}

pub(super) async fn exact_membership_enumeration<S, F, Fut>(factory: &F)
where
    S: JournalStorage,
    F: Fn() -> Fut,
    Fut: Future<Output = Arc<S>>,
{
    let storage = factory().await;
    let objects = ["held elsewhere", "only here"]
        .map(|value| JournalObject::encode(JournalObjectKind::JsonInline, &value).unwrap());
    let owner = put("members-owner", &objects[0]);
    let other = put("members-other", &objects[1]);
    for write in [&owner, &other] {
        storage.apply(write.clone()).await.unwrap();
    }
    assert_eq!(
        storage
            .members(&owner.domain, &owner.session)
            .await
            .unwrap(),
        vec![objects[0].id.clone()],
        "members must exclude bodies admitted only to another session"
    );
    assert!(
        storage
            .members(&owner.domain, &SessionId::new("never-admitted"))
            .await
            .unwrap()
            .is_empty(),
        "a never-admitted session has no members"
    );
    let mut other_role = owner.domain.clone();
    other_role.role = JournalRole::Ordinary;
    let mut other_storage = owner.domain.clone();
    other_storage.storage_id = JournalStorageId::new("members-other-domain");
    for d in [other_role, other_storage] {
        assert!(
            storage
                .members(&d, &owner.session)
                .await
                .unwrap()
                .is_empty(),
            "membership is domain scoped"
        );
    }
    // Keep an independent live copy so physical presence cannot masquerade as membership.
    let holder = put("members-holder", &objects[0]);
    storage.apply(holder).await.unwrap();
    let mut release = edits(&owner);
    release.release = vec![objects[0].id.clone()];
    storage.apply(release).await.unwrap();
    assert!(
        storage
            .members(&owner.domain, &owner.session)
            .await
            .unwrap()
            .is_empty(),
        "members must exclude released ids even while another holder retains the body"
    );
    assert_eq!(
        storage
            .members(&other.domain, &other.session)
            .await
            .unwrap(),
        vec![objects[1].id.clone()]
    );
}
