//! Bounded collection protocol: fence-bound pages, no-effect preflight and
//! complete atomic collection applies.
use super::*;

async fn pages<S: JournalStorage>(
    storage: &S,
    d: &JournalDomain,
    s: &SessionId,
    kind: JournalCollectionPageKind,
    fence: JournalFence,
    max_items: u32,
) -> (Vec<JournalCollectionItems>, usize) {
    let mut out = Vec::new();
    let mut after = None;
    let mut calls = 0;
    loop {
        calls += 1;
        assert!(calls < 10_000, "a page sequence must reach its end marker");
        let page = storage
            .collection_page(
                d,
                s,
                JournalCollectionPageRequest {
                    kind,
                    fence,
                    after,
                    max_items,
                },
            )
            .await
            .unwrap();
        assert_eq!(
            page.fence, fence,
            "a page reports the fence it was read under"
        );
        assert!(
            page.items.len() <= max_items as usize,
            "a page must not exceed max_items"
        );
        after = page.next;
        out.push(page.items);
        if after.is_none() {
            return (out, calls);
        }
    }
}

fn member_ids(items: &[JournalCollectionItems]) -> Vec<JournalObjectId> {
    items
        .iter()
        .flat_map(|items| match items {
            JournalCollectionItems::Members(ids) => ids.clone(),
            other => panic!("member page returned another kind: {other:?}"),
        })
        .collect()
}

pub(super) async fn bounded_collection_protocol<S, F, Fut>(factory: &F)
where
    S: JournalStorage,
    F: Fn() -> Fut,
    Fut: Future<Output = Arc<S>>,
{
    let storage = factory().await;
    let JournalCollectionCapability::Bounded(limits) = storage.collection_capability() else {
        panic!("collection conformance requires the bounded collection capability");
    };
    assert!(limits.max_page_items > 0 && limits.max_apply_units > 0);
    let first = raw_write("bounded", "one", None);
    storage.apply(first.clone()).await.unwrap();
    let head = first
        .record
        .head
        .as_ref()
        .unwrap()
        .decode(JournalObjectKind::Head)
        .unwrap();
    let second = raw_write("bounded", "two", Some(&head));
    storage.apply(second.clone()).await.unwrap();
    let other = raw_write("bounded-other", "three", None);
    storage.apply(other.clone()).await.unwrap();
    let d = &second.domain;
    let s = &second.session;
    let fence = second.record.fence();
    let mut members = storage.members(d, s).await.unwrap();
    members.sort();
    assert!(members.len() > 2);

    // One-item pages enumerate membership exactly once, scoped to the session.
    let (items, calls) = pages(
        &*storage,
        d,
        s,
        JournalCollectionPageKind::Members,
        fence,
        1,
    )
    .await;
    assert!(calls >= members.len(), "one-item pages must be bounded");
    let mut paged = member_ids(&items);
    let unique: BTreeSet<_> = paged.iter().cloned().collect();
    assert_eq!(unique.len(), paged.len(), "a member appears exactly once");
    paged.sort();
    assert_eq!(paged, members, "pages enumerate exact membership");
    let (items, _) = pages(
        &*storage,
        d,
        s,
        JournalCollectionPageKind::Members,
        fence,
        3,
    )
    .await;
    let mut paged = member_ids(&items);
    paged.sort();
    assert_eq!(paged, members);
    // Several pins, one per page: a defect that drops pins from a page is the
    // one page defect that would free live bodies.
    let (pins, pin_calls) = pages(&*storage, d, s, JournalCollectionPageKind::Pins, fence, 1).await;
    let paged_pins: Vec<_> = pins
        .into_iter()
        .flat_map(|items| match items {
            JournalCollectionItems::Pins(pins) => pins,
            other => panic!("pin page returned another kind: {other:?}"),
        })
        .collect();
    assert_eq!(
        paged_pins,
        storage.pins(d, s).await.unwrap(),
        "pin pages in pin order"
    );
    assert!(paged_pins.len() >= 2, "the case seeds several pins");
    assert!(
        pin_calls >= paged_pins.len(),
        "one-item pin pages are bounded"
    );

    // A changed generation rejects both a fresh request and an old continuation.
    let stale = first.record.fence();
    let request = |fence, after| JournalCollectionPageRequest {
        kind: JournalCollectionPageKind::Members,
        fence,
        after,
        max_items: 1,
    };
    assert_conflict(
        storage
            .collection_page(d, s, request(stale, None))
            .await
            .expect_err("a page under a stale fence must be rejected"),
    );
    let continuation = storage
        .collection_page(d, s, request(fence, None))
        .await
        .unwrap()
        .next
        .expect("a one-item page of a larger membership continues");
    assert_conflict(
        storage
            .collection_page(
                d,
                &other.session,
                request(other.record.fence(), Some(continuation.clone())),
            )
            .await
            .expect_err("a continuation is bound to its session"),
    );
    assert_conflict(
        storage
            .collection_page(
                d,
                s,
                JournalCollectionPageRequest {
                    kind: JournalCollectionPageKind::Pins,
                    fence,
                    after: Some(continuation.clone()),
                    max_items: 1,
                },
            )
            .await
            .expect_err("a continuation is bound to its page kind"),
    );

    // Preflight is read-only; apply is complete and atomic under the fence.
    let release: Vec<_> = first
        .objects
        .objects
        .iter()
        .map(|o| o.id.clone())
        .filter(|id| members.contains(id))
        .filter(|id| !second.objects.objects.iter().any(|o| &o.id == id))
        .collect();
    assert!(
        !release.is_empty(),
        "the first revision leaves unreferenced members"
    );
    let mut write = edits(&second);
    write.release = release.clone();
    write.catalog = release
        .iter()
        .cloned()
        .map(JournalCatalogChange::Remove)
        .collect();
    write.prune_operations_before = Some(JournalRevision(0));
    let plan = JournalCollectionPlan {
        write: write.clone(),
        max_units: limits.max_apply_units,
    };
    let before = storage.load(d, s).await.unwrap();
    let preflight = storage.prepare_collection_apply(&plan).await.unwrap();
    let JournalCollectionPreflight::Fits { units: declared } = preflight else {
        panic!("a small release fits: {preflight:?}");
    };
    assert!(
        declared <= limits.max_apply_units,
        "a small release fits: {preflight:?}"
    );
    assert_eq!(
        storage.load(d, s).await.unwrap(),
        before,
        "preflight has no effect"
    );
    let mut unchanged = storage.members(d, s).await.unwrap();
    unchanged.sort();
    assert_eq!(unchanged, members, "preflight has no effect on membership");
    let mut not_collect = plan.clone();
    not_collect.write.kind = JournalWriteKind::Commit;
    storage
        .apply_collection(not_collect)
        .await
        .expect_err("only collection writes use the collection apply");
    let receipt = storage.apply_collection(plan.clone()).await.unwrap();
    assert!(
        receipt.rewritten_units <= declared,
        "an apply rewrites at most its declared units ({} > {declared})",
        receipt.rewritten_units
    );
    let mut after = storage.members(d, s).await.unwrap();
    after.sort();
    let expected: Vec<_> = members
        .iter()
        .filter(|id| !release.contains(id))
        .cloned()
        .collect();
    assert_eq!(
        after, expected,
        "the complete declared release batch applies"
    );
    assert_eq!(
        storage.load(d, s).await.unwrap().unwrap().generation,
        write.record.generation
    );
    assert_conflict(
        storage
            .collection_page(d, s, request(fence, Some(continuation)))
            .await
            .expect_err("an old continuation cannot page a newer generation"),
    );
    assert_conflict(
        storage
            .apply_collection(plan)
            .await
            .expect_err("a stale collection plan must be rejected"),
    );
    let mut final_members = storage.members(d, s).await.unwrap();
    final_members.sort();
    assert_eq!(final_members, expected, "a rejected plan has no effect");
}
