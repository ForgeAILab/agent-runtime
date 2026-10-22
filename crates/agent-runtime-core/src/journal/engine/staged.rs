use super::*;

/// Read-only view of one staged, not yet visible closure. It exists only so
/// [`JournalReader`] can validate a candidate head before publication.
pub(in crate::journal) struct StagedClosure<'a> {
    pub(in crate::journal) storage_id: &'a JournalStorageId,
    pub(in crate::journal) objects: &'a ObjectStore,
    pub(in crate::journal) closure: &'a BTreeSet<JournalObjectId>,
    pub(in crate::journal) trusted: &'a ReachableSet,
}

impl fmt::Debug for StagedClosure<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StagedClosure")
            .field("objects", &self.closure.len())
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl SessionJournal for StagedClosure<'_> {
    fn storage_id(&self) -> JournalStorageId {
        self.storage_id.clone()
    }

    async fn open(&self, _: &SessionId, _: JournalOpen) -> Result<JournalLease, RuntimeError> {
        Err(conflict())
    }

    async fn read(
        &self,
        _: &JournalLease,
        object: &JournalObjectId,
    ) -> Result<Vec<u8>, RuntimeError> {
        if !self.closure.contains(object) && !self.trusted.contains(object) {
            return Err(conflict());
        }
        let bytes = self.objects.get(object).cloned().ok_or_else(conflict)?;
        #[cfg(feature = "test-support")]
        crate::journal::profile::event("rt.work.validation_body_bytes", 1, bytes.len() as u64);
        Ok(bytes)
    }

    async fn commit(
        &self,
        _: &JournalLease,
        _: JournalCommit,
    ) -> Result<JournalHead, RuntimeError> {
        Err(conflict())
    }

    async fn renew(&self, _: &JournalLease) -> Result<JournalLease, RuntimeError> {
        Err(conflict())
    }

    async fn close(&self, _: JournalLease) -> Result<(), RuntimeError> {
        Err(conflict())
    }

    async fn retire(
        &self,
        _: &SessionId,
        _: JournalRevision,
    ) -> Result<JournalRetirement, RuntimeError> {
        Err(conflict())
    }

    async fn collect(&self, _: JournalGcRequest) -> Result<JournalGcReport, RuntimeError> {
        Err(conflict())
    }
}

/// Materializes a staged head through [`JournalReader`] without awaiting under
/// the transaction lock: every staged read is immediately ready, so a single
/// poll completes, and a pending poll fails closed.
pub(in crate::journal) fn materialize_staged_checkpoint(
    storage_id: &JournalStorageId,
    head: &JournalHead,
    objects: &ObjectStore,
    closure: &BTreeSet<JournalObjectId>,
    trusted: &ReachableSet,
    snapshot: &JournalSnapshotView,
    preparation: &JournalPreparationHints,
) -> Result<(), RuntimeError> {
    #[cfg(feature = "test-support")]
    let _profile = crate::journal::profile::enter_sync("rt.staged.materialize_staged_checkpoint");
    let view = StagedClosure {
        storage_id,
        objects,
        closure,
        trusted,
    };
    let lease = JournalLease::new(
        storage_id.clone(),
        head.session.clone(),
        Some(head.clone()),
        None,
        Arc::new(()),
    );
    let limits = JournalReadLimits {
        objects: u64::MAX,
        encoded_bytes: u64::MAX,
        entries: u64::MAX,
    };
    let mut materialized = pin!(async {
        let mut reader = JournalReader::new(&view, &lease, limits)?;
        reader
            .validate_checkpoint_with_view(snapshot, preparation)
            .await
    });
    match materialized
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(result) => result.map_err(|_| conflict()),
        Poll::Pending => Err(conflict()),
    }
}

pub(in crate::journal) fn materialize_staged_snapshot(
    storage_id: &JournalStorageId,
    head: &JournalHead,
    objects: &ObjectStore,
    closure: &BTreeSet<JournalObjectId>,
    trusted: &ReachableSet,
) -> Result<SessionSnapshot, RuntimeError> {
    #[cfg(feature = "test-support")]
    let _profile = crate::journal::profile::enter_sync("rt.staged.materialize_staged_snapshot");
    let view = StagedClosure {
        storage_id,
        objects,
        closure,
        trusted,
    };
    let lease = JournalLease::new(
        storage_id.clone(),
        head.session.clone(),
        Some(head.clone()),
        None,
        Arc::new(()),
    );
    let limits = JournalReadLimits {
        objects: u64::MAX,
        encoded_bytes: u64::MAX,
        entries: u64::MAX,
    };
    let mut materialized = pin!(async {
        let mut reader = JournalReader::new(&view, &lease, limits)?;
        reader.snapshot().await?.ok_or_else(conflict)
    });
    match materialized
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(result) => result.map_err(|_| conflict()),
        Poll::Pending => Err(conflict()),
    }
}
