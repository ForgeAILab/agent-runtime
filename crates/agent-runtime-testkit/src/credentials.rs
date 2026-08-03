//! Deterministic provider credential sources and barriers.

use std::collections::VecDeque;
use std::fmt;
use std::future::pending;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::Notify;

use agent_runtime_core::cancel::Cancellation;
use agent_runtime_core::clock::{Clock, Deadline, Timestamp};
use agent_runtime_core::provider_credential::{
    CredentialInvalidation, ProviderAuthRejection, ProviderCredentialError,
    ProviderCredentialLease, ProviderCredentialRevision, ProviderCredentialSource,
    ProviderCredentialTarget,
};
use agent_runtime_core::store::Secret;

/// Redacted input fixture for one lease issued by a renewable source.
#[derive(Clone)]
pub struct CredentialLeaseFixture {
    secret: String,
    expires_at: Option<Timestamp>,
    revision: ProviderCredentialRevision,
}

impl CredentialLeaseFixture {
    /// Creates a non-expiring lease fixture.
    pub fn non_expiring(
        secret: impl Into<String>,
        revision: impl Into<String>,
    ) -> Result<Self, ProviderCredentialError> {
        Ok(Self {
            secret: secret.into(),
            expires_at: None,
            revision: ProviderCredentialRevision::new(revision)?,
        })
    }

    /// Creates an expiring lease fixture.
    pub fn expiring(
        secret: impl Into<String>,
        expires_at: Timestamp,
        revision: impl Into<String>,
    ) -> Result<Self, ProviderCredentialError> {
        Ok(Self {
            secret: secret.into(),
            expires_at: Some(expires_at),
            revision: ProviderCredentialRevision::new(revision)?,
        })
    }

    fn lease(&self) -> ProviderCredentialLease {
        match self.expires_at {
            Some(expiry) => ProviderCredentialLease::expiring(
                Secret::new(self.secret.clone()),
                expiry,
                self.revision.clone(),
            ),
            None => ProviderCredentialLease::non_expiring(
                Secret::new(self.secret.clone()),
                self.revision.clone(),
            ),
        }
    }
}

impl fmt::Debug for CredentialLeaseFixture {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CredentialLeaseFixture([redacted])")
    }
}

/// One source acquisition call, containing no credential material.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CredentialAcquireRecord {
    /// Host-assigned provider target.
    pub target: String,
    /// Requested minimum remaining validity.
    pub minimum_validity_ms: u64,
    /// Attempt deadline supplied to the source.
    pub deadline: Deadline,
}

/// One exact-revision invalidation call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CredentialInvalidationRecord {
    /// Host-assigned provider target.
    pub target: String,
    /// Opaque rejected revision; its debug output remains opaque.
    pub revision: ProviderCredentialRevision,
    /// Bounded provider rejection classification.
    pub rejection: ProviderAuthRejection,
    /// Attempt deadline supplied to the source.
    pub deadline: Deadline,
}

#[derive(Debug, Default)]
struct BarrierState {
    started: AtomicBool,
    released: AtomicBool,
    changed: Notify,
}

/// A deterministic barrier for acquisition or invalidation fixtures.
#[derive(Debug, Clone, Default)]
pub struct CredentialBarrier {
    state: Arc<BarrierState>,
}

impl CredentialBarrier {
    /// Creates an unreleased barrier.
    pub fn new() -> Self {
        Self::default()
    }

    /// Waits until the source operation reaches this barrier.
    pub async fn wait_started(&self) {
        loop {
            let changed = self.state.changed.notified();
            if self.state.started.load(Ordering::Acquire) {
                return;
            }
            changed.await;
        }
    }

    /// Releases every operation waiting at the barrier.
    pub fn release(&self) {
        self.state.released.store(true, Ordering::Release);
        self.state.changed.notify_waiters();
    }

    async fn wait(
        &self,
        cancel: &Cancellation,
        deadline: Deadline,
        clock: &dyn Clock,
    ) -> Result<(), ProviderCredentialError> {
        self.state.started.store(true, Ordering::Release);
        self.state.changed.notify_waiters();
        loop {
            let changed = self.state.changed.notified();
            if self.state.released.load(Ordering::Acquire) {
                return Ok(());
            }
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return Err(ProviderCredentialError::Cancelled),
                _ = wait_for_deadline(deadline, clock) => {
                    return Err(ProviderCredentialError::Timeout);
                }
                _ = changed => {}
            }
        }
    }
}

async fn wait_for_deadline(deadline: Deadline, clock: &dyn Clock) {
    match deadline.remaining_millis(clock) {
        Some(0) => {}
        Some(ms) => tokio::time::sleep(Duration::from_millis(ms)).await,
        None => pending::<()>().await,
    }
}

#[derive(Debug)]
struct RenewableState {
    current: CredentialLeaseFixture,
    replacements: VecDeque<CredentialLeaseFixture>,
    invalidated: bool,
}

/// A deterministic renewable source with proactive refresh and exact-revision
/// invalidation semantics.
pub struct RenewableProviderCredentialSource {
    clock: Arc<dyn Clock>,
    state: Mutex<RenewableState>,
    acquire_barrier: Option<CredentialBarrier>,
    invalidate_barrier: Option<CredentialBarrier>,
    acquisitions: Mutex<Vec<CredentialAcquireRecord>>,
    invalidations: Mutex<Vec<CredentialInvalidationRecord>>,
}

impl RenewableProviderCredentialSource {
    /// Creates a source with one current lease and ordered replacements.
    pub fn new(
        clock: Arc<dyn Clock>,
        current: CredentialLeaseFixture,
        replacements: impl IntoIterator<Item = CredentialLeaseFixture>,
    ) -> Self {
        Self {
            clock,
            state: Mutex::new(RenewableState {
                current,
                replacements: replacements.into_iter().collect(),
                invalidated: false,
            }),
            acquire_barrier: None,
            invalidate_barrier: None,
            acquisitions: Mutex::new(Vec::new()),
            invalidations: Mutex::new(Vec::new()),
        }
    }

    /// Blocks acquisition at `barrier` until released, cancelled, or timed
    /// out.
    pub fn with_acquire_barrier(mut self, barrier: CredentialBarrier) -> Self {
        self.acquire_barrier = Some(barrier);
        self
    }

    /// Blocks invalidation at `barrier` until released, cancelled, or timed
    /// out.
    pub fn with_invalidate_barrier(mut self, barrier: CredentialBarrier) -> Self {
        self.invalidate_barrier = Some(barrier);
        self
    }

    /// Recorded acquisition calls.
    pub fn acquisitions(&self) -> Vec<CredentialAcquireRecord> {
        self.acquisitions
            .lock()
            .expect("credential acquisitions poisoned")
            .clone()
    }

    /// Recorded exact-revision invalidation calls.
    pub fn invalidations(&self) -> Vec<CredentialInvalidationRecord> {
        self.invalidations
            .lock()
            .expect("credential invalidations poisoned")
            .clone()
    }

    fn check_controls(
        &self,
        cancel: &Cancellation,
        deadline: Deadline,
    ) -> Result<(), ProviderCredentialError> {
        if cancel.is_cancelled() {
            return Err(ProviderCredentialError::Cancelled);
        }
        if deadline.is_expired(self.clock.as_ref()) {
            return Err(ProviderCredentialError::Timeout);
        }
        Ok(())
    }
}

impl fmt::Debug for RenewableProviderCredentialSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RenewableProviderCredentialSource")
            .field("acquisition_count", &self.acquisitions().len())
            .field("invalidation_count", &self.invalidations().len())
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl ProviderCredentialSource for RenewableProviderCredentialSource {
    async fn acquire(
        &self,
        target: &ProviderCredentialTarget,
        minimum_validity_ms: u64,
        cancel: &Cancellation,
        deadline: Deadline,
    ) -> Result<ProviderCredentialLease, ProviderCredentialError> {
        self.check_controls(cancel, deadline)?;
        if let Some(barrier) = &self.acquire_barrier {
            barrier.wait(cancel, deadline, self.clock.as_ref()).await?;
        }
        self.check_controls(cancel, deadline)?;
        self.acquisitions
            .lock()
            .expect("credential acquisitions poisoned")
            .push(CredentialAcquireRecord {
                target: target.as_str().to_owned(),
                minimum_validity_ms,
                deadline,
            });

        let minimum_expiry = self.clock.now().plus_millis(minimum_validity_ms);
        let mut state = self.state.lock().expect("credential state poisoned");
        let too_short = state
            .current
            .expires_at
            .is_some_and(|expiry| expiry < minimum_expiry);
        if state.invalidated || too_short {
            state.current = state
                .replacements
                .pop_front()
                .ok_or(ProviderCredentialError::RefreshFailed)?;
            state.invalidated = false;
        }
        Ok(state.current.lease())
    }

    async fn invalidate(
        &self,
        target: &ProviderCredentialTarget,
        rejected_revision: &ProviderCredentialRevision,
        rejection: ProviderAuthRejection,
        cancel: &Cancellation,
        deadline: Deadline,
    ) -> Result<CredentialInvalidation, ProviderCredentialError> {
        self.check_controls(cancel, deadline)?;
        if let Some(barrier) = &self.invalidate_barrier {
            barrier.wait(cancel, deadline, self.clock.as_ref()).await?;
        }
        self.check_controls(cancel, deadline)?;
        self.invalidations
            .lock()
            .expect("credential invalidations poisoned")
            .push(CredentialInvalidationRecord {
                target: target.as_str().to_owned(),
                revision: rejected_revision.clone(),
                rejection,
                deadline,
            });

        let mut state = self.state.lock().expect("credential state poisoned");
        if &state.current.revision != rejected_revision {
            return Ok(CredentialInvalidation::StaleRevision);
        }
        if state.replacements.is_empty() {
            return Ok(CredentialInvalidation::NoReplacement);
        }
        state.invalidated = true;
        Ok(CredentialInvalidation::ReplacementPossible)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ManualClock;
    use agent_runtime_core::cancel::CancelReason;
    use agent_runtime_core::clock::SystemClock;

    fn target() -> ProviderCredentialTarget {
        ProviderCredentialTarget::new("openrouter").expect("valid target")
    }

    #[tokio::test]
    async fn source_refreshes_proactively_and_stale_invalidation_keeps_newer_lease() {
        let clock = ManualClock::shared(0);
        let source = RenewableProviderCredentialSource::new(
            clock.clone(),
            CredentialLeaseFixture::expiring("old", Timestamp(10), "r1").unwrap(),
            [CredentialLeaseFixture::expiring("new", Timestamp(100), "r2").unwrap()],
        );
        let cancel = Cancellation::new();
        let first = source
            .acquire(&target(), 0, &cancel, Deadline::never())
            .await
            .unwrap();
        clock.advance(20);
        let second = source
            .acquire(&target(), 10, &cancel, Deadline::never())
            .await
            .unwrap();

        assert_eq!(first.secret().expose(), "old");
        assert_eq!(second.secret().expose(), "new");
        assert_eq!(
            source
                .invalidate(
                    &target(),
                    first.revision(),
                    ProviderAuthRejection::Unauthorized,
                    &cancel,
                    Deadline::never(),
                )
                .await
                .unwrap(),
            CredentialInvalidation::StaleRevision
        );
        let still_new = source
            .acquire(&target(), 10, &cancel, Deadline::never())
            .await
            .unwrap();
        assert_eq!(still_new.revision(), second.revision());
    }

    #[tokio::test]
    async fn acquisition_barrier_observes_cancellation() {
        let clock = ManualClock::shared(0);
        let barrier = CredentialBarrier::new();
        let source = Arc::new(
            RenewableProviderCredentialSource::new(
                clock,
                CredentialLeaseFixture::non_expiring("secret", "r1").unwrap(),
                [],
            )
            .with_acquire_barrier(barrier.clone()),
        );
        let cancel = Cancellation::new();
        let task = tokio::spawn({
            let source = source.clone();
            let cancel = cancel.clone();
            async move {
                source
                    .acquire(&target(), 0, &cancel, Deadline::never())
                    .await
            }
        });
        barrier.wait_started().await;
        cancel.cancel(CancelReason::UserRequested);

        assert!(matches!(
            task.await.unwrap(),
            Err(ProviderCredentialError::Cancelled)
        ));
    }

    #[tokio::test]
    async fn acquisition_barrier_observes_deadline() {
        let barrier = CredentialBarrier::new();
        let source = RenewableProviderCredentialSource::new(
            Arc::new(SystemClock),
            CredentialLeaseFixture::non_expiring("secret", "r1").unwrap(),
            [],
        )
        .with_acquire_barrier(barrier);

        let result = tokio::time::timeout(
            Duration::from_millis(100),
            source.acquire(
                &target(),
                0,
                &Cancellation::new(),
                Deadline::after(&SystemClock, 1),
            ),
        )
        .await
        .expect("barrier observes its deadline");

        assert!(matches!(result, Err(ProviderCredentialError::Timeout)));
        assert!(source.acquisitions().is_empty());
    }

    #[tokio::test]
    async fn current_revision_invalidation_promotes_one_replacement() {
        let source = RenewableProviderCredentialSource::new(
            ManualClock::shared(0),
            CredentialLeaseFixture::non_expiring("old", "r1").unwrap(),
            [CredentialLeaseFixture::non_expiring("new", "r2").unwrap()],
        );
        let cancel = Cancellation::new();
        let first = source
            .acquire(&target(), 0, &cancel, Deadline::never())
            .await
            .unwrap();

        assert_eq!(
            source
                .invalidate(
                    &target(),
                    first.revision(),
                    ProviderAuthRejection::Unauthorized,
                    &cancel,
                    Deadline::never(),
                )
                .await
                .unwrap(),
            CredentialInvalidation::ReplacementPossible
        );
        let replacement = source
            .acquire(&target(), 0, &cancel, Deadline::never())
            .await
            .unwrap();
        assert_eq!(replacement.secret().expose(), "new");
        assert_ne!(replacement.revision(), first.revision());
    }
}
