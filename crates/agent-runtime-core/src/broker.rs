//! Host-mediated broker contracts: credentials, network egress, and
//! filesystem access.
//!
//! Every broker here mediates one category of grant-derived host authority
//! (security-enforcement's "Profile-conformant isolated execution": "only
//! grant-derived, broker-mediated host operations"). None of them is
//! implemented in this crate — a conforming host supplies each broker, the
//! same way it supplies a [`crate::provider::Provider`] or
//! [`crate::tool::Tool`].
//!
//! [`EgressBroker`] in particular is an *adopted host conformance contract*,
//! not a runtime-owned HTTP stack (design.md Decision 6): this crate
//! authorizes the normalized request tuple but neither performs DNS
//! resolution nor dials a connection, and depends on no HTTP client or URL
//! parsing crate to do so.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::cancel::Cancellation;
use crate::clock::Deadline;
use crate::grant::CapabilityGrant;
use crate::manifest::SegmentSensitivity;

// ---------------------------------------------------------------------
// Credential broker
// ---------------------------------------------------------------------

/// An opaque, bounded reference to a credential a tool may request use of
/// (security-enforcement's "Credential non-disclosure"; design.md
/// Decision 5). Never the secret value itself — just a stable name a
/// [`CredentialBroker`] resolves internally.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CredentialRef(String);

impl CredentialRef {
    /// Wraps a credential reference name.
    pub fn new(reference: impl Into<String>) -> Self {
        Self(reference.into())
    }

    /// The reference as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for CredentialRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A destination a [`CredentialBroker`] writes brokered material directly
/// into.
///
/// This is the structural reason [`CredentialBroker::apply_to`] cannot hand
/// a raw secret back to its caller: the method's own return type is
/// `Result<(), CredentialError>`, which carries no secret-shaped data at
/// all. The only place brokered material can go is through
/// [`CredentialSink::receive`], which the *caller* supplies — a `Tool`
/// implementation, which never holds a sink and never holds a
/// `dyn CredentialBroker`, has no path to this data through this API.
pub trait CredentialSink: Send {
    /// Receives one brokered field's rendered value for `field_name`. An
    /// implementation owns where the bytes go next (for example into a
    /// header map assembled at the transport boundary); this contract does
    /// not, and this crate does not implement one.
    fn receive(&mut self, field_name: &str, value: &[u8]);
}

/// A structured, bounded credential-resolution failure
/// (security-enforcement's "Credential non-disclosure": "Credential
/// resolution fails" — "a bounded error containing the credential reference
/// name or identifier only... no partial secret or backend diagnostic is
/// released"). Every variant below carries only a [`CredentialRef`], never
/// a backend diagnostic message that could itself echo partial secret
/// material.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CredentialError {
    /// No credential is bound to this reference.
    NotFound {
        /// The reference that failed to resolve. Never the secret.
        reference: CredentialRef,
    },
    /// The credential exists but the broker declined to apply it (for
    /// example an authorization precondition, such as endpoint approval,
    /// was not satisfied first — design.md Decision 6's credential-
    /// injection ordering).
    Unauthorized {
        /// The reference. Never the secret.
        reference: CredentialRef,
    },
    /// Resolution failed for a backend-internal reason bounded to a fixed,
    /// non-secret code — never a raw backend diagnostic message.
    ResolutionFailed {
        /// The reference. Never the secret.
        reference: CredentialRef,
        /// A stable, non-secret failure code.
        code: String,
    },
}

/// A host-injected credential broker (security-enforcement's "Credential
/// non-disclosure"; design.md Decision 5).
///
/// **Why no method on this trait can return a raw secret to a tool.**
/// [`CredentialBroker::apply_to`] returns `Result<(), CredentialError>` —
/// neither arm carries a secret value — and writes brokered material only
/// into a caller-supplied [`CredentialSink`], never into its own return
/// value. [`CredentialBroker::sign`] returns a signature, which does not
/// permit recovering the signing key. There is no `fn resolve(&self, ...)
/// -> Secret`-shaped method here, and deliberately never will be one added
/// to this trait: the accessor shape that would let a tool receive a raw
/// secret is not expressible against this surface.
///
/// The broker's own internal secret storage — where a resolved value
/// actually lives while an operation executes — is outside this
/// engine-neutral contract; a conforming implementation is responsible for
/// the zeroizing-storage properties design.md Decision 5 describes.
#[async_trait]
pub trait CredentialBroker: Send + Sync + fmt::Debug {
    /// Writes the rendered value of `field_name` for the credential
    /// `reference` resolves to directly into `sink`, without ever
    /// returning it through this method's own return type.
    async fn apply_to(
        &self,
        reference: &CredentialRef,
        field_name: &str,
        sink: &mut dyn CredentialSink,
        cancel: &Cancellation,
        deadline: Deadline,
    ) -> Result<(), CredentialError>;

    /// Signs `payload` using the credential `reference` resolves to,
    /// returning only the signature — never the signing key or any other
    /// representation of the credential itself.
    async fn sign(
        &self,
        reference: &CredentialRef,
        payload: &[u8],
        cancel: &Cancellation,
        deadline: Deadline,
    ) -> Result<Vec<u8>, CredentialError>;
}

// ---------------------------------------------------------------------
// Network egress broker
// ---------------------------------------------------------------------

/// The normalized request tuple an [`EgressBroker`] authorizes
/// (security-enforcement's "Network egress endpoint authorization"): scheme,
/// IDNA (UTS-46, A-label) hostname, explicit port, method, and a
/// percent-decode/re-encode-normalized path.
///
/// Every field is caller-supplied text. This contract defines what an
/// authorization decision is made against, not how a host performs
/// percent-decoding, IDNA canonicalization, or dot-segment rejection —
/// per design.md Decision 6, the runtime specifies transport *behavior* as
/// a conformance contract rather than owning a URL-parsing dependency, so
/// this type deliberately carries no parsing/normalization logic of its
/// own.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct EgressTuple {
    /// The request scheme (for example `https`).
    pub scheme: String,
    /// The IDNA A-label canonical hostname.
    pub host: String,
    /// The explicit port.
    pub port: u16,
    /// The request method.
    pub method: String,
    /// The normalized path.
    pub path: String,
}

/// One outbound request an [`EgressBroker`] authorizes before any DNS
/// resolution or connection is attempted
/// (security-enforcement's "Network egress endpoint authorization";
/// "Sensitivity-aware data egress").
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EgressRequest {
    /// The normalized request tuple.
    pub tuple: EgressTuple,
    /// Caller-supplied headers. A rule MAY further constrain which names
    /// and values are permitted.
    pub headers: BTreeMap<String, String>,
    /// Caller-supplied query parameter keys. A rule MAY further constrain
    /// which keys are permitted.
    pub query_keys: BTreeSet<String>,
    /// The request content type, if any.
    pub content_type: Option<String>,
    /// The request body size in bytes, if known ahead of send.
    pub body_bytes: Option<u64>,
    /// The credential this request would bind, if any. Authorization of the
    /// endpoint MUST precede resolving or injecting it (design.md
    /// Decision 6).
    pub credential: Option<CredentialRef>,
    /// The sensitivity classification of the payload this request would
    /// carry (security-enforcement's "Sensitivity-aware data egress": "An
    /// allowed endpoint MUST NOT imply authority to transmit arbitrary
    /// workspace, user, credential, or tool-result content").
    pub payload_sensitivity: SegmentSensitivity,
}

/// The single guest-visible egress denial class
/// (security-enforcement's "Network egress endpoint authorization": "Guest-
/// visible egress denials collapse to a single denial class regardless of
/// cause").
///
/// Carries no field, code, or message: an allowlist miss, an ambiguous URL,
/// and a protected-address-class denial MUST be indistinguishable to the
/// guest, so this type has no variant or data that could leak which of
/// those occurred. It is a zero-sized type — there is no bit pattern this
/// value could take on that would let two different denial causes compare
/// unequal. The discriminating reason belongs only in a host-side event
/// (security-enforcement's "Security decision event emission"), which is
/// out of this contract's scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EgressDenied;

/// A successful egress authorization: the runtime's own decision that a
/// conforming host transport (design.md Decision 6) may now dial `tuple`.
///
/// Carries no credential and no live connection: this contract authorizes
/// the request tuple. DNS resolution, dialing, connection pooling, and TLS
/// remain the conforming host transport's own responsibility, upstream of
/// anything this crate can observe or own.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EgressAuthorization {
    tuple: EgressTuple,
    redirects_enabled: bool,
    max_redirect_hops: u32,
}

impl EgressAuthorization {
    /// Builds an authorization for `tuple`. Redirects default to disabled
    /// with zero hops permitted, matching security-enforcement's "Network
    /// redirect handling": "Redirects SHALL be disabled by default."
    pub fn new(tuple: EgressTuple) -> Self {
        Self {
            tuple,
            redirects_enabled: false,
            max_redirect_hops: 0,
        }
    }

    /// Explicitly enables redirect following for this authorization, up to
    /// `max_hops`.
    pub fn with_redirects_enabled(mut self, max_hops: u32) -> Self {
        self.redirects_enabled = true;
        self.max_redirect_hops = max_hops;
        self
    }

    /// The authorized tuple.
    pub fn tuple(&self) -> &EgressTuple {
        &self.tuple
    }

    /// Whether redirect following was explicitly enabled for this
    /// authorization.
    pub fn redirects_enabled(&self) -> bool {
        self.redirects_enabled
    }

    /// The host-configured redirect hop-count ceiling.
    pub fn max_redirect_hops(&self) -> u32 {
        self.max_redirect_hops
    }
}

/// A host-injected network egress broker
/// (security-enforcement's "Network egress endpoint authorization",
/// "Network redirect handling", "Sensitivity-aware data egress";
/// design.md Decision 6: an adopted host conformance contract, not a
/// runtime-owned HTTP client).
///
/// This trait authorizes; it does not dial. A conforming implementation —
/// or its production successor, `HttpTransport` — owns DNS resolution,
/// connection pooling, redirect surfacing, and TLS, none of which this
/// crate depends on, links, or can observe. What "conforming" means for
/// those parts is a shared conformance suite outside this crate, per
/// design.md Decision 6's "Consequence" paragraph, not additional trait
/// surface here.
#[async_trait]
pub trait EgressBroker: Send + Sync + fmt::Debug {
    /// Authorizes `request`'s normalized tuple before any DNS resolution or
    /// connection is attempted. Rules MAY further constrain headers, query
    /// keys, body/response size, content type, and credential binding —
    /// this contract's job is the decision boundary, not the concrete rule
    /// representation, which is host policy data.
    async fn authorize(
        &self,
        request: &EgressRequest,
        cancel: &Cancellation,
        deadline: Deadline,
    ) -> Result<EgressAuthorization, EgressDenied>;

    /// Reauthorizes one redirect hop as a new request
    /// (security-enforcement's "Network redirect handling").
    ///
    /// An implementation MUST deny unless `original` explicitly enabled
    /// redirect following ([`EgressAuthorization::redirects_enabled`]), and
    /// MUST evaluate `target` — including a rewritten method, for example a
    /// 303's POST-to-GET passed as `rewritten_method` — through exactly the
    /// same checks as [`EgressBroker::authorize`], never a relaxed path.
    async fn authorize_redirect(
        &self,
        original: &EgressAuthorization,
        target: &EgressTuple,
        rewritten_method: Option<&str>,
        cancel: &Cancellation,
        deadline: Deadline,
    ) -> Result<EgressAuthorization, EgressDenied>;
}

// ---------------------------------------------------------------------
// Filesystem broker
// ---------------------------------------------------------------------

/// A virtual guest mount name a filesystem grant is exposed under
/// (security-enforcement's "Handle-relative filesystem protection").
/// Distinct from a host path: a mount name has no relationship to any host
/// directory string a caller could supply as authority.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MountName(String);

impl MountName {
    /// Wraps a virtual mount name.
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    /// The mount name as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for MountName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A single filesystem right a [`FilesystemHandle`] may hold
/// (security-enforcement's "Handle-relative filesystem protection":
/// "separate read, write, create, delete, rename, link, symlink-create,
/// readdir, and truncate permissions").
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilesystemRight {
    /// Read file contents.
    Read,
    /// Write to an existing file.
    Write,
    /// Create a new file.
    Create,
    /// Delete a file.
    Delete,
    /// Rename an entry.
    Rename,
    /// Create a hard link.
    Link,
    /// Create a symlink. Denied unless explicitly granted, independent of
    /// every other right (security-enforcement's "Handle-relative
    /// filesystem protection": "symlink creation MUST be denied unless a
    /// permission explicitly grants it").
    SymlinkCreate,
    /// List directory entries.
    Readdir,
    /// Truncate a file.
    Truncate,
}

/// A bounded set of [`FilesystemRight`]s a [`FilesystemHandle`] was opened
/// with.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct FilesystemRights(BTreeSet<FilesystemRight>);

impl FilesystemRights {
    /// No rights.
    pub fn none() -> Self {
        Self::default()
    }

    /// A set containing exactly one right.
    pub fn single(right: FilesystemRight) -> Self {
        Self(BTreeSet::from([right]))
    }

    /// Adds a right.
    pub fn with(mut self, right: FilesystemRight) -> Self {
        self.0.insert(right);
        self
    }

    /// Whether `right` is a member.
    pub fn contains(&self, right: FilesystemRight) -> bool {
        self.0.contains(&right)
    }
}

impl FromIterator<FilesystemRight> for FilesystemRights {
    fn from_iter<I: IntoIterator<Item = FilesystemRight>>(iter: I) -> Self {
        Self(iter.into_iter().collect())
    }
}

/// The single guest-visible filesystem denial class
/// (security-enforcement's "Handle-relative filesystem protection": "Guest-
/// visible filesystem failures collapse to a single denial class regardless
/// of cause; in particular `ENOENT` and `EPERM` outside an active grant
/// MUST be indistinguishable to the guest").
///
/// Carries no errno, path, or cause — the same zero-sized-type pattern as
/// [`EgressDenied`], for the same reason: there must be no data on this
/// value two different failures could disagree on. The distinguishing
/// errno and path detail belong only in a host-side event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FilesystemError;

/// A host-opened, grant-scoped filesystem handle
/// (security-enforcement's "Handle-relative filesystem protection").
///
/// **Why there is no path-taking constructor and no absolute-path API
/// anywhere on this trait.** Every operation resolves relative to `self` —
/// the specific host object [`FilesystemBroker::open`] returned when the
/// grant was issued — never relative to a path string supplied at call
/// time. Every method below takes path *segments* (`&[String]`) relative to
/// whichever directory `self` already denotes; none accepts a single path
/// string, and none returns a host path string a caller could feed back
/// into a different API as authority. This is what
/// security-enforcement's "Handle-relative filesystem protection" means by
/// "the runtime MUST NOT re-resolve an authorized path string at the time
/// of use": there is no function signature here through which that
/// re-resolution could even be expressed.
#[async_trait]
pub trait FilesystemHandle: Send + Sync + fmt::Debug {
    /// The virtual guest mount name this handle was opened under.
    fn mount(&self) -> &MountName;

    /// The rights this handle was opened with.
    fn rights(&self) -> &FilesystemRights;

    /// Reads the file at `segments`, relative to this handle. Fails unless
    /// [`FilesystemRight::Read`] was granted.
    async fn read(
        &self,
        segments: &[String],
        cancel: &Cancellation,
        deadline: Deadline,
    ) -> Result<Vec<u8>, FilesystemError>;

    /// Writes `data` to `segments`, relative to this handle. Fails unless
    /// [`FilesystemRight::Write`] was granted.
    async fn write(
        &self,
        segments: &[String],
        data: &[u8],
        cancel: &Cancellation,
        deadline: Deadline,
    ) -> Result<(), FilesystemError>;

    /// Creates a new file at `segments`, relative to this handle. Fails
    /// unless [`FilesystemRight::Create`] was granted.
    async fn create(
        &self,
        segments: &[String],
        cancel: &Cancellation,
        deadline: Deadline,
    ) -> Result<(), FilesystemError>;

    /// Deletes `segments`, relative to this handle. Fails unless
    /// [`FilesystemRight::Delete`] was granted.
    async fn delete(
        &self,
        segments: &[String],
        cancel: &Cancellation,
        deadline: Deadline,
    ) -> Result<(), FilesystemError>;

    /// Renames `from` to `to`, both relative to this handle. Fails unless
    /// [`FilesystemRight::Rename`] was granted.
    async fn rename(
        &self,
        from: &[String],
        to: &[String],
        cancel: &Cancellation,
        deadline: Deadline,
    ) -> Result<(), FilesystemError>;

    /// Creates a hard link at `to` pointing at `from`, both relative to
    /// this handle. Fails unless [`FilesystemRight::Link`] was granted.
    async fn link(
        &self,
        from: &[String],
        to: &[String],
        cancel: &Cancellation,
        deadline: Deadline,
    ) -> Result<(), FilesystemError>;

    /// Creates a symlink at `segments`, relative to this handle, pointing
    /// at `target`. Fails unless [`FilesystemRight::SymlinkCreate`] was
    /// explicitly granted.
    async fn create_symlink(
        &self,
        segments: &[String],
        target: &[String],
        cancel: &Cancellation,
        deadline: Deadline,
    ) -> Result<(), FilesystemError>;

    /// Lists entries at `segments`, relative to this handle. Fails unless
    /// [`FilesystemRight::Readdir`] was granted.
    async fn readdir(
        &self,
        segments: &[String],
        cancel: &Cancellation,
        deadline: Deadline,
    ) -> Result<Vec<String>, FilesystemError>;

    /// Truncates the file at `segments`, relative to this handle, to `len`
    /// bytes. Fails unless [`FilesystemRight::Truncate`] was granted.
    async fn truncate(
        &self,
        segments: &[String],
        len: u64,
        cancel: &Cancellation,
        deadline: Deadline,
    ) -> Result<(), FilesystemError>;
}

/// A host-injected filesystem broker
/// (security-enforcement's "Handle-relative filesystem protection").
#[async_trait]
pub trait FilesystemBroker: Send + Sync + fmt::Debug {
    /// Opens a fresh handle scoped to `mount` with exactly `rights`, bound
    /// to `grant`, at the moment the grant is issued.
    ///
    /// This is the only place a mount name is ever consumed by this
    /// contract: every subsequent operation goes through the returned
    /// [`FilesystemHandle`], never back through the broker with a fresh
    /// path — the handle used at enforcement is, by construction, the one
    /// opened here.
    async fn open(
        &self,
        mount: &MountName,
        rights: FilesystemRights,
        grant: &CapabilityGrant,
        cancel: &Cancellation,
        deadline: Deadline,
    ) -> Result<Box<dyn FilesystemHandle>, FilesystemError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- CredentialBroker ----------------------------------------------

    #[derive(Default)]
    struct RecordingSink {
        received: Vec<(String, Vec<u8>)>,
    }

    impl CredentialSink for RecordingSink {
        fn receive(&mut self, field_name: &str, value: &[u8]) {
            self.received.push((field_name.to_owned(), value.to_vec()));
        }
    }

    #[derive(Debug)]
    struct FakeCredentialBroker;

    #[async_trait]
    impl CredentialBroker for FakeCredentialBroker {
        async fn apply_to(
            &self,
            reference: &CredentialRef,
            field_name: &str,
            sink: &mut dyn CredentialSink,
            _cancel: &Cancellation,
            _deadline: Deadline,
        ) -> Result<(), CredentialError> {
            if reference.as_str() == "missing" {
                return Err(CredentialError::NotFound {
                    reference: reference.clone(),
                });
            }
            sink.receive(field_name, b"bearer material");
            Ok(())
        }

        async fn sign(
            &self,
            _reference: &CredentialRef,
            payload: &[u8],
            _cancel: &Cancellation,
            _deadline: Deadline,
        ) -> Result<Vec<u8>, CredentialError> {
            Ok(payload.to_vec())
        }
    }

    #[tokio::test]
    async fn credential_material_only_reaches_the_caller_through_a_sink() {
        let broker = FakeCredentialBroker;
        let mut sink = RecordingSink::default();
        let outcome = broker
            .apply_to(
                &CredentialRef::new("api-key"),
                "authorization",
                &mut sink,
                &Cancellation::new(),
                Deadline::never(),
            )
            .await;
        // The method's own return value carries nothing secret-shaped.
        assert_eq!(outcome, Ok(()));
        // The only place the material appeared is the sink the caller
        // itself supplied.
        assert_eq!(sink.received.len(), 1);
        assert_eq!(sink.received[0].0, "authorization");
    }

    #[tokio::test]
    async fn credential_resolution_failure_carries_only_the_reference() {
        let broker = FakeCredentialBroker;
        let mut sink = RecordingSink::default();
        let err = broker
            .apply_to(
                &CredentialRef::new("missing"),
                "authorization",
                &mut sink,
                &Cancellation::new(),
                Deadline::never(),
            )
            .await
            .unwrap_err();
        assert_eq!(
            err,
            CredentialError::NotFound {
                reference: CredentialRef::new("missing")
            }
        );
        assert!(sink.received.is_empty());
    }

    // --- EgressBroker ----------------------------------------------------

    fn tuple(path: &str) -> EgressTuple {
        EgressTuple {
            scheme: "https".into(),
            host: "api.example.test".into(),
            port: 443,
            method: "GET".into(),
            path: path.into(),
        }
    }

    fn request(path: &str) -> EgressRequest {
        EgressRequest {
            tuple: tuple(path),
            headers: BTreeMap::new(),
            query_keys: BTreeSet::new(),
            content_type: None,
            body_bytes: None,
            credential: None,
            payload_sensitivity: SegmentSensitivity::Public,
        }
    }

    #[derive(Debug)]
    struct FakeEgressBroker;

    #[async_trait]
    impl EgressBroker for FakeEgressBroker {
        async fn authorize(
            &self,
            request: &EgressRequest,
            _cancel: &Cancellation,
            _deadline: Deadline,
        ) -> Result<EgressAuthorization, EgressDenied> {
            if request.tuple.path == "/v1/jobs" {
                Ok(EgressAuthorization::new(request.tuple.clone()))
            } else {
                Err(EgressDenied)
            }
        }

        async fn authorize_redirect(
            &self,
            _original: &EgressAuthorization,
            _target: &EgressTuple,
            _rewritten_method: Option<&str>,
            _cancel: &Cancellation,
            _deadline: Deadline,
        ) -> Result<EgressAuthorization, EgressDenied> {
            Err(EgressDenied)
        }
    }

    #[tokio::test]
    async fn allowlist_miss_and_protected_address_denial_are_the_same_guest_visible_type() {
        let broker = FakeEgressBroker;
        // Two denials with entirely different underlying causes (an
        // unlisted path here; a real broker would separately deny a
        // protected-address-class resolution) both surface as exactly the
        // same value.
        let unlisted = broker
            .authorize(
                &request("/v1/other"),
                &Cancellation::new(),
                Deadline::never(),
            )
            .await
            .unwrap_err();
        let also_denied = broker
            .authorize(
                &request("/v1/elsewhere"),
                &Cancellation::new(),
                Deadline::never(),
            )
            .await
            .unwrap_err();
        assert_eq!(unlisted, also_denied);
        assert_eq!(unlisted, EgressDenied);
    }

    #[test]
    fn egress_denied_and_filesystem_error_carry_no_discriminating_data() {
        // A zero-sized type has exactly one possible value: there is no bit
        // pattern left over on which two denials could differ.
        assert_eq!(std::mem::size_of::<EgressDenied>(), 0);
        assert_eq!(std::mem::size_of::<FilesystemError>(), 0);
    }

    #[tokio::test]
    async fn redirects_are_reauthorized_through_the_same_broker_never_followed_silently() {
        let broker = FakeEgressBroker;
        let authorized = broker
            .authorize(
                &request("/v1/jobs"),
                &Cancellation::new(),
                Deadline::never(),
            )
            .await
            .unwrap();
        assert!(!authorized.redirects_enabled());
        let redirect = broker
            .authorize_redirect(
                &authorized,
                &tuple("/v1/other"),
                Some("GET"),
                &Cancellation::new(),
                Deadline::never(),
            )
            .await;
        assert_eq!(redirect, Err(EgressDenied));
    }

    // --- FilesystemBroker --------------------------------------------------

    #[derive(Debug)]
    struct FakeFilesystemHandle {
        mount: MountName,
        rights: FilesystemRights,
    }

    #[async_trait]
    impl FilesystemHandle for FakeFilesystemHandle {
        fn mount(&self) -> &MountName {
            &self.mount
        }

        fn rights(&self) -> &FilesystemRights {
            &self.rights
        }

        async fn read(
            &self,
            segments: &[String],
            _cancel: &Cancellation,
            _deadline: Deadline,
        ) -> Result<Vec<u8>, FilesystemError> {
            if !self.rights.contains(FilesystemRight::Read) {
                return Err(FilesystemError);
            }
            Ok(segments.join("/").into_bytes())
        }

        async fn write(
            &self,
            _segments: &[String],
            _data: &[u8],
            _cancel: &Cancellation,
            _deadline: Deadline,
        ) -> Result<(), FilesystemError> {
            Err(FilesystemError)
        }

        async fn create(
            &self,
            _segments: &[String],
            _cancel: &Cancellation,
            _deadline: Deadline,
        ) -> Result<(), FilesystemError> {
            Err(FilesystemError)
        }

        async fn delete(
            &self,
            _segments: &[String],
            _cancel: &Cancellation,
            _deadline: Deadline,
        ) -> Result<(), FilesystemError> {
            Err(FilesystemError)
        }

        async fn rename(
            &self,
            _from: &[String],
            _to: &[String],
            _cancel: &Cancellation,
            _deadline: Deadline,
        ) -> Result<(), FilesystemError> {
            Err(FilesystemError)
        }

        async fn link(
            &self,
            _from: &[String],
            _to: &[String],
            _cancel: &Cancellation,
            _deadline: Deadline,
        ) -> Result<(), FilesystemError> {
            Err(FilesystemError)
        }

        async fn create_symlink(
            &self,
            _segments: &[String],
            _target: &[String],
            _cancel: &Cancellation,
            _deadline: Deadline,
        ) -> Result<(), FilesystemError> {
            Err(FilesystemError)
        }

        async fn readdir(
            &self,
            _segments: &[String],
            _cancel: &Cancellation,
            _deadline: Deadline,
        ) -> Result<Vec<String>, FilesystemError> {
            Err(FilesystemError)
        }

        async fn truncate(
            &self,
            _segments: &[String],
            _len: u64,
            _cancel: &Cancellation,
            _deadline: Deadline,
        ) -> Result<(), FilesystemError> {
            Err(FilesystemError)
        }
    }

    #[derive(Debug)]
    struct FakeFilesystemBroker;

    #[async_trait]
    impl FilesystemBroker for FakeFilesystemBroker {
        async fn open(
            &self,
            mount: &MountName,
            rights: FilesystemRights,
            _grant: &CapabilityGrant,
            _cancel: &Cancellation,
            _deadline: Deadline,
        ) -> Result<Box<dyn FilesystemHandle>, FilesystemError> {
            Ok(Box::new(FakeFilesystemHandle {
                mount: mount.clone(),
                rights,
            }))
        }
    }

    #[tokio::test]
    async fn every_filesystem_operation_is_relative_to_an_already_opened_handle() {
        // The only two entry points in this whole test are `open` (which
        // takes a MountName, never a host path) and read/write/etc, which
        // take `&[String]` segments relative to `self`. There is no method
        // anywhere in this API that accepts a single absolute path string
        // as authority.
        let broker = FakeFilesystemBroker;
        let handle = broker
            .open(
                &MountName::new("workspace"),
                FilesystemRights::single(FilesystemRight::Read),
                &test_grant(),
                &Cancellation::new(),
                Deadline::never(),
            )
            .await
            .unwrap();

        let content = handle
            .read(
                &["generated".to_owned(), "out.txt".to_owned()],
                &Cancellation::new(),
                Deadline::never(),
            )
            .await
            .unwrap();
        assert_eq!(content, b"generated/out.txt");

        // Absent rights deny even a structurally valid, relative operation.
        assert_eq!(
            handle
                .write(
                    &["x".to_owned()],
                    b"y",
                    &Cancellation::new(),
                    Deadline::never()
                )
                .await,
            Err(FilesystemError)
        );
    }

    fn test_grant() -> CapabilityGrant {
        use crate::ids::{SessionId, TenantId};
        use crate::security::{
            CheckSetRevision, PermissionSet, SecurityAction, SecurityContext, SecurityResource,
            SecuritySubject,
        };
        use agent_runtime_registry::Permission;

        let context = SecurityContext::new(
            SecuritySubject::new("user-1"),
            SessionId::new("s-1"),
            TenantId::new("tenant-1"),
            CheckSetRevision::new("cs-1"),
        );
        CapabilityGrant::issue(
            &context,
            SecurityAction::new("fs.open"),
            SecurityResource::filesystem("workspace", vec![]),
            PermissionSet::single(Permission::FsRead),
            crate::grant::PolicyEpoch::new(CheckSetRevision::new("cs-1")),
            Deadline::never(),
            1,
        )
    }
}
