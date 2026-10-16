//! Policy-explicit reqwest implementation of [`HttpTransport`].
//!
//! Enabled by the off-by-default `reqwest-transport` feature. Hosts choose
//! destination authority explicitly; no proxy or redirect can bypass it.

use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use crate::transport::{ByteStream, HttpRequest, HttpResponse, HttpTransport};
use agent_runtime_core::provider::{ProviderError, ProviderErrorKind};
use async_trait::async_trait;
use futures_util::StreamExt;
use url::{Host, Url};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_ERROR_BODY_BYTES: usize = 8 * 1024;
const MAX_RESPONSE_HEADERS: usize = 128;
const MAX_RESPONSE_HEADER_VALUE_BYTES: usize = 8 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
struct AllowedOrigin {
    scheme: String,
    host: String,
    port: u16,
}

impl AllowedOrigin {
    fn parse(url: &str, policy: DestinationPolicy) -> Result<Self, ProviderError> {
        let parsed = parse_provider_url(url, policy)?;
        Self::from_url(&parsed)
    }

    fn from_url(url: &Url) -> Result<Self, ProviderError> {
        let host = url.host_str().ok_or_else(rejected_url)?;
        let port = url.port_or_known_default().ok_or_else(rejected_url)?;
        Ok(Self {
            scheme: url.scheme().to_owned(),
            host: host.trim_end_matches('.').to_ascii_lowercase(),
            port,
        })
    }
}

#[async_trait]
trait DnsResolver: Send + Sync + fmt::Debug {
    async fn resolve(&self, host: &str, port: u16) -> Result<Vec<SocketAddr>, ProviderError>;
}

#[derive(Debug)]
struct SystemDnsResolver;

#[async_trait]
impl DnsResolver for SystemDnsResolver {
    async fn resolve(&self, host: &str, port: u16) -> Result<Vec<SocketAddr>, ProviderError> {
        tokio::net::lookup_host((host, port))
            .await
            .map(|addresses| addresses.collect())
            .map_err(|_| network_error("provider DNS resolution failed"))
    }
}

/// Destination authority chosen explicitly by the embedding host.
///
/// Every resolved address must satisfy the policy. Every variant disables
/// proxies and redirects and rejects URL userinfo and fragments.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum DestinationPolicy {
    /// HTTPS only, denying the CLI's restricted hostname and IP classes.
    PublicHttps,
    /// HTTP or HTTPS to 127/8, ::1, or localhost only.
    ///
    /// DNS answers for localhost must all be loopback. Other DNS names and
    /// IPv4-mapped IPv6 addresses are rejected, even if they resolve locally.
    Loopback,
    /// HTTP or HTTPS to exactly the origin given to
    /// [`ReqwestTransport::with_origin`], whatever address class it resolves
    /// to: a self-hosted endpoint, such as a model server on the local
    /// network, that the user configured explicitly. Without an origin every
    /// request is rejected.
    ConfiguredOrigin,
}

impl DestinationPolicy {
    fn permits_ip(self, ip: IpAddr) -> bool {
        match self {
            Self::PublicHttps => !restricted_provider_ip(ip),
            Self::Loopback => ip.is_loopback(),
            Self::ConfiguredOrigin => true,
        }
    }

    fn permits_domain(self, domain: &str) -> bool {
        match self {
            Self::PublicHttps => !restricted_provider_hostname(domain),
            Self::Loopback => domain
                .trim_end_matches('.')
                .eq_ignore_ascii_case("localhost"),
            Self::ConfiguredOrigin => true,
        }
    }

    fn permits_http(self) -> bool {
        matches!(self, Self::Loopback | Self::ConfiguredOrigin)
    }
}

/// Streaming HTTP transport with required destination policy.
///
/// Construction does no I/O. Each request resolves, validates and pins every
/// DNS answer into a fresh client. Proxies and redirects are always disabled.
/// Successful bodies are streamed; error bodies are capped at 8 KiB and never
/// quoted in errors. Responses carry at most 128 headers, each at most 8 KiB.
///
/// Dropping the pending request future or returned [`ByteStream`] cancels its
/// I/O. There are no detached tasks, hidden retries, or total-request timeout;
/// the calling adapter owns cancellation and deadlines.
///
/// ```
/// use std::time::Duration;
/// use agent_runtime_provider::{DestinationPolicy, ReqwestTransport};
/// use agent_runtime_provider::transport::HttpTransport;
///
/// # fn example() -> Result<(), agent_runtime_provider::core::provider::ProviderError> {
/// let public = ReqwestTransport::new(DestinationPolicy::PublicHttps)
///     .with_origin("https://api.example.com/v1")?
///     .with_connect_timeout(Duration::from_secs(5));
/// let local = ReqwestTransport::new(DestinationPolicy::Loopback)
///     .with_origin("http://127.0.0.1:8080/v1")?;
/// let _: &dyn HttpTransport = &public;
/// let _: &dyn HttpTransport = &local;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct ReqwestTransport {
    policy: DestinationPolicy,
    allowed_origin: Option<AllowedOrigin>,
    connect_timeout: Duration,
    resolver: Arc<dyn DnsResolver>,
}

impl fmt::Debug for ReqwestTransport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReqwestTransport")
            .field("policy", &self.policy)
            .field("origin_pinned", &self.allowed_origin.is_some())
            .field("connect_timeout", &self.connect_timeout)
            .finish_non_exhaustive()
    }
}

impl ReqwestTransport {
    /// Creates a transport with explicit policy and a ten-second connect timeout.
    ///
    /// Without [`Self::with_origin`], any destination satisfying the selected
    /// policy is allowed. This constructor never resolves or connects.
    pub fn new(policy: DestinationPolicy) -> Self {
        Self {
            policy,
            allowed_origin: None,
            connect_timeout: CONNECT_TIMEOUT,
            resolver: Arc::new(SystemDnsResolver),
        }
    }

    /// Pins requests to this URL's scheme, normalized host and effective port.
    ///
    /// The URL is validated against the policy without DNS or network I/O.
    /// Path and query are not part of an origin; neither enters Debug or errors.
    pub fn with_origin(mut self, base_url: &str) -> Result<Self, ProviderError> {
        self.allowed_origin = Some(AllowedOrigin::parse(base_url, self.policy)?);
        Ok(self)
    }

    /// Sets the TCP/TLS connect timeout; caller-owned deadlines cover DNS/body I/O.
    #[must_use]
    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    async fn validated_target(
        &self,
        request_url: &str,
    ) -> Result<(Url, String, Vec<SocketAddr>), ProviderError> {
        let url = parse_provider_url(request_url, self.policy)?;
        let origin = AllowedOrigin::from_url(&url)?;
        let unauthorized = match self.allowed_origin.as_ref() {
            Some(allowed) => *allowed != origin,
            // This policy authorizes nothing but the configured origin.
            None => self.policy == DestinationPolicy::ConfiguredOrigin,
        };
        if unauthorized {
            return Err(ProviderError::new(
                ProviderErrorKind::Network,
                "provider request origin is not authorized",
            ));
        }

        let host = url.host_str().ok_or_else(rejected_url)?.to_owned();
        let port = url.port_or_known_default().ok_or_else(rejected_url)?;
        let addresses = match url.host() {
            Some(Host::Domain(domain)) => {
                if !self.policy.permits_domain(domain) {
                    return Err(rejected_destination());
                }
                self.resolver.resolve(domain, port).await?
            }
            Some(Host::Ipv4(address)) => vec![SocketAddr::new(IpAddr::V4(address), port)],
            Some(Host::Ipv6(address)) => vec![SocketAddr::new(IpAddr::V6(address), port)],
            None => return Err(rejected_url()),
        };

        if addresses.is_empty()
            || addresses
                .iter()
                .any(|address| !self.policy.permits_ip(address.ip()))
        {
            return Err(rejected_destination());
        }

        Ok((url, host, addresses))
    }

    async fn send(&self, request: HttpRequest) -> Result<HttpResponse, ProviderError> {
        let (url, host, addresses) = self.validated_target(&request.url).await?;
        let client = reqwest::Client::builder()
            .connect_timeout(self.connect_timeout)
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .resolve_to_addrs(&host, &addresses)
            .build()
            .map_err(|_| {
                ProviderError::new(
                    ProviderErrorKind::Network,
                    "provider HTTP client setup failed",
                )
            })?;

        let mut builder = client.post(url).body(request.body);
        for (name, value) in request.headers {
            builder = builder.header(name, value);
        }
        let response = builder.send().await.map_err(map_reqwest_error)?;
        let status = response.status().as_u16();
        if !(200..300).contains(&status) {
            let excerpt = read_bounded_error_body(response).await;
            return Err(classify_http_failure(status, &excerpt));
        }

        let headers = bounded_response_headers(response.headers());
        let body: ByteStream = Box::pin(response.bytes_stream().map(|chunk| {
            chunk
                .map(|bytes| bytes.to_vec())
                .map_err(|_| network_error("provider response stream failed"))
        }));
        Ok(HttpResponse {
            status,
            headers,
            body,
        })
    }
}

#[async_trait]
impl HttpTransport for ReqwestTransport {
    async fn post_stream(&self, request: HttpRequest) -> Result<ByteStream, ProviderError> {
        Ok(self.send(request).await?.body)
    }

    async fn post_response(&self, request: HttpRequest) -> Result<HttpResponse, ProviderError> {
        self.send(request).await
    }
}

fn bounded_response_headers(headers: &reqwest::header::HeaderMap) -> Vec<(String, String)> {
    headers
        .iter()
        .take(MAX_RESPONSE_HEADERS)
        .filter_map(|(name, value)| {
            let value = value.to_str().ok()?;
            (value.len() <= MAX_RESPONSE_HEADER_VALUE_BYTES)
                .then(|| (name.as_str().to_owned(), value.to_owned()))
        })
        .collect()
}

fn parse_provider_url(url: &str, policy: DestinationPolicy) -> Result<Url, ProviderError> {
    let invalid_url = || {
        if policy.permits_http() {
            ProviderError::new(
                ProviderErrorKind::Network,
                "provider URL rejected; an HTTP or HTTPS URL without userinfo or a fragment is required",
            )
        } else {
            rejected_url()
        }
    };
    let parsed = Url::parse(url).map_err(|_| invalid_url())?;
    if !(parsed.scheme() == "https" || (policy.permits_http() && parsed.scheme() == "http"))
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.fragment().is_some()
        || parsed.host().is_none()
    {
        return Err(invalid_url());
    }
    match parsed.host() {
        Some(Host::Domain(domain)) if !policy.permits_domain(domain) => {
            return Err(rejected_destination());
        }
        Some(Host::Ipv4(address)) if !policy.permits_ip(IpAddr::V4(address)) => {
            return Err(rejected_destination());
        }
        Some(Host::Ipv6(address)) if !policy.permits_ip(IpAddr::V6(address)) => {
            return Err(rejected_destination());
        }
        _ => {}
    }
    Ok(parsed)
}

fn rejected_url() -> ProviderError {
    ProviderError::new(
        ProviderErrorKind::Network,
        "provider URL rejected; an HTTPS URL without userinfo or a fragment is required",
    )
}

fn rejected_destination() -> ProviderError {
    ProviderError::new(
        ProviderErrorKind::Network,
        "provider destination is restricted",
    )
}

fn network_error(message: &'static str) -> ProviderError {
    ProviderError::new(ProviderErrorKind::Network, message).retryable()
}

fn map_reqwest_error(error: reqwest::Error) -> ProviderError {
    if error.is_timeout() {
        ProviderError::new(
            ProviderErrorKind::Timeout,
            "provider HTTP request timed out",
        )
        .retryable()
    } else if error.is_builder() {
        ProviderError::new(
            ProviderErrorKind::BadRequest,
            "provider HTTP request was invalid",
        )
    } else {
        network_error("provider HTTP request failed")
    }
}

async fn read_bounded_error_body(response: reqwest::Response) -> Vec<u8> {
    let mut body = Vec::new();
    let mut stream = response.bytes_stream();
    while body.len() < MAX_ERROR_BODY_BYTES {
        let Some(chunk) = stream.next().await else {
            break;
        };
        let Ok(chunk) = chunk else {
            break;
        };
        append_bounded(&mut body, &chunk);
    }
    body
}

fn append_bounded(body: &mut Vec<u8>, chunk: &[u8]) {
    let remaining = MAX_ERROR_BODY_BYTES.saturating_sub(body.len());
    body.extend_from_slice(&chunk[..chunk.len().min(remaining)]);
}

fn classify_http_failure(status: u16, body: &[u8]) -> ProviderError {
    match status {
        300..=399 => ProviderError::new(ProviderErrorKind::Network, "provider redirect rejected"),
        401 | 403 => ProviderError::new(ProviderErrorKind::Auth, "provider authentication failed"),
        429 if usage_limit_reached(body) => ProviderError::new(
            ProviderErrorKind::LimitExhausted,
            "provider usage limit reached",
        ),
        429 => ProviderError::new(
            ProviderErrorKind::RateLimited,
            "provider rate limit reached",
        )
        .retryable(),
        400..=499 => ProviderError::new(
            ProviderErrorKind::BadRequest,
            format!("provider rejected the request with HTTP {status}"),
        ),
        500..=599 => ProviderError::new(
            ProviderErrorKind::Server,
            format!("provider returned HTTP {status}"),
        )
        .retryable(),
        _ => ProviderError::new(
            ProviderErrorKind::Network,
            format!("provider returned unexpected HTTP {status}"),
        ),
    }
}

fn usage_limit_reached(body: &[u8]) -> bool {
    serde_json::from_slice::<serde_json::Value>(body)
        .ok()
        .and_then(|value| value.get("error").cloned())
        .is_some_and(|error| {
            error.get("type").and_then(serde_json::Value::as_str) == Some("usage_limit_reached")
                || error.get("resets_in_seconds").is_some()
        })
}

fn restricted_provider_hostname(host: &str) -> bool {
    let host = host.trim_end_matches('.').to_ascii_lowercase();
    host == "localhost"
        || host.ends_with(".localhost")
        || host.ends_with(".local")
        || host.ends_with(".internal")
        || host.ends_with(".home.arpa")
}

fn restricted_provider_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(address) => restricted_ipv4(address),
        IpAddr::V6(address) => restricted_ipv6(address),
    }
}

fn restricted_ipv4(address: Ipv4Addr) -> bool {
    let octets = address.octets();
    address.is_private()
        || address.is_loopback()
        || address.is_link_local()
        || address.is_unspecified()
        || address.is_broadcast()
        || (octets[0] == 100 && (64..=127).contains(&octets[1]))
        || (octets[0] == 192 && octets[1] == 0 && octets[2] == 0)
        || (octets[0] == 192 && octets[1] == 0 && octets[2] == 2)
        || (octets[0] == 198 && (18..=19).contains(&octets[1]))
        || (octets[0] == 198 && octets[1] == 51 && octets[2] == 100)
        || (octets[0] == 203 && octets[1] == 0 && octets[2] == 113)
        || octets[0] >= 224
}

fn restricted_ipv6(address: Ipv6Addr) -> bool {
    let segments = address.segments();
    address.is_loopback()
        || address.is_unspecified()
        || address.is_multicast()
        || (segments[0] & 0xffc0) == 0xfe80
        || (segments[0] & 0xffc0) == 0xfec0
        || (segments[0] & 0xfe00) == 0xfc00
        || (segments[0] == 0x2001 && segments[1] == 0x0db8)
        || address.to_ipv4().is_some_and(restricted_ipv4)
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use super::*;

    #[derive(Debug)]
    struct ScriptedResolver {
        answers: Mutex<VecDeque<Vec<SocketAddr>>>,
    }

    impl ScriptedResolver {
        fn new(answers: impl IntoIterator<Item = Vec<SocketAddr>>) -> Self {
            Self {
                answers: Mutex::new(answers.into_iter().collect()),
            }
        }
    }

    #[async_trait]
    impl DnsResolver for ScriptedResolver {
        async fn resolve(&self, _host: &str, _port: u16) -> Result<Vec<SocketAddr>, ProviderError> {
            self.answers
                .lock()
                .expect("resolver answers poisoned")
                .pop_front()
                .ok_or_else(|| network_error("test resolver exhausted"))
        }
    }

    #[test]
    fn base_url_rejects_unsafe_shapes() {
        for url in [
            "http://api.example.com/v1",
            "https://user:secret@api.example.com/v1",
            "https://api.example.com/v1#fragment",
            "https://localhost/v1",
            "https://service.internal/v1",
            "https://127.0.0.1/v1",
            "not a url",
        ] {
            assert!(
                ReqwestTransport::new(DestinationPolicy::PublicHttps)
                    .with_origin(url)
                    .is_err(),
                "url={url}"
            );
        }
        assert!(
            ReqwestTransport::new(DestinationPolicy::PublicHttps)
                .with_origin("https://api.example.com/v1")
                .is_ok()
        );
    }

    #[test]
    fn restricted_address_classes_include_mapped_ipv4() {
        for ip in [
            "127.0.0.1",
            "10.0.0.1",
            "169.254.1.1",
            "192.0.2.1",
            "224.0.0.1",
            "::1",
            "fe80::1",
            "fc00::1",
            "2001:db8::1",
            "::ffff:127.0.0.1",
        ] {
            assert!(
                restricted_provider_ip(ip.parse().expect("valid test IP")),
                "ip={ip}"
            );
        }
        assert!(!restricted_provider_ip("8.8.8.8".parse().unwrap()));
        assert!(!restricted_provider_ip(
            "2606:4700:4700::1111".parse().unwrap()
        ));
    }

    #[tokio::test]
    async fn every_request_is_resolved_and_rebinding_is_denied() {
        let resolver = Arc::new(ScriptedResolver::new([
            vec!["8.8.8.8:443".parse().unwrap()],
            vec!["127.0.0.1:443".parse().unwrap()],
        ]));
        let transport = ReqwestTransport {
            resolver,
            ..ReqwestTransport::new(DestinationPolicy::PublicHttps)
                .with_origin("https://api.example.com/v1")
                .unwrap()
        };

        transport
            .validated_target("https://api.example.com/v1/responses")
            .await
            .expect("first public answer is accepted");
        let error = transport
            .validated_target("https://api.example.com/v1/responses")
            .await
            .expect_err("second private answer is rejected");
        assert_eq!(error.message, "provider destination is restricted");
    }

    #[tokio::test]
    async fn exact_origin_is_enforced_before_dns() {
        let resolver = Arc::new(ScriptedResolver::new([vec![
            "8.8.8.8:443".parse().unwrap(),
        ]]));
        let transport = ReqwestTransport {
            resolver,
            ..ReqwestTransport::new(DestinationPolicy::PublicHttps)
                .with_origin("https://api.example.com/v1")
                .unwrap()
        };
        let error = transport
            .validated_target("https://other.example.com/v1/responses")
            .await
            .expect_err("other origin must fail");
        assert_eq!(error.message, "provider request origin is not authorized");
    }

    #[test]
    fn redirect_and_status_errors_are_typed_and_redacted() {
        let redirect = classify_http_failure(302, b"Bearer very-secret");
        assert_eq!(redirect.kind, ProviderErrorKind::Network);
        assert_eq!(redirect.message, "provider redirect rejected");
        assert!(!redirect.to_string().contains("very-secret"));

        let auth = classify_http_failure(401, b"very-secret");
        assert_eq!(auth.kind, ProviderErrorKind::Auth);
        assert!(!auth.to_string().contains("very-secret"));

        let exhausted = classify_http_failure(
            429,
            br#"{"error":{"type":"usage_limit_reached","resets_in_seconds":3600}}"#,
        );
        assert_eq!(exhausted.kind, ProviderErrorKind::LimitExhausted);
        assert!(!exhausted.retryable);

        let server = classify_http_failure(503, b"unavailable");
        assert_eq!(server.kind, ProviderErrorKind::Server);
        assert!(server.retryable);
    }

    #[test]
    fn error_body_collection_is_strictly_bounded() {
        let mut body = vec![b'a'; MAX_ERROR_BODY_BYTES - 2];
        append_bounded(&mut body, b"secret material beyond cap");
        assert_eq!(body.len(), MAX_ERROR_BODY_BYTES);
        assert!(!String::from_utf8_lossy(&body).contains("secret"));
        append_bounded(&mut body, b"more");
        assert_eq!(body.len(), MAX_ERROR_BODY_BYTES);
    }

    #[tokio::test]
    async fn loopback_accepts_only_local_names_and_literals() {
        let transport = ReqwestTransport::new(DestinationPolicy::Loopback);
        for url in [
            "http://127.0.0.1:8080/v1",
            "https://127.255.255.254/v1",
            "http://[::1]:8080/v1",
            "https://[::1]/v1",
        ] {
            transport.validated_target(url).await.expect(url);
        }
        for url in [
            "http://10.0.0.1/v1",
            "https://192.168.1.1/v1",
            "http://8.8.8.8/v1",
            "https://api.example.com/v1",
            "http://local-model.example/v1",
            "http://service.localhost/v1",
            "http://[::ffff:127.0.0.1]/v1",
            "http://[::]/v1",
            "http://169.254.1.1/v1",
            "ftp://localhost/v1",
            "http://secret@localhost/v1",
            "http://localhost/v1#secret",
        ] {
            assert!(transport.validated_target(url).await.is_err(), "url={url}");
        }
        for url in ["http://localhost:8080/v1", "https://LOCALHOST./v1"] {
            let transport = ReqwestTransport {
                resolver: Arc::new(ScriptedResolver::new([vec![
                    "127.0.0.1:8080".parse().unwrap(),
                    "[::1]:8080".parse().unwrap(),
                ]])),
                ..ReqwestTransport::new(DestinationPolicy::Loopback)
            };
            transport.validated_target(url).await.expect(url);
        }
    }

    #[tokio::test]
    async fn all_dns_answers_must_satisfy_policy() {
        for policy in [DestinationPolicy::Loopback, DestinationPolicy::PublicHttps] {
            let url = match policy {
                DestinationPolicy::Loopback => "http://localhost/v1",
                DestinationPolicy::PublicHttps => "https://api.example.com/v1",
                DestinationPolicy::ConfiguredOrigin => unreachable!(),
            };
            for answers in [
                vec![],
                vec!["127.0.0.1:443", "8.8.8.8:443"],
                vec!["8.8.8.8:443", "192.168.1.1:443"],
                vec!["10.0.0.1:443"],
            ] {
                let transport = ReqwestTransport {
                    resolver: Arc::new(ScriptedResolver::new([answers
                        .into_iter()
                        .map(|address| address.parse().unwrap())
                        .collect()])),
                    ..ReqwestTransport::new(policy)
                };
                let error = transport.validated_target(url).await.unwrap_err();
                assert_eq!(error.message, "provider destination is restricted");
            }
        }
    }

    #[tokio::test]
    async fn configured_origin_allows_only_that_origin_on_any_address_class() {
        let unpinned = ReqwestTransport::new(DestinationPolicy::ConfiguredOrigin);
        let error = unpinned
            .validated_target("http://192.168.1.5:1234/v1")
            .await
            .unwrap_err();
        assert_eq!(error.message, "provider request origin is not authorized");

        let lan = ReqwestTransport::new(DestinationPolicy::ConfiguredOrigin)
            .with_origin("http://192.168.1.5:1234/v1")
            .unwrap();
        lan.validated_target("http://192.168.1.5:1234/v1/chat/completions")
            .await
            .expect("configured private origin");
        for url in [
            "http://192.168.1.5:1235/v1",
            "https://192.168.1.5:1234/v1",
            "http://192.168.1.6:1234/v1",
            "http://secret@192.168.1.5:1234/v1",
            "ftp://192.168.1.5:1234/v1",
        ] {
            assert!(lan.validated_target(url).await.is_err(), "url={url}");
        }

        let named = ReqwestTransport {
            resolver: Arc::new(ScriptedResolver::new([vec![
                "10.0.0.7:8080".parse().unwrap(),
            ]])),
            ..ReqwestTransport::new(DestinationPolicy::ConfiguredOrigin)
                .with_origin("http://models.lan:8080")
                .unwrap()
        };
        named
            .validated_target("http://models.lan:8080/v1/chat/completions")
            .await
            .expect("configured named origin");
    }

    #[tokio::test]
    async fn localhost_rebinding_is_denied_and_other_dns_names_never_resolve() {
        let resolver = Arc::new(ScriptedResolver::new([
            vec!["127.0.0.1:80".parse().unwrap()],
            vec!["8.8.8.8:80".parse().unwrap()],
        ]));
        let transport = ReqwestTransport {
            resolver: resolver.clone(),
            ..ReqwestTransport::new(DestinationPolicy::Loopback)
        };
        assert!(
            transport
                .validated_target("http://model.example/v1")
                .await
                .is_err()
        );
        assert_eq!(resolver.answers.lock().unwrap().len(), 2);
        transport
            .validated_target("http://localhost/v1")
            .await
            .unwrap();
        assert!(
            transport
                .validated_target("http://localhost/v1")
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn loopback_origin_pin_checks_scheme_host_and_port_before_dns() {
        let resolver = Arc::new(ScriptedResolver::new([vec![
            "127.0.0.1:80".parse().unwrap(),
        ]]));
        let transport = ReqwestTransport {
            resolver: resolver.clone(),
            ..ReqwestTransport::new(DestinationPolicy::Loopback)
                .with_origin("http://LOCALHOST.:80/base?key=secret")
                .unwrap()
        };
        for url in [
            "https://localhost/v1",
            "http://localhost:8080/v1",
            "http://127.0.0.1/v1",
        ] {
            let error = transport.validated_target(url).await.unwrap_err();
            assert_eq!(error.message, "provider request origin is not authorized");
        }
        assert_eq!(resolver.answers.lock().unwrap().len(), 1);
        transport
            .validated_target("http://localhost/v1?key=another")
            .await
            .unwrap();
    }

    #[test]
    fn configuration_debug_and_errors_omit_url_secrets() {
        let transport = ReqwestTransport::new(DestinationPolicy::Loopback)
            .with_origin("http://localhost:8080/secret-path?key=query-secret")
            .unwrap()
            .with_connect_timeout(Duration::from_millis(123));
        assert_eq!(transport.connect_timeout, Duration::from_millis(123));
        let debug = format!("{transport:?}");
        assert!(!debug.contains("secret"));
        assert!(!debug.contains("localhost"));
        let error = ReqwestTransport::new(DestinationPolicy::PublicHttps)
            .with_origin("https://user:user-secret@api.example.com/?key=query-secret")
            .unwrap_err();
        assert!(!format!("{error:?} {error}").contains("secret"));
    }

    #[test]
    fn returned_headers_are_bounded_and_values_are_redacted() {
        use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
        let mut headers = HeaderMap::new();
        for index in 0..MAX_RESPONSE_HEADERS + 10 {
            headers.insert(
                HeaderName::from_bytes(format!("x-test-{index}").as_bytes()).unwrap(),
                HeaderValue::from_static("header-secret"),
            );
        }
        assert_eq!(
            bounded_response_headers(&headers).len(),
            MAX_RESPONSE_HEADERS
        );
        headers.clear();
        headers.insert(
            "x-too-large",
            HeaderValue::from_bytes(&vec![b'a'; MAX_RESPONSE_HEADER_VALUE_BYTES + 1]).unwrap(),
        );
        headers.insert(
            "x-at-bound",
            HeaderValue::from_bytes(&vec![b'a'; MAX_RESPONSE_HEADER_VALUE_BYTES]).unwrap(),
        );
        headers.insert("x-invalid", HeaderValue::from_bytes(&[0xff]).unwrap());
        headers.insert("x-key", HeaderValue::from_static("header-secret"));
        let observed = bounded_response_headers(&headers);
        assert_eq!(observed.len(), 2);
        assert!(
            observed
                .iter()
                .all(|(_, value)| value.len() <= MAX_RESPONSE_HEADER_VALUE_BYTES)
        );
        let response = HttpResponse {
            status: 200,
            headers: observed,
            body: Box::pin(futures_util::stream::empty()),
        };
        assert!(!format!("{response:?}").contains("header-secret"));
    }
}
