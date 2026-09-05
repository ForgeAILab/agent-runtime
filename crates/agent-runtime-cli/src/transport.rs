//! Restrictive Reqwest transport owned by the CLI host.

use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use agent_runtime::core::provider::{ProviderError, ProviderErrorKind};
use agent_runtime::provider::transport::{ByteStream, HttpRequest, HttpResponse, HttpTransport};
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
    fn parse(url: &str) -> Result<Self, ProviderError> {
        let parsed = parse_provider_url(url)?;
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

/// CLI-owned HTTPS transport with exact-origin and restricted-address checks.
#[derive(Clone)]
pub struct CliHttpTransport {
    allowed_origin: AllowedOrigin,
    resolver: Arc<dyn DnsResolver>,
}

impl fmt::Debug for CliHttpTransport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CliHttpTransport")
            .field("allowed_origin", &self.allowed_origin)
            .finish_non_exhaustive()
    }
}

impl CliHttpTransport {
    /// Binds a transport to the exact origin of `base_url`.
    pub fn new(base_url: &str) -> Result<Self, ProviderError> {
        Ok(Self {
            allowed_origin: AllowedOrigin::parse(base_url)?,
            resolver: Arc::new(SystemDnsResolver),
        })
    }

    #[cfg(test)]
    fn with_resolver(
        base_url: &str,
        resolver: Arc<dyn DnsResolver>,
    ) -> Result<Self, ProviderError> {
        Ok(Self {
            allowed_origin: AllowedOrigin::parse(base_url)?,
            resolver,
        })
    }

    async fn validated_target(
        &self,
        request_url: &str,
    ) -> Result<(Url, String, Vec<SocketAddr>), ProviderError> {
        let url = parse_provider_url(request_url)?;
        let origin = AllowedOrigin::from_url(&url)?;
        if origin != self.allowed_origin {
            return Err(ProviderError::new(
                ProviderErrorKind::Network,
                "provider request origin is not authorized",
            ));
        }

        let host = url.host_str().ok_or_else(rejected_url)?.to_owned();
        let port = url.port_or_known_default().ok_or_else(rejected_url)?;
        let addresses = match url.host() {
            Some(Host::Domain(domain)) => {
                if restricted_provider_hostname(domain) {
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
                .any(|address| restricted_provider_ip(address.ip()))
        {
            return Err(rejected_destination());
        }

        Ok((url, host, addresses))
    }

    async fn send(&self, request: HttpRequest) -> Result<HttpResponse, ProviderError> {
        let (url, host, addresses) = self.validated_target(&request.url).await?;
        let client = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
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

        let headers = response
            .headers()
            .iter()
            .take(MAX_RESPONSE_HEADERS)
            .filter_map(|(name, value)| {
                let value = value.to_str().ok()?;
                (value.len() <= MAX_RESPONSE_HEADER_VALUE_BYTES)
                    .then(|| (name.as_str().to_owned(), value.to_owned()))
            })
            .collect();
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
impl HttpTransport for CliHttpTransport {
    async fn post_stream(&self, request: HttpRequest) -> Result<ByteStream, ProviderError> {
        Ok(self.send(request).await?.body)
    }

    async fn post_response(&self, request: HttpRequest) -> Result<HttpResponse, ProviderError> {
        self.send(request).await
    }
}

fn parse_provider_url(url: &str) -> Result<Url, ProviderError> {
    let parsed = Url::parse(url).map_err(|_| rejected_url())?;
    if parsed.scheme() != "https"
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.fragment().is_some()
        || parsed.host().is_none()
    {
        return Err(rejected_url());
    }
    match parsed.host() {
        Some(Host::Domain(domain)) if restricted_provider_hostname(domain) => {
            return Err(rejected_destination());
        }
        Some(Host::Ipv4(address)) if restricted_ipv4(address) => {
            return Err(rejected_destination());
        }
        Some(Host::Ipv6(address)) if restricted_ipv6(address) => {
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
            assert!(CliHttpTransport::new(url).is_err(), "url={url}");
        }
        assert!(CliHttpTransport::new("https://api.example.com/v1").is_ok());
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
        let transport =
            CliHttpTransport::with_resolver("https://api.example.com/v1", resolver).unwrap();

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
        let transport =
            CliHttpTransport::with_resolver("https://api.example.com/v1", resolver).unwrap();
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
}
