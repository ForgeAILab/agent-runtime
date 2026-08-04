//! A replay HTTP transport that emits recorded SSE bytes offline.

use std::sync::Mutex;

use async_stream::stream;
use async_trait::async_trait;

use agent_runtime::provider::transport::{ByteStream, HttpRequest, HttpResponse, HttpTransport};
use agent_runtime_core::provider::ProviderError;

/// A transport that replays a fixed sequence of response byte chunks, ignoring
/// the request. It records the requests it received for assertions.
///
/// Response headers are opt-in via [`ReplayTransport::with_headers`]. A replay
/// built without them reports none, which is the honest fixture default: a
/// recorded body says nothing about what the server reported about limits.
#[derive(Debug)]
pub struct ReplayTransport {
    chunks: Vec<Vec<u8>>,
    headers: Vec<(String, String)>,
    requests: Mutex<Vec<HttpRequest>>,
}

impl ReplayTransport {
    /// A transport that replays `chunks` (each a raw SSE byte slice).
    pub fn new(chunks: impl IntoIterator<Item = impl Into<Vec<u8>>>) -> Self {
        Self {
            chunks: chunks.into_iter().map(Into::into).collect(),
            headers: Vec::new(),
            requests: Mutex::new(Vec::new()),
        }
    }

    /// A transport that replays a single SSE body.
    pub fn single(body: impl Into<Vec<u8>>) -> Self {
        Self::new(vec![body.into()])
    }

    /// Replays `headers` alongside the body.
    #[must_use]
    pub fn with_headers(
        mut self,
        headers: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> Self {
        self.headers = headers
            .into_iter()
            .map(|(name, value)| (name.into(), value.into()))
            .collect();
        self
    }

    /// The requests received so far.
    pub fn requests(&self) -> Vec<HttpRequest> {
        self.requests.lock().expect("requests poisoned").clone()
    }

    fn replay(&self, request: HttpRequest) -> ByteStream {
        self.requests
            .lock()
            .expect("requests poisoned")
            .push(request);
        let chunks = self.chunks.clone();
        Box::pin(stream! {
            for chunk in chunks {
                yield Ok(chunk);
            }
        })
    }
}

#[async_trait]
impl HttpTransport for ReplayTransport {
    async fn post_stream(&self, request: HttpRequest) -> Result<ByteStream, ProviderError> {
        Ok(self.replay(request))
    }

    async fn post_response(&self, request: HttpRequest) -> Result<HttpResponse, ProviderError> {
        Ok(HttpResponse {
            status: 200,
            headers: self.headers.clone(),
            body: self.replay(request),
        })
    }
}
