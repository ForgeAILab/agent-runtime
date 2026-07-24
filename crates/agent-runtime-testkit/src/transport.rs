//! A replay HTTP transport that emits recorded SSE bytes offline.

use std::sync::Mutex;

use async_stream::stream;
use async_trait::async_trait;

use agent_runtime::provider::transport::{ByteStream, HttpRequest, HttpTransport};
use agent_runtime_core::provider::ProviderError;

/// A transport that replays a fixed sequence of response byte chunks, ignoring
/// the request. It records the requests it received for assertions.
#[derive(Debug)]
pub struct ReplayTransport {
    chunks: Vec<Vec<u8>>,
    requests: Mutex<Vec<HttpRequest>>,
}

impl ReplayTransport {
    /// A transport that replays `chunks` (each a raw SSE byte slice).
    pub fn new(chunks: impl IntoIterator<Item = impl Into<Vec<u8>>>) -> Self {
        Self {
            chunks: chunks.into_iter().map(Into::into).collect(),
            requests: Mutex::new(Vec::new()),
        }
    }

    /// A transport that replays a single SSE body.
    pub fn single(body: impl Into<Vec<u8>>) -> Self {
        Self::new(vec![body.into()])
    }

    /// The requests received so far.
    pub fn requests(&self) -> Vec<HttpRequest> {
        self.requests.lock().expect("requests poisoned").clone()
    }
}

#[async_trait]
impl HttpTransport for ReplayTransport {
    async fn post_stream(&self, request: HttpRequest) -> Result<ByteStream, ProviderError> {
        self.requests
            .lock()
            .expect("requests poisoned")
            .push(request);
        let chunks = self.chunks.clone();
        let out = stream! {
            for chunk in chunks {
                yield Ok(chunk);
            }
        };
        Ok(Box::pin(out))
    }
}
