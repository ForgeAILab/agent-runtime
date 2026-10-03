#![cfg(feature = "reqwest-transport")]

use std::time::Duration;

use agent_runtime_provider::core::provider::ProviderErrorKind;
use agent_runtime_provider::transport::{HttpRequest, HttpTransport};
use agent_runtime_provider::{DestinationPolicy, ReqwestTransport};
use futures_util::StreamExt;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::time::timeout;

const TEST_TIMEOUT: Duration = Duration::from_secs(5);

async fn bind() -> (TcpListener, String) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/v1", listener.local_addr().unwrap());
    (listener, url)
}

// Empty request bodies keep the fixture protocol small; production bodies are
// still streamed through reqwest's normal POST implementation.
async fn read_request(socket: &mut TcpStream) -> String {
    let mut request = Vec::new();
    while !request.ends_with(b"\r\n\r\n") {
        assert!(request.len() < 16 * 1024);
        request.push(socket.read_u8().await.unwrap());
    }
    String::from_utf8(request).unwrap()
}

fn request(url: String) -> HttpRequest {
    HttpRequest {
        url,
        headers: vec![("authorization".into(), "Bearer request-secret".into())],
        body: Vec::new(),
    }
}

#[tokio::test]
async fn loopback_streams_before_the_server_finishes_and_returns_headers() {
    let (listener, url) = bind().await;
    let (release, released) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let incoming = read_request(&mut socket).await;
        assert!(incoming.starts_with("POST /v1 HTTP/1.1\r\n"));
        assert!(incoming.contains("authorization: Bearer request-secret\r\n"));
        socket.write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nX-RateLimit-Remaining: 7\r\nX-Echo: header-secret\r\n\r\n5\r\nhello\r\n").await.unwrap();
        released.await.unwrap();
        socket.write_all(b"5\r\nworld\r\n0\r\n\r\n").await.unwrap();
    });
    let transport = ReqwestTransport::new(DestinationPolicy::Loopback)
        .with_origin(&url)
        .unwrap();
    let mut response = timeout(TEST_TIMEOUT, transport.post_response(request(url)))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response.status, 200);
    assert_eq!(response.header("x-ratelimit-remaining"), Some("7"));
    assert!(!format!("{response:?}").contains("header-secret"));
    let first = timeout(TEST_TIMEOUT, response.body.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(first, b"hello");
    release.send(()).unwrap();
    let mut rest = Vec::new();
    while let Some(chunk) = timeout(TEST_TIMEOUT, response.body.next()).await.unwrap() {
        rest.extend(chunk.unwrap());
    }
    assert_eq!(rest, b"world");
    timeout(TEST_TIMEOUT, server).await.unwrap().unwrap();
}

#[tokio::test]
async fn localhost_uses_the_checked_dns_addresses() {
    let (listener, url) = bind().await;
    let url = url.replace("127.0.0.1", "localhost");
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_request(&mut socket).await;
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
            .await
            .unwrap();
    });
    let mut body = timeout(
        TEST_TIMEOUT,
        ReqwestTransport::new(DestinationPolicy::Loopback).post_stream(request(url)),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        timeout(TEST_TIMEOUT, body.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        b"ok"
    );
    timeout(TEST_TIMEOUT, server).await.unwrap().unwrap();
}

#[tokio::test]
async fn loopback_rejects_redirects_without_following_or_exposing_credentials() {
    let (listener, url) = bind().await;
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_request(&mut socket).await;
        socket.write_all(b"HTTP/1.1 302 Found\r\nLocation: http://10.0.0.1/?key=location-secret\r\nContent-Length: 11\r\n\r\nbody-secret").await.unwrap();
    });
    let transport = ReqwestTransport::new(DestinationPolicy::Loopback);
    let error = timeout(TEST_TIMEOUT, transport.post_response(request(url)))
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(error.kind, ProviderErrorKind::Network);
    assert_eq!(error.message, "provider redirect rejected");
    assert!(!format!("{error:?} {error}").contains("secret"));
    timeout(TEST_TIMEOUT, server).await.unwrap().unwrap();
}

#[tokio::test]
async fn oversized_error_body_stops_at_the_cap_without_waiting_for_eof() {
    let (listener, url) = bind().await;
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_request(&mut socket).await;
        socket
            .write_all(b"HTTP/1.1 503 Unavailable\r\nContent-Length: 999999\r\n\r\n")
            .await
            .unwrap();
        socket.write_all(&vec![b'a'; 8 * 1024]).await.unwrap();
        // Still no EOF: the client must stop reading at its 8 KiB cap.
        assert_eq!(
            timeout(TEST_TIMEOUT, socket.read(&mut [0]))
                .await
                .unwrap()
                .unwrap(),
            0
        );
    });
    let error = timeout(
        TEST_TIMEOUT,
        ReqwestTransport::new(DestinationPolicy::Loopback).post_response(request(url)),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert_eq!(error.kind, ProviderErrorKind::Server);
    assert!(error.retryable);
    timeout(TEST_TIMEOUT, server).await.unwrap().unwrap();
}

#[tokio::test]
async fn dropping_the_body_cancels_the_open_response() {
    let (listener, url) = bind().await;
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_request(&mut socket).await;
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n")
            .await
            .unwrap();
        assert_eq!(
            timeout(TEST_TIMEOUT, socket.read(&mut [0]))
                .await
                .unwrap()
                .unwrap(),
            0
        );
    });
    let mut body = timeout(
        TEST_TIMEOUT,
        ReqwestTransport::new(DestinationPolicy::Loopback).post_stream(request(url)),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        timeout(TEST_TIMEOUT, body.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        b"hello"
    );
    drop(body);
    timeout(TEST_TIMEOUT, server).await.unwrap().unwrap();
}

#[tokio::test]
async fn dropping_a_pending_request_cancels_before_response_headers() {
    let (listener, url) = bind().await;
    let (received, ready) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_request(&mut socket).await;
        received.send(()).unwrap();
        assert_eq!(
            timeout(TEST_TIMEOUT, socket.read(&mut [0]))
                .await
                .unwrap()
                .unwrap(),
            0
        );
    });
    let transport = ReqwestTransport::new(DestinationPolicy::Loopback);
    {
        let pending = transport.post_response(request(url));
        tokio::pin!(pending);
        tokio::select! {
            _ = timeout(TEST_TIMEOUT, ready) => {}
            _ = &mut pending => panic!("server has not sent a response"),
        }
        // Dropping the scoped future must release the request connection.
    }
    timeout(TEST_TIMEOUT, server).await.unwrap().unwrap();
}

#[tokio::test]
async fn invalid_request_header_errors_do_not_echo_the_key_or_url() {
    let error = ReqwestTransport::new(DestinationPolicy::Loopback)
        .post_response(HttpRequest {
            url: "http://127.0.0.1:1/secret-path?key=url-secret".into(),
            headers: vec![("authorization".into(), "key-secret\ninvalid".into())],
            body: b"body-secret".to_vec(),
        })
        .await
        .unwrap_err();
    assert_eq!(error.kind, ProviderErrorKind::BadRequest);
    assert!(!format!("{error:?} {error}").contains("secret"));
}
