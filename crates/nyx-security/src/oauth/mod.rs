use aes_gcm::aead::OsRng;
use aes_gcm::aead::rand_core::RngCore;
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use reqwest::Url;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::SecurityError;

pub mod store;
pub mod token;

pub use store::{FileOAuthProfileStore, OAuthProfileStore};
pub use token::{OAuthProfile, TokenSet, extract_account_id_from_jwt};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PkceState {
    pub code_verifier: String,
    pub code_challenge: String,
    pub state: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeviceCodeResponse {
    pub device_code: String,
    pub user_code: String,
    pub verification_uri: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verification_uri_complete: Option<String>,
    pub expires_in: u64,
    #[serde(default = "default_interval")]
    pub interval: u64,
}

fn default_interval() -> u64 {
    5
}

#[derive(Debug, Deserialize)]
struct OAuthTokenWire {
    access_token: String,
    #[serde(default)]
    refresh_token: Option<String>,
    #[serde(default)]
    id_token: Option<String>,
    #[serde(default)]
    expires_in: Option<u64>,
    #[serde(default)]
    token_type: Option<String>,
    #[serde(default)]
    scope: Option<String>,
}

impl OAuthTokenWire {
    fn into_token_set(self) -> TokenSet {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        TokenSet {
            access_token: self.access_token,
            refresh_token: self.refresh_token,
            id_token: self.id_token,
            expires_at: now + self.expires_in.unwrap_or(3600) as i64,
            token_type: self.token_type,
            scope: self.scope,
        }
    }
}

#[derive(Debug, Deserialize)]
struct OAuthErrorBody {
    #[serde(default)]
    error: String,
    #[serde(default)]
    error_description: Option<String>,
}

pub fn generate_pkce_state() -> PkceState {
    let mut verifier = [0_u8; 64];
    let mut csrf_state = [0_u8; 24];
    OsRng.fill_bytes(&mut verifier);
    OsRng.fill_bytes(&mut csrf_state);

    let code_verifier = URL_SAFE_NO_PAD.encode(verifier);
    let code_challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(code_verifier.as_bytes()));
    let state = URL_SAFE_NO_PAD.encode(csrf_state);

    PkceState {
        code_verifier,
        code_challenge,
        state,
    }
}

pub fn build_authorize_url(
    endpoint: &str,
    client_id: &str,
    redirect_uri: &str,
    scopes: &[&str],
    pkce: &PkceState,
    extra_params: Option<&HashMap<String, String>>,
) -> Result<String, SecurityError> {
    let mut url =
        Url::parse(endpoint).map_err(|err| SecurityError::InvalidPath(err.to_string()))?;
    {
        let mut query = url.query_pairs_mut();
        query.append_pair("response_type", "code");
        query.append_pair("client_id", client_id);
        query.append_pair("redirect_uri", redirect_uri);
        query.append_pair("scope", &scopes.join(" "));
        query.append_pair("code_challenge", &pkce.code_challenge);
        query.append_pair("code_challenge_method", "S256");
        query.append_pair("state", &pkce.state);
        if let Some(extra) = extra_params {
            for (k, v) in extra {
                query.append_pair(k, v);
            }
        }
    }
    Ok(url.to_string())
}

pub async fn receive_loopback_code(
    port: u16,
    expected_state: &str,
    timeout_secs: u64,
) -> Result<String, SecurityError> {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
        .await
        .map_err(|err| SecurityError::InvalidPath(err.to_string()))?;

    let accepted = tokio::time::timeout(Duration::from_secs(timeout_secs), listener.accept())
        .await
        .map_err(|_| SecurityError::InvalidPath("oauth callback timed out".to_string()))?
        .map_err(|err| SecurityError::InvalidPath(err.to_string()))?;

    let (mut stream, _) = accepted;
    let mut buffer = [0_u8; 4096];
    let n = stream
        .read(&mut buffer)
        .await
        .map_err(|err| SecurityError::InvalidPath(err.to_string()))?;
    let request = String::from_utf8_lossy(&buffer[..n]);
    let first_line = request.lines().next().unwrap_or_default();
    let path = first_line
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| SecurityError::InvalidPath("invalid oauth callback request".to_string()))?;

    let fake_url = format!("http://localhost{path}");
    let parsed =
        Url::parse(&fake_url).map_err(|err| SecurityError::InvalidPath(err.to_string()))?;
    let mut code = None;
    let mut state = None;
    for (k, v) in parsed.query_pairs() {
        match k.as_ref() {
            "code" => code = Some(v.to_string()),
            "state" => state = Some(v.to_string()),
            _ => {}
        }
    }

    let response = if state.as_deref() != Some(expected_state) {
        "HTTP/1.1 400 Bad Request\r\ncontent-type: text/plain\r\n\r\nState mismatch"
    } else {
        "HTTP/1.1 200 OK\r\ncontent-type: text/html\r\n\r\n<html><body><h1>Authentication complete</h1><p>You can return to nyx.</p></body></html>"
    };
    stream
        .write_all(response.as_bytes())
        .await
        .map_err(|err| SecurityError::InvalidPath(err.to_string()))?;

    if state.as_deref() != Some(expected_state) {
        return Err(SecurityError::InvalidPath(
            "oauth state mismatch".to_string(),
        ));
    }

    code.ok_or_else(|| SecurityError::InvalidPath("missing oauth authorization code".to_string()))
}

pub async fn exchange_code_for_tokens(
    token_url: &str,
    client_id: &str,
    redirect_uri: &str,
    code: &str,
    code_verifier: &str,
) -> Result<TokenSet, SecurityError> {
    let client = reqwest::Client::new();
    let response = client
        .post(token_url)
        .form(&[
            ("grant_type", "authorization_code"),
            ("client_id", client_id),
            ("redirect_uri", redirect_uri),
            ("code", code),
            ("code_verifier", code_verifier),
        ])
        .send()
        .await
        .map_err(|err| SecurityError::EncryptionFailed(err.to_string()))?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(SecurityError::EncryptionFailed(format!(
            "oauth token exchange failed: {status} {body}"
        )));
    }

    let wire: OAuthTokenWire = response
        .json()
        .await
        .map_err(|err| SecurityError::EncryptionFailed(err.to_string()))?;
    Ok(wire.into_token_set())
}

pub async fn start_device_code_flow(
    device_url: &str,
    client_id: &str,
    scopes: &[&str],
) -> Result<DeviceCodeResponse, SecurityError> {
    let client = reqwest::Client::new();
    let response = client
        .post(device_url)
        .form(&[("client_id", client_id), ("scope", &scopes.join(" "))])
        .send()
        .await
        .map_err(|err| SecurityError::EncryptionFailed(err.to_string()))?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(SecurityError::EncryptionFailed(format!(
            "oauth device flow start failed: {status} {body}"
        )));
    }

    response
        .json::<DeviceCodeResponse>()
        .await
        .map_err(|err| SecurityError::EncryptionFailed(err.to_string()))
}

pub async fn poll_device_code(
    token_url: &str,
    client_id: &str,
    device_code: &str,
    interval_secs: u64,
    timeout_secs: u64,
) -> Result<TokenSet, SecurityError> {
    let client = reqwest::Client::new();
    let deadline = Instant::now() + Duration::from_secs(timeout_secs);
    let mut current_interval = interval_secs.max(1);

    loop {
        if Instant::now() > deadline {
            return Err(SecurityError::InvalidPath(
                "oauth device flow timed out".to_string(),
            ));
        }

        let response = client
            .post(token_url)
            .form(&[
                ("grant_type", "urn:ietf:params:oauth:grant-type:device_code"),
                ("client_id", client_id),
                ("device_code", device_code),
            ])
            .send()
            .await
            .map_err(|err| SecurityError::EncryptionFailed(err.to_string()))?;

        if response.status().is_success() {
            let wire: OAuthTokenWire = response
                .json()
                .await
                .map_err(|err| SecurityError::EncryptionFailed(err.to_string()))?;
            return Ok(wire.into_token_set());
        }

        let parsed = response
            .json::<OAuthErrorBody>()
            .await
            .unwrap_or(OAuthErrorBody {
                error: "unknown_error".to_string(),
                error_description: None,
            });
        match parsed.error.as_str() {
            "authorization_pending" => {
                tokio::time::sleep(Duration::from_secs(current_interval)).await;
            }
            "slow_down" => {
                current_interval += 5;
                tokio::time::sleep(Duration::from_secs(current_interval)).await;
            }
            "access_denied" | "expired_token" => {
                return Err(SecurityError::EncryptionFailed(format!(
                    "oauth device flow failed: {}",
                    parsed
                        .error_description
                        .unwrap_or_else(|| parsed.error.to_string())
                )));
            }
            other => {
                return Err(SecurityError::EncryptionFailed(format!(
                    "oauth device flow failed: {other}"
                )));
            }
        }
    }
}

use std::time::Instant;

pub async fn refresh_access_token(
    token_url: &str,
    client_id: &str,
    refresh_token: &str,
) -> Result<TokenSet, SecurityError> {
    let client = reqwest::Client::new();
    let delays = [350_u64, 700_u64, 1050_u64];
    let mut last_error = None;

    for (attempt, delay_ms) in delays.iter().enumerate() {
        let response = client
            .post(token_url)
            .form(&[
                ("grant_type", "refresh_token"),
                ("client_id", client_id),
                ("refresh_token", refresh_token),
            ])
            .send()
            .await;

        match response {
            Ok(resp) if resp.status().is_success() => {
                let mut token: TokenSet = resp
                    .json::<OAuthTokenWire>()
                    .await
                    .map_err(|err| SecurityError::EncryptionFailed(err.to_string()))?
                    .into_token_set();
                if token.refresh_token.is_none() {
                    token.refresh_token = Some(refresh_token.to_string());
                }
                return Ok(token);
            }
            Ok(resp) => {
                last_error = Some(format!(
                    "attempt {} failed: {}",
                    attempt + 1,
                    resp.text().await.unwrap_or_default()
                ));
            }
            Err(err) => {
                last_error = Some(format!("attempt {} failed: {err}", attempt + 1));
            }
        }

        tokio::time::sleep(Duration::from_millis(*delay_ms)).await;
    }

    Err(SecurityError::EncryptionFailed(format!(
        "oauth refresh failed after retries: {}",
        last_error.unwrap_or_else(|| "unknown error".to_string())
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[test]
    fn pkce_state_has_expected_shape() {
        let pkce = generate_pkce_state();
        assert!(pkce.code_verifier.len() >= 43);
        assert!(!pkce.state.is_empty());

        let expected = URL_SAFE_NO_PAD.encode(Sha256::digest(pkce.code_verifier.as_bytes()));
        assert_eq!(expected, pkce.code_challenge);
    }

    #[test]
    fn build_authorize_url_contains_expected_params() {
        let pkce = PkceState {
            code_verifier: "v".to_string(),
            code_challenge: "c".to_string(),
            state: "s".to_string(),
        };
        let url = build_authorize_url(
            "https://auth.example.com/authorize",
            "client",
            "http://localhost/callback",
            &["openid", "offline_access"],
            &pkce,
            None,
        )
        .expect("url");

        assert!(url.contains("response_type=code"));
        assert!(url.contains("client_id=client"));
        assert!(url.contains("code_challenge=c"));
        assert!(url.contains("state=s"));
        assert!(url.contains("scope=openid+offline_access"));
    }

    #[tokio::test]
    async fn exchange_code_for_tokens_maps_expires_at() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "a",
                "refresh_token": "r",
                "id_token": "i",
                "expires_in": 120,
                "token_type": "Bearer"
            })))
            .mount(&server)
            .await;

        let token = exchange_code_for_tokens(
            &format!("{}/token", server.uri()),
            "client",
            "http://localhost/callback",
            "code",
            "verifier",
        )
        .await
        .expect("exchange");

        assert_eq!(token.access_token, "a");
        assert_eq!(token.refresh_token.as_deref(), Some("r"));
        assert!(token.expires_at > 0);
    }

    #[tokio::test]
    async fn poll_device_code_handles_slow_down_then_success() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/token"))
            .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
                "error": "slow_down"
            })))
            .up_to_n_times(1)
            .mount(&server)
            .await;

        Mock::given(method("POST"))
            .and(path("/token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "a2",
                "refresh_token": "r2",
                "expires_in": 3600
            })))
            .mount(&server)
            .await;

        let token = poll_device_code(
            &format!("{}/token", server.uri()),
            "client",
            "device",
            0,
            10,
        )
        .await
        .expect("poll");

        assert_eq!(token.access_token, "a2");
    }

    #[tokio::test]
    async fn refresh_retries_then_succeeds_and_preserves_refresh_token() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/token"))
            .respond_with(ResponseTemplate::new(500).set_body_string("nope"))
            .up_to_n_times(2)
            .mount(&server)
            .await;

        Mock::given(method("POST"))
            .and(path("/token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "new-access",
                "expires_in": 60
            })))
            .mount(&server)
            .await;

        let token =
            refresh_access_token(&format!("{}/token", server.uri()), "client", "old-refresh")
                .await
                .expect("refresh");
        assert_eq!(token.access_token, "new-access");
        assert_eq!(token.refresh_token.as_deref(), Some("old-refresh"));
    }
}
