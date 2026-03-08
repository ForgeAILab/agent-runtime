use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TokenSet {
    pub access_token: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id_token: Option<String>,
    pub expires_at: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
}

impl TokenSet {
    pub fn is_expired(&self) -> bool {
        self.expires_within(Duration::from_secs(0))
    }

    pub fn expires_within(&self, duration: Duration) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        self.expires_at <= now + duration.as_secs() as i64
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OAuthProfile {
    pub id: String,
    pub provider: String,
    pub profile_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account_id: Option<String>,
    pub token_set: TokenSet,
    pub created_at: i64,
    pub updated_at: i64,
}

pub fn extract_account_id_from_jwt(token: &str) -> Option<String> {
    let payload = token.split('.').nth(1)?;
    let decoded = URL_SAFE_NO_PAD.decode(payload).ok()?;
    let value: Value = serde_json::from_slice(&decoded).ok()?;

    // OpenAI stores the account ID under a namespaced claim:
    // { "https://api.openai.com/auth": { "chatgpt_account_id": "acct_xxx" } }
    if let Some(acct) = value
        .get("https://api.openai.com/auth")
        .and_then(|auth| auth.get("chatgpt_account_id"))
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
    {
        return Some(acct.to_string());
    }

    // Fallback to generic claims for non-OpenAI providers
    ["account_id", "accountId", "acct", "sub"]
        .iter()
        .find_map(|key| {
            value
                .get(key)
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .map(ToString::to_string)
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn now_unix() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64
    }

    #[test]
    fn token_expiry_helpers_work() {
        let now = now_unix();
        let expired = TokenSet {
            access_token: "a".to_string(),
            refresh_token: None,
            id_token: None,
            expires_at: now - 5,
            token_type: Some("Bearer".to_string()),
            scope: None,
        };
        assert!(expired.is_expired());

        let soon = TokenSet {
            expires_at: now + 30,
            ..expired.clone()
        };
        assert!(soon.expires_within(Duration::from_secs(60)));
        assert!(!soon.expires_within(Duration::from_secs(10)));
    }

    #[test]
    fn extracts_account_id_from_openai_namespaced_claim() {
        let mk = |json: &str| -> String {
            let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"none"}"#);
            let payload = URL_SAFE_NO_PAD.encode(json.as_bytes());
            format!("{header}.{payload}.")
        };

        // OpenAI-style namespaced claim takes priority
        assert_eq!(
            extract_account_id_from_jwt(&mk(
                r#"{"https://api.openai.com/auth":{"chatgpt_account_id":"acct-openai"},"sub":"acct-sub"}"#
            )),
            Some("acct-openai".to_string())
        );
    }

    #[test]
    fn extracts_account_id_from_common_claims() {
        let mk = |json: &str| -> String {
            let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"none"}"#);
            let payload = URL_SAFE_NO_PAD.encode(json.as_bytes());
            format!("{header}.{payload}.")
        };

        assert_eq!(
            extract_account_id_from_jwt(&mk(r#"{"account_id":"acct-1"}"#)),
            Some("acct-1".to_string())
        );
        assert_eq!(
            extract_account_id_from_jwt(&mk(r#"{"accountId":"acct-2"}"#)),
            Some("acct-2".to_string())
        );
        assert_eq!(
            extract_account_id_from_jwt(&mk(r#"{"acct":"acct-3"}"#)),
            Some("acct-3".to_string())
        );
        assert_eq!(
            extract_account_id_from_jwt(&mk(r#"{"sub":"acct-4"}"#)),
            Some("acct-4".to_string())
        );
        assert_eq!(extract_account_id_from_jwt("bad"), None);
    }
}
