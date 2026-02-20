use std::path::PathBuf;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::{Sandbox, SecretStore, SecurityError};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SecurityConfig {
    #[serde(default = "default_sandbox_kind")]
    pub sandbox: String,
    #[serde(default = "default_secret_store_kind")]
    pub secret_store: String,
    #[serde(default = "default_operators")]
    pub operators: Vec<String>,
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            sandbox: default_sandbox_kind(),
            secret_store: default_secret_store_kind(),
            operators: default_operators(),
        }
    }
}

pub fn default_sandbox_kind() -> String {
    "os".to_string()
}

pub fn default_secret_store_kind() -> String {
    "encrypted".to_string()
}

pub fn default_operators() -> Vec<String> {
    vec!["local".to_string()]
}

pub fn build_sandbox(cfg: &SecurityConfig) -> Result<Arc<dyn Sandbox>, SecurityError> {
    let sandbox: Arc<dyn Sandbox> = match cfg.sandbox.as_str() {
        "noop" => Arc::new(crate::testing::NoopSandbox),
        #[cfg(feature = "os-sandbox")]
        "os" => {
            let root = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
            Arc::new(crate::os_sandbox::OsSandbox::new(root)?)
        }
        other => return Err(SecurityError::UnknownKind(other.to_string())),
    };

    tracing::debug!(kind = cfg.sandbox.as_str(), "building sandbox");
    Ok(sandbox)
}

pub fn build_secret_store(cfg: &SecurityConfig) -> Result<Arc<dyn SecretStore>, SecurityError> {
    let store: Arc<dyn SecretStore> = match cfg.secret_store.as_str() {
        #[cfg(feature = "encrypted")]
        "encrypted" => {
            let key_path = std::env::var_os("HOME")
                .map(|h| PathBuf::from(h).join(".nyx").join(".secret_key"))
                .unwrap_or_else(|| PathBuf::from(".nyx").join(".secret_key"));
            Arc::new(crate::encrypted::EncryptedSecretStore::from_env_or_file(
                &key_path,
            )?)
        }
        "in-memory" => Arc::new(crate::testing::InMemorySecretStore::new()),
        other => return Err(SecurityError::UnknownKind(other.to_string())),
    };

    tracing::debug!(kind = cfg.secret_store.as_str(), "building secret store");
    Ok(store)
}
