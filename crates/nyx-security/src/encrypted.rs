use std::collections::HashMap;

use aes_gcm::aead::rand_core::RngCore;
use aes_gcm::aead::{Aead, KeyInit, OsRng};
use aes_gcm::{Aes256Gcm, Nonce};
use async_trait::async_trait;
use sha2::{Digest, Sha256};
use tokio::sync::RwLock;

use crate::{Secret, SecretError, SecretStore};

const MASTER_KEY_ENV: &str = "NYX_SECURITY_MASTER_KEY";
const KEYRING_SERVICE: &str = "nyx.security.master_key";

#[derive(Debug, Clone)]
struct EncryptedSecret {
    nonce: [u8; 12],
    ciphertext: Vec<u8>,
}

#[derive(Debug)]
pub struct EncryptedSecretStore {
    key: [u8; 32],
    entries: RwLock<HashMap<String, EncryptedSecret>>,
}

impl EncryptedSecretStore {
    pub fn from_env_or_keyring() -> Result<Self, SecretError> {
        let passphrase = std::env::var(MASTER_KEY_ENV)
            .ok()
            .or_else(load_keyring_passphrase);
        let passphrase = passphrase.ok_or(SecretError::MissingMasterKey(KEYRING_SERVICE))?;
        Ok(Self::from_passphrase(passphrase.as_bytes()))
    }

    pub fn from_passphrase(passphrase: &[u8]) -> Self {
        let digest = Sha256::digest(passphrase);
        let mut key = [0_u8; 32];
        key.copy_from_slice(&digest);

        Self {
            key,
            entries: RwLock::new(HashMap::new()),
        }
    }

    fn cipher(&self) -> Result<Aes256Gcm, SecretError> {
        Aes256Gcm::new_from_slice(&self.key)
            .map_err(|err| SecretError::Crypto(format!("invalid key material: {err}")))
    }

    fn encrypt(&self, secret: &Secret) -> Result<EncryptedSecret, SecretError> {
        let mut nonce = [0_u8; 12];
        OsRng.fill_bytes(&mut nonce);

        let cipher = self.cipher()?;
        let ciphertext = cipher
            .encrypt(Nonce::from_slice(&nonce), secret.expose())
            .map_err(|err| SecretError::Crypto(format!("encrypt failed: {err}")))?;

        Ok(EncryptedSecret { nonce, ciphertext })
    }

    fn decrypt(&self, secret: &EncryptedSecret) -> Result<Secret, SecretError> {
        let cipher = self.cipher()?;
        let plaintext = cipher
            .decrypt(Nonce::from_slice(&secret.nonce), secret.ciphertext.as_ref())
            .map_err(|err| SecretError::Crypto(format!("decrypt failed: {err}")))?;
        Ok(Secret::from_bytes(plaintext))
    }
}

#[async_trait]
impl SecretStore for EncryptedSecretStore {
    async fn get(&self, key: &str) -> Result<Secret, SecretError> {
        let guard = self.entries.read().await;
        let encrypted = guard
            .get(key)
            .ok_or_else(|| SecretError::NotFound(key.to_string()))?;

        self.decrypt(encrypted)
    }

    async fn set(&self, key: &str, value: Secret) -> Result<(), SecretError> {
        let encrypted = self.encrypt(&value)?;
        let mut guard = self.entries.write().await;
        guard.insert(key.to_string(), encrypted);
        Ok(())
    }
}

fn load_keyring_passphrase() -> Option<String> {
    #[cfg(target_os = "macos")]
    {
        let output = std::process::Command::new("security")
            .args(["find-generic-password", "-s", KEYRING_SERVICE, "-w"])
            .output()
            .ok()?;

        if output.status.success() {
            let secret = String::from_utf8(output.stdout).ok()?;
            let trimmed = secret.trim().to_string();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed)
            }
        } else {
            None
        }
    }

    #[cfg(all(unix, not(target_os = "macos")))]
    {
        let output = std::process::Command::new("secret-tool")
            .args(["lookup", "service", KEYRING_SERVICE])
            .output()
            .ok()?;

        if output.status.success() {
            let secret = String::from_utf8(output.stdout).ok()?;
            let trimmed = secret.trim().to_string();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed)
            }
        } else {
            None
        }
    }

    #[cfg(not(unix))]
    {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn encrypted_secret_store_round_trips_secret() {
        let store = EncryptedSecretStore::from_passphrase(b"unit-test-master-key");

        store
            .set("openai_api_key", Secret::from_string("super-secret-key"))
            .await
            .expect("set secret");

        let value = store.get("openai_api_key").await.expect("read secret back");

        assert_eq!(value.expose(), b"super-secret-key");
    }
}
