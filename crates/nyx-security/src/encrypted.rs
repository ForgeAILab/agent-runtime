use std::collections::HashMap;
use std::path::Path;

use aes_gcm::aead::rand_core::RngCore;
use aes_gcm::aead::{Aead, KeyInit, OsRng};
use aes_gcm::{Aes256Gcm, Nonce};
use async_trait::async_trait;
use sha2::{Digest, Sha256};
use tokio::sync::RwLock;

use crate::{Secret, SecretError, SecretStore};

const MASTER_KEY_ENV: &str = "NYX_SECURITY_MASTER_KEY";
#[cfg(feature = "keyring")]
const KEYRING_SERVICE: &str = "nyx.security.master_key";
#[cfg(feature = "keyring")]
const KEYRING_USER: &str = "nyx";

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
    /// Default constructor: checks `NYX_SECURITY_MASTER_KEY` env var first,
    /// then the OS keychain (if the `keyring` feature is enabled), then
    /// loads or creates a random key at `key_path` (0600 permissions on unix).
    /// No subprocess is ever spawned.
    pub fn from_env_or_file(key_path: &Path) -> Result<Self, SecretError> {
        if let Ok(passphrase) = std::env::var(MASTER_KEY_ENV) {
            return Ok(Self::from_passphrase(passphrase.as_bytes()));
        }

        #[cfg(feature = "keyring")]
        if let Some(passphrase) = try_keyring_passphrase() {
            return Ok(Self::from_passphrase(passphrase.as_bytes()));
        }

        let key = load_or_create_file_key(key_path)?;
        Ok(Self::from_key(key))
    }

    /// Build from a passphrase by deriving a 32-byte key with SHA-256.
    pub fn from_passphrase(passphrase: &[u8]) -> Self {
        let digest = Sha256::digest(passphrase);
        let mut key = [0u8; 32];
        key.copy_from_slice(&digest);
        Self::from_key(key)
    }

    fn from_key(key: [u8; 32]) -> Self {
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

/// Load a 32-byte key from `path` (hex-encoded), or generate and persist a
/// fresh random key if the file doesn't exist yet.
fn load_or_create_file_key(path: &Path) -> Result<[u8; 32], SecretError> {
    if path.exists() {
        let hex = std::fs::read_to_string(path)?;
        let bytes = hex_decode(hex.trim())
            .map_err(|_| SecretError::Crypto("key file is corrupt (bad hex)".into()))?;
        if bytes.len() != 32 {
            return Err(SecretError::Crypto(format!(
                "key file has wrong length: expected 32 bytes, got {}",
                bytes.len()
            )));
        }
        let mut key = [0u8; 32];
        key.copy_from_slice(&bytes);
        Ok(key)
    } else {
        let mut key = [0u8; 32];
        OsRng.fill_bytes(&mut key);

        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, hex_encode(&key))?;

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
        }

        Ok(key)
    }
}

#[cfg(feature = "keyring")]
fn try_keyring_passphrase() -> Option<String> {
    keyring::Entry::new(KEYRING_SERVICE, KEYRING_USER)
        .ok()
        .and_then(|e| e.get_password().ok())
}

fn hex_encode(data: &[u8]) -> String {
    let mut s = String::with_capacity(data.len() * 2);
    for b in data {
        use std::fmt::Write;
        let _ = write!(s, "{b:02x}");
    }
    s
}

fn hex_decode(hex: &str) -> Result<Vec<u8>, ()> {
    if hex.len() % 2 != 0 {
        return Err(());
    }
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).map_err(|_| ()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn store_in(tmp: &TempDir) -> EncryptedSecretStore {
        let key_path = tmp.path().join(".secret_key");
        EncryptedSecretStore::from_env_or_file(&key_path)
            .expect("from_env_or_file should succeed")
    }

    #[tokio::test]
    async fn encrypted_secret_store_round_trips_secret() {
        let tmp = TempDir::new().unwrap();
        let store = store_in(&tmp);

        store
            .set("openai_api_key", Secret::from_string("super-secret-key"))
            .await
            .expect("set secret");

        let value = store.get("openai_api_key").await.expect("read secret back");
        assert_eq!(value.expose(), b"super-secret-key");
    }

    #[tokio::test]
    async fn creates_key_file_on_first_use() {
        let tmp = TempDir::new().unwrap();
        let key_path = tmp.path().join(".secret_key");
        assert!(!key_path.exists());

        EncryptedSecretStore::from_env_or_file(&key_path).unwrap();
        assert!(key_path.exists(), "key file should be created");

        let hex = std::fs::read_to_string(&key_path).unwrap();
        assert_eq!(hex.len(), 64, "32 bytes = 64 hex chars");
    }

    #[tokio::test]
    async fn reuses_existing_key_file() {
        let tmp = TempDir::new().unwrap();
        let key_path = tmp.path().join(".secret_key");

        let store1 = EncryptedSecretStore::from_env_or_file(&key_path).unwrap();
        store1
            .set("k", Secret::from_string("v"))
            .await
            .unwrap();
        // A second store pointing at the same file should share the key
        // (keys match, but entries are per-instance — just verify no panic)
        let _store2 = EncryptedSecretStore::from_env_or_file(&key_path).unwrap();
    }

    #[test]
    fn from_passphrase_is_deterministic() {
        let s1 = EncryptedSecretStore::from_passphrase(b"test");
        let s2 = EncryptedSecretStore::from_passphrase(b"test");
        assert_eq!(s1.key, s2.key);
    }

    #[test]
    fn corrupt_key_file_returns_error() {
        let tmp = TempDir::new().unwrap();
        let key_path = tmp.path().join(".secret_key");
        std::fs::write(&key_path, "not-valid-hex!!").unwrap();
        let result = EncryptedSecretStore::from_env_or_file(&key_path);
        assert!(result.is_err());
    }

    #[test]
    fn wrong_length_key_file_returns_error() {
        let tmp = TempDir::new().unwrap();
        let key_path = tmp.path().join(".secret_key");
        // 16 bytes = 32 hex chars, not 32 bytes
        std::fs::write(&key_path, "aabbccddeeff00112233445566778899").unwrap();
        let result = EncryptedSecretStore::from_env_or_file(&key_path);
        assert!(result.is_err());
    }
}
