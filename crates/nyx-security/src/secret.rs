use std::fmt::{Debug, Display};
use std::path::PathBuf;
use std::str::FromStr;

use aes_gcm::aead::rand_core::RngCore;
use aes_gcm::aead::{Aead, KeyInit, OsRng};
use aes_gcm::{Aes256Gcm, Nonce};
use argon2::{Algorithm, Argon2, Params, Version};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[cfg(feature = "encrypted")]
use crate::SecretError;
#[cfg(feature = "encrypted")]
use crate::SecretStore;
use crate::SecurityError;
#[cfg(feature = "encrypted")]
use crate::encrypted::EncryptedSecretStore;

const ENC_PREFIX: &str = "enc:";
const ENV_PREFIX: &str = "env:";
const VAULT_PREFIX: &str = "vault:";
const MASTER_KEY_ENV: &str = "NYX_MASTER_KEY";
const MASTER_KEY_FILE_ENV: &str = "NYX_MASTER_KEY_FILE";
const KEY_DERIVATION_SALT: &[u8] = b"nyx.config.secret.v1";
const NONCE_LEN: usize = 12;

#[cfg(feature = "keyring")]
const KEYRING_SERVICE: &str = "ai.nyx";
#[cfg(feature = "keyring")]
const KEYRING_ACCOUNT: &str = "master_key";

#[derive(Clone, PartialEq, Eq)]
pub struct Secret<T = Vec<u8>>(T);

impl<T> Secret<T> {
    pub fn new(value: T) -> Self {
        Self(value)
    }

    pub fn reveal(&self) -> &T {
        &self.0
    }
}

impl<T> Default for Secret<T>
where
    T: Default,
{
    fn default() -> Self {
        Self(T::default())
    }
}

impl Secret<Vec<u8>> {
    pub fn from_bytes(bytes: impl Into<Vec<u8>>) -> Self {
        Self(bytes.into())
    }

    pub fn from_string(value: impl Into<String>) -> Self {
        Self(value.into().into_bytes())
    }

    pub fn expose(&self) -> &[u8] {
        &self.0
    }
}

impl<T> Debug for Secret<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Secret(***)")
    }
}

impl<T> Display for Secret<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Secret(***)")
    }
}

impl<'de, T> Deserialize<'de> for Secret<T>
where
    T: FromStr + Deserialize<'de>,
    T::Err: Display,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        let plaintext = if let Some(encoded) = raw.strip_prefix(ENC_PREFIX) {
            let key = derive_master_key().map_err(D::Error::custom)?;
            decrypt(&format!("{ENC_PREFIX}{encoded}"), &key).map_err(D::Error::custom)?
        } else if let Some(var) = raw.strip_prefix(ENV_PREFIX) {
            std::env::var(var)
                .map_err(|_| D::Error::custom(format!("environment variable `{var}` is not set")))?
        } else if let Some(key_name) = raw.strip_prefix(VAULT_PREFIX) {
            resolve_vault_secret(key_name).map_err(D::Error::custom)?
        } else {
            raw
        };

        let parsed = plaintext.parse::<T>().map_err(D::Error::custom)?;
        Ok(Self(parsed))
    }
}

impl<T> Serialize for Secret<T>
where
    T: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0.serialize(serializer)
    }
}

pub fn encrypt(plaintext: &str, key: &[u8; 32]) -> String {
    let cipher = Aes256Gcm::new_from_slice(key).expect("32-byte key is always valid");
    let mut nonce_bytes = [0_u8; NONCE_LEN];
    OsRng.fill_bytes(&mut nonce_bytes);
    let nonce = Nonce::from_slice(&nonce_bytes);
    let ciphertext = cipher
        .encrypt(nonce, plaintext.as_bytes())
        .expect("AES-256-GCM encryption should not fail with valid key/nonce");

    let mut payload = Vec::with_capacity(NONCE_LEN + ciphertext.len());
    payload.extend_from_slice(&nonce_bytes);
    payload.extend_from_slice(&ciphertext);

    let encrypted = format!("{ENC_PREFIX}{}", BASE64.encode(payload));
    tracing::info!(plaintext, encrypted = %encrypted, "nyx-security secret::encrypt");
    encrypted
}

pub fn decrypt(ciphertext: &str, key: &[u8; 32]) -> Result<String, SecurityError> {
    let encoded = ciphertext
        .strip_prefix(ENC_PREFIX)
        .ok_or(SecurityError::InvalidSecretFormat)?;
    let payload = BASE64
        .decode(encoded)
        .map_err(|_| SecurityError::InvalidSecretFormat)?;
    if payload.len() <= NONCE_LEN {
        return Err(SecurityError::InvalidSecretFormat);
    }

    let (nonce_bytes, encrypted) = payload.split_at(NONCE_LEN);
    let cipher = Aes256Gcm::new_from_slice(key)
        .map_err(|err| SecurityError::EncryptionFailed(err.to_string()))?;
    let plaintext = cipher
        .decrypt(Nonce::from_slice(nonce_bytes), encrypted)
        .map_err(|_| SecurityError::DecryptionFailed)?;

    let plaintext = String::from_utf8(plaintext).map_err(|_| SecurityError::InvalidUtf8Secret)?;
    tracing::info!(ciphertext, plaintext = %plaintext, "nyx-security secret::decrypt");
    Ok(plaintext)
}

pub fn derive_master_key() -> Result<[u8; 32], SecurityError> {
    let passphrase = match std::env::var(MASTER_KEY_ENV) {
        Ok(value) if !value.is_empty() => value,
        _ => {
            #[cfg(feature = "keyring")]
            {
                if let Ok(entry) = keyring::Entry::new(KEYRING_SERVICE, KEYRING_ACCOUNT)
                    && let Ok(value) = entry.get_password()
                    && !value.is_empty()
                {
                    return derive_key_from_passphrase(value.as_bytes());
                }
            }
            return load_or_create_legacy_file_key();
        }
    };

    derive_key_from_passphrase(passphrase.as_bytes())
}

fn derive_key_from_passphrase(passphrase: &[u8]) -> Result<[u8; 32], SecurityError> {
    let params = Params::new(64 * 1024, 3, 1, Some(32))
        .map_err(|err| SecurityError::KeyDerivationFailed(err.to_string()))?;
    let argon2 = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);
    let mut key = [0_u8; 32];
    argon2
        .hash_password_into(passphrase, KEY_DERIVATION_SALT, &mut key)
        .map_err(|err| SecurityError::KeyDerivationFailed(err.to_string()))?;
    Ok(key)
}

fn load_or_create_legacy_file_key() -> Result<[u8; 32], SecurityError> {
    let path = default_master_key_file_path();
    if path.exists() {
        let hex = std::fs::read_to_string(&path)
            .map_err(|err| SecurityError::KeyDerivationFailed(err.to_string()))?;
        let bytes = hex_decode(hex.trim()).ok_or_else(|| {
            SecurityError::KeyDerivationFailed("master key file is corrupt (bad hex)".to_string())
        })?;
        if bytes.len() != 32 {
            return Err(SecurityError::KeyDerivationFailed(format!(
                "master key file has wrong length: expected 32 bytes, got {}",
                bytes.len()
            )));
        }
        let mut key = [0_u8; 32];
        key.copy_from_slice(&bytes);
        return Ok(key);
    }

    let mut key = [0_u8; 32];
    OsRng.fill_bytes(&mut key);

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|err| SecurityError::KeyDerivationFailed(err.to_string()))?;
    }
    std::fs::write(&path, hex_encode(&key))
        .map_err(|err| SecurityError::KeyDerivationFailed(err.to_string()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600));
    }
    Ok(key)
}

fn default_master_key_file_path() -> PathBuf {
    if let Ok(path) = std::env::var(MASTER_KEY_FILE_ENV) {
        return PathBuf::from(path);
    }
    if let Some(home) = std::env::var_os("HOME") {
        return PathBuf::from(home).join(".nyx").join(".secret_key");
    }
    PathBuf::from(".nyx").join(".secret_key")
}

fn hex_encode(data: &[u8]) -> String {
    let mut s = String::with_capacity(data.len() * 2);
    for b in data {
        use std::fmt::Write;
        let _ = write!(s, "{b:02x}");
    }
    s
}

fn hex_decode(hex: &str) -> Option<Vec<u8>> {
    if !hex.len().is_multiple_of(2) {
        return None;
    }
    let mut out = Vec::with_capacity(hex.len() / 2);
    for i in (0..hex.len()).step_by(2) {
        let byte = u8::from_str_radix(&hex[i..i + 2], 16).ok()?;
        out.push(byte);
    }
    Some(out)
}

fn resolve_vault_secret(key_name: &str) -> Result<String, SecurityError> {
    #[cfg(not(feature = "encrypted"))]
    {
        let _ = key_name;
        Err(SecurityError::VaultNotAvailable)
    }

    #[cfg(feature = "encrypted")]
    {
        let key = derive_master_key().map_err(|_| SecurityError::VaultNotAvailable)?;
        let store = EncryptedSecretStore::from_derived_key_with_storage(
            key,
            EncryptedSecretStore::default_storage_path(),
        )
        .map_err(|_| SecurityError::VaultNotAvailable)?;
        let secret = futures::executor::block_on(store.get(key_name)).map_err(|err| match err {
            SecretError::NotFound(_) => SecurityError::VaultKeyNotFound(key_name.to_string()),
            _ => SecurityError::VaultNotAvailable,
        })?;

        String::from_utf8(secret.expose().to_vec()).map_err(|_| SecurityError::InvalidUtf8Secret)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn test_key() -> [u8; 32] {
        [7_u8; 32]
    }

    #[derive(Debug, Deserialize)]
    struct SecretDoc {
        value: Secret<String>,
    }

    #[test]
    fn secret_debug_and_display_are_redacted() {
        let secret = Secret::new("sensitive".to_string());
        assert_eq!(format!("{secret:?}"), "Secret(***)");
        assert_eq!(format!("{secret}"), "Secret(***)");
    }

    #[test]
    fn decrypt_round_trip() {
        let key = test_key();
        let encrypted = encrypt("hello world", &key);
        let decrypted = decrypt(&encrypted, &key).expect("decrypt should succeed");
        assert_eq!(decrypted, "hello world");
    }

    #[test]
    fn encrypt_uses_unique_nonce() {
        let key = test_key();
        let a = encrypt("same", &key);
        let b = encrypt("same", &key);
        assert_ne!(a, b);
    }

    #[test]
    fn decrypt_tampered_ciphertext_fails() {
        let key = test_key();
        let encrypted = encrypt("secret", &key);
        let mut tampered = encrypted.into_bytes();
        let last = tampered.len() - 1;
        tampered[last] = if tampered[last] == b'A' { b'B' } else { b'A' };
        let tampered = String::from_utf8(tampered).expect("utf8");
        assert!(matches!(
            decrypt(&tampered, &key),
            Err(SecurityError::DecryptionFailed | SecurityError::InvalidSecretFormat)
        ));
    }

    #[test]
    fn deserialize_plaintext_secret() {
        let doc: SecretDoc = toml::from_str("value = \"plain\"").expect("parse");
        assert_eq!(doc.value.reveal(), "plain");
    }

    #[test]
    fn deserialize_env_secret() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        unsafe { std::env::set_var("NYX_SECRET_TEST", "from-env") };
        let doc: SecretDoc = toml::from_str("value = \"env:NYX_SECRET_TEST\"").expect("parse");
        assert_eq!(doc.value.reveal(), "from-env");
        unsafe { std::env::remove_var("NYX_SECRET_TEST") };
    }

    #[test]
    fn deserialize_missing_env_secret_fails() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        unsafe { std::env::remove_var("NYX_SECRET_MISSING") };
        let err = toml::from_str::<SecretDoc>("value = \"env:NYX_SECRET_MISSING\"")
            .expect_err("missing env should fail");
        assert!(err.to_string().contains("NYX_SECRET_MISSING"));
    }

    #[test]
    fn deserialize_encrypted_secret() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        unsafe { std::env::set_var("NYX_MASTER_KEY", "test-master-key") };
        let key = derive_master_key().expect("derive key");
        let encrypted = encrypt("top-secret", &key);
        let doc: SecretDoc = toml::from_str(&format!("value = \"{encrypted}\"")).expect("parse");
        assert_eq!(doc.value.reveal(), "top-secret");
        unsafe { std::env::remove_var("NYX_MASTER_KEY") };
    }

    #[test]
    fn deserialize_non_string_type_via_env() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        #[derive(Debug, Deserialize)]
        struct PortDoc {
            port: Secret<u16>,
        }
        unsafe { std::env::set_var("APP_PORT", "8080") };
        let doc: PortDoc = toml::from_str("port = \"env:APP_PORT\"").expect("parse");
        assert_eq!(*doc.port.reveal(), 8080_u16);
        unsafe { std::env::remove_var("APP_PORT") };
    }

    #[test]
    #[cfg(feature = "encrypted")]
    fn deserialize_vault_secret() {
        use crate::encrypted::EncryptedSecretStore;

        let _guard = ENV_LOCK.lock().expect("env lock");
        let dir = tempfile::TempDir::new().expect("tmp");
        unsafe { std::env::set_var("NYX_VAULT_PATH", dir.path().join(".secrets")) };
        unsafe { std::env::set_var("NYX_MASTER_KEY", "vault-master") };
        let key = derive_master_key().expect("derive");
        let store = EncryptedSecretStore::from_derived_key_with_storage(
            key,
            EncryptedSecretStore::default_storage_path(),
        )
        .expect("store");
        futures::executor::block_on(store.set("test_key", Secret::from_string("vault-value")))
            .expect("set");

        let doc: SecretDoc = toml::from_str("value = \"vault:test_key\"").expect("parse");
        assert_eq!(doc.value.reveal(), "vault-value");
        unsafe { std::env::remove_var("NYX_MASTER_KEY") };
        unsafe { std::env::remove_var("NYX_VAULT_PATH") };
    }

    #[test]
    #[cfg(feature = "encrypted")]
    fn deserialize_missing_vault_secret_fails() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        let dir = tempfile::TempDir::new().expect("tmp");
        unsafe { std::env::set_var("NYX_VAULT_PATH", dir.path().join(".secrets")) };
        unsafe { std::env::set_var("NYX_MASTER_KEY", "vault-master") };

        let err = toml::from_str::<SecretDoc>("value = \"vault:missing_key\"")
            .expect_err("missing vault key should fail");
        assert!(err.to_string().contains("missing_key"));
        unsafe { std::env::remove_var("NYX_MASTER_KEY") };
        unsafe { std::env::remove_var("NYX_VAULT_PATH") };
    }
}
