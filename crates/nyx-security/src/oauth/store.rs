use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::{Secret, SecretStore, SecurityError};

use super::token::OAuthProfile;

#[async_trait]
pub trait OAuthProfileStore: Send + Sync {
    async fn get_profile(
        &self,
        provider: &str,
        name: &str,
    ) -> Result<Option<OAuthProfile>, SecurityError>;

    async fn store_profile(&self, profile: OAuthProfile) -> Result<(), SecurityError>;

    async fn list_profiles(&self, provider: Option<&str>) -> Result<Vec<OAuthProfile>, SecurityError>;

    async fn delete_profile(&self, provider: &str, name: &str) -> Result<bool, SecurityError>;

    async fn get_active(&self, provider: &str) -> Result<Option<String>, SecurityError>;

    async fn set_active(&self, provider: &str, name: &str) -> Result<(), SecurityError>;
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct StoredProfiles {
    #[serde(default)]
    profiles: Vec<OAuthProfile>,
    #[serde(default)]
    active: HashMap<String, String>,
}

#[derive(Clone)]
pub struct FileOAuthProfileStore {
    path: PathBuf,
    lock_path: PathBuf,
    secret_store: Option<Arc<dyn SecretStore>>,
}

impl FileOAuthProfileStore {
    pub fn new(config_dir: impl Into<PathBuf>, secret_store: Option<Arc<dyn SecretStore>>) -> Self {
        let path = config_dir.into().join("oauth-profiles.json");
        let lock_path = path.with_extension("json.lock");
        Self {
            path,
            lock_path,
            secret_store,
        }
    }

    async fn with_lock<T, F, Fut>(&self, op: F) -> Result<T, SecurityError>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T, SecurityError>>,
    {
        let start = Instant::now();
        loop {
            if self.try_acquire_lock().await? {
                break;
            }
            if start.elapsed() > Duration::from_secs(10) {
                return Err(SecurityError::EncryptionFailed(
                    "timed out waiting for oauth profile lock".to_string(),
                ));
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let result = op().await;
        let _ = tokio::fs::remove_file(&self.lock_path).await;
        result
    }

    async fn try_acquire_lock(&self) -> Result<bool, SecurityError> {
        if let Some(parent) = self.lock_path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|err| SecurityError::EncryptionFailed(err.to_string()))?;
        }
        match tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&self.lock_path)
            .await
        {
            Ok(_) => Ok(true),
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => Ok(false),
            Err(err) => Err(SecurityError::EncryptionFailed(err.to_string())),
        }
    }

    async fn load_raw(&self) -> Result<StoredProfiles, SecurityError> {
        load_profiles_file(&self.path)
    }

    async fn save_raw(&self, data: &StoredProfiles) -> Result<(), SecurityError> {
        save_profiles_file(&self.path, data)
    }

    async fn hydrate_profile(&self, mut profile: OAuthProfile) -> Result<OAuthProfile, SecurityError> {
        let Some(store) = &self.secret_store else {
            return Ok(profile);
        };

        if let Some(value) = profile.token_set.refresh_token.as_ref().cloned()
            && let Some(key) = value.strip_prefix("vault:")
            && let Ok(secret) = store.get(key).await
        {
            profile.token_set.refresh_token =
                Some(String::from_utf8_lossy(secret.expose()).to_string());
        }

        if let Some(value) = profile.token_set.id_token.as_ref().cloned()
            && let Some(key) = value.strip_prefix("vault:")
            && let Ok(secret) = store.get(key).await
        {
            profile.token_set.id_token = Some(String::from_utf8_lossy(secret.expose()).to_string());
        }

        if let Some(key) = profile.token_set.access_token.strip_prefix("vault:")
            && let Ok(secret) = store.get(key).await
        {
            profile.token_set.access_token = String::from_utf8_lossy(secret.expose()).to_string();
        }

        Ok(profile)
    }

    async fn dehydrate_profile(&self, mut profile: OAuthProfile) -> Result<OAuthProfile, SecurityError> {
        let Some(store) = &self.secret_store else {
            return Ok(profile);
        };

        let access_key = format!(
            "oauth:{}:{}:access_token",
            profile.provider, profile.profile_name
        );
        store
            .set(&access_key, Secret::from_string(profile.token_set.access_token.clone()))
            .await
            .map_err(SecurityError::from)?;
        profile.token_set.access_token = format!("vault:{access_key}");

        if let Some(refresh) = profile.token_set.refresh_token.clone() {
            let refresh_key = format!(
                "oauth:{}:{}:refresh_token",
                profile.provider, profile.profile_name
            );
            store
                .set(&refresh_key, Secret::from_string(refresh))
                .await
                .map_err(SecurityError::from)?;
            profile.token_set.refresh_token = Some(format!("vault:{refresh_key}"));
        }

        if let Some(id_token) = profile.token_set.id_token.clone() {
            let id_key = format!("oauth:{}:{}:id_token", profile.provider, profile.profile_name);
            store
                .set(&id_key, Secret::from_string(id_token))
                .await
                .map_err(SecurityError::from)?;
            profile.token_set.id_token = Some(format!("vault:{id_key}"));
        }

        Ok(profile)
    }
}

#[async_trait]
impl OAuthProfileStore for FileOAuthProfileStore {
    async fn get_profile(
        &self,
        provider: &str,
        name: &str,
    ) -> Result<Option<OAuthProfile>, SecurityError> {
        self.with_lock(|| async move {
            let data = self.load_raw().await?;
            let found = data
                .profiles
                .into_iter()
                .find(|p| p.provider == provider && p.profile_name == name);
            match found {
                Some(profile) => Ok(Some(self.hydrate_profile(profile).await?)),
                None => Ok(None),
            }
        })
        .await
    }

    async fn store_profile(&self, profile: OAuthProfile) -> Result<(), SecurityError> {
        self.with_lock(|| async move {
            let mut data = self.load_raw().await?;
            data.profiles
                .retain(|p| !(p.provider == profile.provider && p.profile_name == profile.profile_name));
            data.profiles.push(self.dehydrate_profile(profile).await?);
            self.save_raw(&data).await
        })
        .await
    }

    async fn list_profiles(&self, provider: Option<&str>) -> Result<Vec<OAuthProfile>, SecurityError> {
        self.with_lock(|| async move {
            let data = self.load_raw().await?;
            let mut out = Vec::new();
            for profile in data.profiles {
                if provider.is_none_or(|p| p == profile.provider) {
                    out.push(self.hydrate_profile(profile).await?);
                }
            }
            Ok(out)
        })
        .await
    }

    async fn delete_profile(&self, provider: &str, name: &str) -> Result<bool, SecurityError> {
        self.with_lock(|| async move {
            let mut data = self.load_raw().await?;
            let before = data.profiles.len();
            data.profiles
                .retain(|p| !(p.provider == provider && p.profile_name == name));
            if data.active.get(provider).is_some_and(|active| active == name) {
                data.active.remove(provider);
            }
            let removed = data.profiles.len() != before;
            self.save_raw(&data).await?;
            Ok(removed)
        })
        .await
    }

    async fn get_active(&self, provider: &str) -> Result<Option<String>, SecurityError> {
        self.with_lock(|| async move {
            let data = self.load_raw().await?;
            Ok(data.active.get(provider).cloned())
        })
        .await
    }

    async fn set_active(&self, provider: &str, name: &str) -> Result<(), SecurityError> {
        self.with_lock(|| async move {
            let mut data = self.load_raw().await?;
            let exists = data
                .profiles
                .iter()
                .any(|p| p.provider == provider && p.profile_name == name);
            if !exists {
                return Err(SecurityError::InvalidPath(format!(
                    "oauth profile not found: {provider}/{name}"
                )));
            }
            data.active.insert(provider.to_string(), name.to_string());
            self.save_raw(&data).await
        })
        .await
    }
}

fn load_profiles_file(path: &Path) -> Result<StoredProfiles, SecurityError> {
    if !path.exists() {
        return Ok(StoredProfiles::default());
    }
    let raw = std::fs::read_to_string(path).map_err(|err| SecurityError::EncryptionFailed(err.to_string()))?;
    serde_json::from_str(&raw).map_err(|err| SecurityError::EncryptionFailed(err.to_string()))
}

fn save_profiles_file(path: &Path, data: &StoredProfiles) -> Result<(), SecurityError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|err| SecurityError::EncryptionFailed(err.to_string()))?;
    }
    let tmp = path.with_extension("json.tmp");
    let encoded =
        serde_json::to_string_pretty(data).map_err(|err| SecurityError::EncryptionFailed(err.to_string()))?;
    std::fs::write(&tmp, encoded).map_err(|err| SecurityError::EncryptionFailed(err.to_string()))?;
    std::fs::rename(tmp, path).map_err(|err| SecurityError::EncryptionFailed(err.to_string()))
}

pub mod testing {
    use super::*;

    #[derive(Clone, Default)]
    pub struct InMemoryOAuthProfileStore {
        profiles: Arc<RwLock<Vec<OAuthProfile>>>,
        active: Arc<RwLock<HashMap<String, String>>>,
    }

    #[async_trait]
    impl OAuthProfileStore for InMemoryOAuthProfileStore {
        async fn get_profile(
            &self,
            provider: &str,
            name: &str,
        ) -> Result<Option<OAuthProfile>, SecurityError> {
            let guard = self.profiles.read().await;
            Ok(guard
                .iter()
                .find(|p| p.provider == provider && p.profile_name == name)
                .cloned())
        }

        async fn store_profile(&self, profile: OAuthProfile) -> Result<(), SecurityError> {
            let mut guard = self.profiles.write().await;
            guard.retain(|p| !(p.provider == profile.provider && p.profile_name == profile.profile_name));
            guard.push(profile);
            Ok(())
        }

        async fn list_profiles(&self, provider: Option<&str>) -> Result<Vec<OAuthProfile>, SecurityError> {
            let guard = self.profiles.read().await;
            Ok(guard
                .iter()
                .filter(|p| provider.is_none_or(|provider| provider == p.provider))
                .cloned()
                .collect())
        }

        async fn delete_profile(&self, provider: &str, name: &str) -> Result<bool, SecurityError> {
            let mut guard = self.profiles.write().await;
            let before = guard.len();
            guard.retain(|p| !(p.provider == provider && p.profile_name == name));
            let removed = before != guard.len();
            if removed {
                let mut active = self.active.write().await;
                if active.get(provider).is_some_and(|candidate| candidate == name) {
                    active.remove(provider);
                }
            }
            Ok(removed)
        }

        async fn get_active(&self, provider: &str) -> Result<Option<String>, SecurityError> {
            Ok(self.active.read().await.get(provider).cloned())
        }

        async fn set_active(&self, provider: &str, name: &str) -> Result<(), SecurityError> {
            let profiles = self.profiles.read().await;
            let exists = profiles
                .iter()
                .any(|p| p.provider == provider && p.profile_name == name);
            drop(profiles);
            if !exists {
                return Err(SecurityError::InvalidPath(format!(
                    "oauth profile not found: {provider}/{name}"
                )));
            }
            self.active
                .write()
                .await
                .insert(provider.to_string(), name.to_string());
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::InMemorySecretStore;

    fn sample_profile() -> OAuthProfile {
        OAuthProfile {
            id: "openai-codex:default".to_string(),
            provider: "openai-codex".to_string(),
            profile_name: "default".to_string(),
            account_id: Some("acct_123".to_string()),
            token_set: super::super::token::TokenSet {
                access_token: "access-plain".to_string(),
                refresh_token: Some("refresh-plain".to_string()),
                id_token: Some("id-plain".to_string()),
                expires_at: 1_800_000_000,
                token_type: Some("Bearer".to_string()),
                scope: Some("openid".to_string()),
            },
            created_at: 1_700_000_000,
            updated_at: 1_700_000_000,
        }
    }

    #[tokio::test]
    async fn file_store_roundtrip_active_and_delete() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = FileOAuthProfileStore::new(dir.path(), None);

        let profile = sample_profile();
        store
            .store_profile(profile.clone())
            .await
            .expect("store profile");

        let fetched = store
            .get_profile("openai-codex", "default")
            .await
            .expect("get")
            .expect("exists");
        assert_eq!(fetched.profile_name, profile.profile_name);

        store
            .set_active("openai-codex", "default")
            .await
            .expect("set active");
        assert_eq!(
            store.get_active("openai-codex").await.expect("active"),
            Some("default".to_string())
        );

        assert!(store
            .delete_profile("openai-codex", "default")
            .await
            .expect("delete"));
        assert!(store
            .get_profile("openai-codex", "default")
            .await
            .expect("get")
            .is_none());
    }

    #[tokio::test]
    async fn file_store_encrypts_token_fields_when_secret_store_present() {
        let dir = tempfile::tempdir().expect("tempdir");
        let secrets: Arc<dyn SecretStore> = Arc::new(InMemorySecretStore::new());
        let store = FileOAuthProfileStore::new(dir.path(), Some(secrets));

        let profile = sample_profile();
        store.store_profile(profile).await.expect("store profile");

        let raw = std::fs::read_to_string(dir.path().join("oauth-profiles.json")).expect("read");
        assert!(raw.contains("vault:oauth:openai-codex:default:access_token"));

        let fetched = store
            .get_profile("openai-codex", "default")
            .await
            .expect("get")
            .expect("exists");
        assert_eq!(fetched.token_set.access_token, "access-plain");
        assert_eq!(fetched.token_set.refresh_token.as_deref(), Some("refresh-plain"));
    }
}
