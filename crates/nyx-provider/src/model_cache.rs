#[cfg(feature = "model-cache-sqlite")]
use std::sync::{Arc, Mutex};
#[cfg(feature = "model-cache-sqlite")]
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(feature = "model-cache-sqlite")]
use crate::ModelInfo;

#[cfg(feature = "model-cache-sqlite")]
const NYX_PROVIDER_MODELS_NAMESPACE: &str = "nyx-provider-models";
#[cfg(feature = "model-cache-sqlite")]
const NYX_PROVIDER_MODEL_MIGRATIONS: &[(u32, &str, &str)] = &[(
    1,
    "create model metadata table",
    r#"
    CREATE TABLE IF NOT EXISTS model_metadata (
        model_id TEXT PRIMARY KEY,
        context_window INTEGER NOT NULL,
        max_output_tokens INTEGER,
        supports_vision INTEGER NOT NULL DEFAULT 0,
        supports_tool_use INTEGER NOT NULL DEFAULT 1,
        supports_streaming INTEGER NOT NULL DEFAULT 1,
        fetched_at TEXT NOT NULL,
        source TEXT NOT NULL
    );
    "#,
)];

#[cfg(feature = "model-cache-sqlite")]
pub fn model_cache_migrations() -> (&'static str, &'static [(u32, &'static str, &'static str)]) {
    (NYX_PROVIDER_MODELS_NAMESPACE, NYX_PROVIDER_MODEL_MIGRATIONS)
}

#[cfg(feature = "model-cache-sqlite")]
#[derive(Debug, Clone)]
pub struct SqliteModelCache {
    connection: Arc<Mutex<rusqlite::Connection>>,
}

#[cfg(feature = "model-cache-sqlite")]
impl SqliteModelCache {
    pub fn new(connection: Arc<Mutex<rusqlite::Connection>>) -> Self {
        Self { connection }
    }

    pub fn get(&self, model_id: &str) -> Result<Option<ModelInfo>, rusqlite::Error> {
        let conn = self
            .connection
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        let mut stmt = conn.prepare(
            "SELECT model_id, context_window, max_output_tokens, supports_vision, supports_tool_use, supports_streaming
             FROM model_metadata
             WHERE model_id = ?1",
        )?;
        let mut rows = stmt.query(rusqlite::params![model_id])?;
        let Some(row) = rows.next()? else {
            return Ok(None);
        };
        Ok(Some(ModelInfo {
            model_id: row.get(0)?,
            context_window: row.get::<_, i64>(1)? as usize,
            max_output_tokens: row.get::<_, Option<i64>>(2)?.map(|value| value as usize),
            supports_vision: row.get::<_, i64>(3)? != 0,
            supports_tool_use: row.get::<_, i64>(4)? != 0,
            supports_streaming: row.get::<_, i64>(5)? != 0,
            context_budget_ratio: None,
        }))
    }

    pub fn put(&self, model_id: &str, info: &ModelInfo) -> Result<(), rusqlite::Error> {
        let conn = self
            .connection
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        conn.execute(
            "INSERT INTO model_metadata (
                model_id, context_window, max_output_tokens, supports_vision,
                supports_tool_use, supports_streaming, fetched_at, source
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
            ON CONFLICT(model_id) DO UPDATE SET
                context_window = excluded.context_window,
                max_output_tokens = excluded.max_output_tokens,
                supports_vision = excluded.supports_vision,
                supports_tool_use = excluded.supports_tool_use,
                supports_streaming = excluded.supports_streaming,
                fetched_at = excluded.fetched_at,
                source = excluded.source",
            rusqlite::params![
                model_id,
                info.context_window as i64,
                info.max_output_tokens.map(|value| value as i64),
                if info.supports_vision { 1 } else { 0 },
                if info.supports_tool_use { 1 } else { 0 },
                if info.supports_streaming { 1 } else { 0 },
                current_timestamp(),
                "remote",
            ],
        )?;
        Ok(())
    }

    pub fn put_batch<I>(&self, entries: I) -> Result<(), rusqlite::Error>
    where
        I: IntoIterator<Item = (String, ModelInfo)>,
    {
        for (model_id, info) in entries {
            self.put(&model_id, &info)?;
        }
        Ok(())
    }
}

#[cfg(feature = "model-cache-sqlite")]
fn current_timestamp() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .to_string()
}

#[cfg(all(test, feature = "model-cache-sqlite"))]
mod tests {
    use nyx_store::NyxDb;

    use super::{SqliteModelCache, model_cache_migrations};
    use crate::ModelInfo;

    fn test_cache() -> SqliteModelCache {
        let db =
            NyxDb::in_memory(&[nyx_store::migrations(), model_cache_migrations()]).expect("db");
        SqliteModelCache::new(db.connection())
    }

    #[test]
    fn sqlite_model_cache_put_and_get() {
        let cache = test_cache();
        let info = ModelInfo {
            model_id: "custom-model".to_string(),
            context_window: 32_000,
            max_output_tokens: Some(4_000),
            supports_vision: true,
            supports_tool_use: true,
            supports_streaming: true,
            context_budget_ratio: None,
        };

        cache.put("custom-model", &info).expect("put");
        assert_eq!(cache.get("custom-model").expect("get"), Some(info));
    }

    #[test]
    fn sqlite_model_cache_overwrites_existing_entries() {
        let cache = test_cache();
        let info_v1 = ModelInfo {
            model_id: "model-x".to_string(),
            context_window: 32_000,
            max_output_tokens: Some(2_048),
            supports_vision: false,
            supports_tool_use: true,
            supports_streaming: true,
            context_budget_ratio: None,
        };
        let info_v2 = ModelInfo {
            context_window: 64_000,
            supports_vision: true,
            ..info_v1.clone()
        };

        cache.put("model-x", &info_v1).expect("put v1");
        cache.put("model-x", &info_v2).expect("put v2");

        assert_eq!(cache.get("model-x").expect("get"), Some(info_v2));
    }

    #[test]
    fn sqlite_model_cache_returns_none_for_missing_key() {
        let cache = test_cache();
        assert_eq!(cache.get("missing-model").expect("get"), None);
    }

    #[test]
    fn sqlite_model_cache_put_batch_persists_all_entries() {
        let cache = test_cache();
        cache
            .put_batch([
                (
                    "model-a".to_string(),
                    ModelInfo {
                        model_id: "model-a".to_string(),
                        context_window: 8_000,
                        max_output_tokens: None,
                        supports_vision: false,
                        supports_tool_use: true,
                        supports_streaming: true,
                        context_budget_ratio: None,
                    },
                ),
                (
                    "model-b".to_string(),
                    ModelInfo {
                        model_id: "model-b".to_string(),
                        context_window: 16_000,
                        max_output_tokens: Some(2_000),
                        supports_vision: true,
                        supports_tool_use: true,
                        supports_streaming: false,
                        context_budget_ratio: None,
                    },
                ),
            ])
            .expect("put batch");

        assert!(cache.get("model-a").expect("get a").is_some());
        assert!(cache.get("model-b").expect("get b").is_some());
    }
}
