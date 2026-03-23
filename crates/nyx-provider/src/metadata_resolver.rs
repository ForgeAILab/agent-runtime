use std::collections::HashMap;
use std::sync::Arc;

use crate::config::ProviderConfig;
use crate::{ModelInfo, ModelRegistry};

#[derive(Debug, Clone)]
pub struct ProviderMetadataResolver {
    registry: Arc<ModelRegistry>,
    provider_configs: HashMap<String, ProviderConfig>,
    default_provider_config: ProviderConfig,
}

impl ProviderMetadataResolver {
    pub fn new(
        registry: Arc<ModelRegistry>,
        provider_configs: HashMap<String, ProviderConfig>,
        default_provider_config: ProviderConfig,
    ) -> Self {
        Self {
            registry,
            provider_configs,
            default_provider_config,
        }
    }

    pub fn resolve(&self, provider_name: &str, model: &str) -> ModelInfo {
        let mut cfg = self
            .provider_configs
            .get(provider_name)
            .cloned()
            .unwrap_or_else(|| self.default_provider_config.clone());
        cfg.model = model.to_string();
        cfg.resolved_model_info(&self.registry)
    }

    pub fn resolve_default(&self) -> ModelInfo {
        self.default_provider_config
            .resolved_model_info(&self.registry)
    }

    pub fn registry(&self) -> &Arc<ModelRegistry> {
        &self.registry
    }

    pub fn provider_configs(&self) -> &HashMap<String, ProviderConfig> {
        &self.provider_configs
    }

    pub fn default_provider_config(&self) -> &ProviderConfig {
        &self.default_provider_config
    }
}
