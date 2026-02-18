use std::collections::HashSet;
use std::sync::Arc;

use thiserror::Error;

use crate::Tool;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum RegistryError {
    #[error("tool name conflict: {name}")]
    NameConflict { name: String },
}

#[derive(Default)]
pub struct ToolRegistry {
    names: HashSet<String>,
    tools: Vec<Arc<dyn Tool>>,
}

impl ToolRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, tool: Arc<dyn Tool>) -> Result<(), RegistryError> {
        let name = tool.name().to_string();
        if self.names.contains(&name) {
            return Err(RegistryError::NameConflict { name });
        }
        self.names.insert(name);
        self.tools.push(tool);
        Ok(())
    }

    pub fn register_all(&mut self, tools: Vec<Arc<dyn Tool>>) -> Result<(), RegistryError> {
        for tool in tools {
            self.register(tool)?;
        }
        Ok(())
    }

    pub fn seal(self) -> Vec<Arc<dyn Tool>> {
        self.tools
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{RegistryError, ToolRegistry};
    use crate::testing;

    #[tokio::test]
    async fn tool_registry_name_conflict() {
        let mut registry = ToolRegistry::new();
        registry
            .register(Arc::new(testing::NoopTool::named("same")))
            .expect("first register");

        let err = registry
            .register(Arc::new(testing::NoopTool::named("same")))
            .expect_err("duplicate should fail");
        assert!(matches!(err, RegistryError::NameConflict { .. }));
    }

    #[tokio::test]
    async fn tool_registry_seal_preserves_order() {
        let mut registry = ToolRegistry::new();
        registry
            .register(Arc::new(testing::NoopTool::named("first")))
            .expect("register first");
        registry
            .register(Arc::new(testing::NoopTool::named("second")))
            .expect("register second");

        let names = registry
            .seal()
            .iter()
            .map(|tool| tool.name().to_string())
            .collect::<Vec<_>>();
        assert_eq!(names, vec!["first", "second"]);
    }
}
