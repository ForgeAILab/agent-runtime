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

#[derive(Clone, Default)]
pub struct SealedToolRegistry {
    names: HashSet<String>,
    tools: Vec<Arc<dyn Tool>>,
}

impl SealedToolRegistry {
    pub fn len(&self) -> usize {
        self.tools.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tools.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = &Arc<dyn Tool>> {
        self.tools.iter()
    }

    pub fn tools(&self) -> &[Arc<dyn Tool>] {
        &self.tools
    }

    pub fn to_vec(&self) -> Vec<Arc<dyn Tool>> {
        self.tools.clone()
    }

    pub fn into_tools(self) -> Vec<Arc<dyn Tool>> {
        self.tools
    }

    pub fn contains(&self, name: &str) -> bool {
        self.names.contains(name)
    }

    pub fn get(&self, name: &str) -> Option<Arc<dyn Tool>> {
        self.tools.iter().find(|tool| tool.name() == name).cloned()
    }
}

impl std::ops::Deref for SealedToolRegistry {
    type Target = [Arc<dyn Tool>];

    fn deref(&self) -> &Self::Target {
        self.tools()
    }
}

impl IntoIterator for SealedToolRegistry {
    type Item = Arc<dyn Tool>;
    type IntoIter = std::vec::IntoIter<Arc<dyn Tool>>;

    fn into_iter(self) -> Self::IntoIter {
        self.tools.into_iter()
    }
}

impl<'a> IntoIterator for &'a SealedToolRegistry {
    type Item = &'a Arc<dyn Tool>;
    type IntoIter = std::slice::Iter<'a, Arc<dyn Tool>>;

    fn into_iter(self) -> Self::IntoIter {
        self.tools.iter()
    }
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

    pub fn register_all(
        &mut self,
        tools: impl IntoIterator<Item = Arc<dyn Tool>>,
    ) -> Result<(), RegistryError> {
        for tool in tools {
            self.register(tool)?;
        }
        Ok(())
    }

    pub fn seal(self) -> SealedToolRegistry {
        SealedToolRegistry {
            names: self.names,
            tools: self.tools,
        }
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
    async fn register_all_fails_duplicate_startup_tool_name() {
        let mut registry = ToolRegistry::new();

        let err = registry
            .register_all(vec![
                Arc::new(testing::NoopTool::named("same")) as Arc<dyn crate::Tool>,
                Arc::new(testing::NoopTool::named("same")) as Arc<dyn crate::Tool>,
            ])
            .expect_err("duplicate startup tool should fail");

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

        let sealed = registry.seal();
        let names = sealed
            .iter()
            .map(|tool| tool.name().to_string())
            .collect::<Vec<_>>();
        assert_eq!(names, vec!["first", "second"]);
        assert!(sealed.contains("first"));
    }
}
