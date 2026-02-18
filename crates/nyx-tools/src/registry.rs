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
