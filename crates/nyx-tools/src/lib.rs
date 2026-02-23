mod builtins;
mod catalog_service;
mod core;
#[cfg(feature = "mcp")]
mod mcp;
mod registry;
mod skill_tool;
mod terminal;
pub mod testing;
#[cfg(feature = "workflow")]
mod workflow;

use std::sync::Arc;

use nyx_security::Sandbox;

pub use builtins::*;
pub use catalog_service::*;
pub use core::*;
#[cfg(feature = "mcp")]
pub use mcp::*;
pub use registry::*;
pub use skill_tool::*;
pub use terminal::*;
#[cfg(feature = "workflow")]
pub use workflow::*;

pub fn build_tools(sandbox: Arc<dyn Sandbox>) -> Result<Vec<Arc<dyn Tool>>, RegistryError> {
    let mut registry = ToolRegistry::new();
    register_builtins(&mut registry, sandbox)?;
    Ok(registry.seal())
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use nyx_security::testing::NoopSandbox;

    use crate::build_tools;

    #[test]
    fn build_tools_returns_expected_builtins() {
        let tools = build_tools(Arc::new(NoopSandbox)).expect("build builtin tools");
        assert!(
            !tools.is_empty(),
            "build_tools should return at least one tool"
        );

        let names = tools
            .iter()
            .map(|tool| tool.name().to_string())
            .collect::<HashSet<_>>();

        #[cfg(feature = "file")]
        assert!(names.contains("read"), "missing read tool");
        #[cfg(feature = "shell")]
        assert!(names.contains("shell"), "missing shell tool");
        #[cfg(feature = "http")]
        assert!(names.contains("http"), "missing http tool");
        assert!(names.contains("skill"), "missing skill tool");
    }
}
