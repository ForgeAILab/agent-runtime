//! Layer 2 tool contracts, registries, and built-in tools.
//!
//! `nyx-tools` defines the `Tool` trait, `ToolContext`, tool catalog services,
//! registry helpers, and feature-gated built-ins. Tools receive pre-built
//! context and use control-plane services for cross-capability access.

mod builtins;
mod catalog_service;
mod core;
#[cfg(feature = "cost")]
mod cost;
#[cfg(feature = "mcp")]
mod mcp;
#[cfg(unix)]
mod pty;
mod registry;
#[cfg(feature = "permission")]
mod request_permission;
#[cfg(feature = "session")]
mod session;
mod skill_tool;
#[cfg(feature = "sub-agent")]
mod sub_agent_tool;
mod terminal;
pub mod testing;
mod tool_tool;
#[cfg(any(feature = "web-search-brave", feature = "web-search-tavily"))]
mod web_search;

use std::sync::Arc;

use nyx_security::Sandbox;

pub use builtins::*;
pub use catalog_service::*;
pub use core::*;
#[cfg(feature = "cost")]
pub use cost::*;
#[cfg(feature = "mcp")]
pub use mcp::*;
pub use registry::*;
#[cfg(feature = "permission")]
pub use request_permission::*;
#[cfg(feature = "session")]
pub use session::*;
pub use skill_tool::*;
#[cfg(feature = "sub-agent")]
pub use sub_agent_tool::*;
pub use terminal::*;
pub use tool_tool::*;
#[cfg(any(feature = "web-search-brave", feature = "web-search-tavily"))]
pub use web_search::*;

pub fn build_tools(sandbox: Arc<dyn Sandbox>) -> Result<Vec<Arc<dyn Tool>>, RegistryError> {
    #[cfg(feature = "terminal")]
    {
        build_tools_with_terminal_registry(sandbox, Arc::new(TerminalRegistry::new()))
    }
    #[cfg(not(feature = "terminal"))]
    {
        let mut registry = ToolRegistry::new();
        register_builtins(&mut registry, sandbox)?;
        #[cfg(feature = "permission")]
        registry.register(Arc::new(RequestPermissionTool::default()))?;
        #[cfg(feature = "session")]
        registry.register(Arc::new(SessionTool))?;
        registry.register(Arc::new(ToolTool))?;
        Ok(registry.seal())
    }
}

#[cfg(feature = "terminal")]
pub fn build_tools_with_terminal_registry(
    sandbox: Arc<dyn Sandbox>,
    terminal_registry: Arc<TerminalRegistry>,
) -> Result<Vec<Arc<dyn Tool>>, RegistryError> {
    let mut registry = ToolRegistry::new();
    register_builtins(&mut registry, sandbox, terminal_registry)?;
    #[cfg(feature = "permission")]
    registry.register(Arc::new(RequestPermissionTool::default()))?;
    #[cfg(feature = "session")]
    registry.register(Arc::new(SessionTool))?;
    registry.register(Arc::new(ToolTool))?;
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
        #[cfg(feature = "terminal")]
        assert!(names.contains("process"), "missing process tool");
        #[cfg(feature = "http")]
        assert!(names.contains("http"), "missing http tool");
        #[cfg(feature = "sub-agent")]
        assert!(names.contains("sub_agent"), "missing sub_agent tool");
        #[cfg(feature = "permission")]
        assert!(
            names.contains("request_permission"),
            "missing request_permission tool"
        );
        #[cfg(feature = "session")]
        assert!(names.contains("session"), "missing session tool");
    }
}
