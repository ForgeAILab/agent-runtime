mod builtins;
mod catalog_service;
mod core;
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
#[cfg(feature = "permission")]
pub use request_permission::*;
#[cfg(feature = "session")]
pub use session::*;
pub use skill_tool::*;
#[cfg(feature = "sub-agent")]
pub use sub_agent_tool::*;
pub use terminal::*;
#[cfg(feature = "workflow")]
pub use workflow::*;

pub fn build_tools(sandbox: Arc<dyn Sandbox>) -> Result<Vec<Arc<dyn Tool>>, RegistryError> {
    let mut registry = ToolRegistry::new();
    #[cfg(feature = "terminal")]
    register_builtins(&mut registry, sandbox, Arc::new(TerminalRegistry::new()))?;
    #[cfg(not(feature = "terminal"))]
    register_builtins(&mut registry, sandbox)?;
    #[cfg(feature = "permission")]
    registry.register(Arc::new(RequestPermissionTool::default()))?;
    #[cfg(feature = "session")]
    registry.register(Arc::new(SessionTool))?;
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
        assert!(
            names.contains("process_start"),
            "missing process_start tool"
        );
        #[cfg(feature = "terminal")]
        assert!(names.contains("process_wait"), "missing process_wait tool");
        #[cfg(feature = "http")]
        assert!(names.contains("http"), "missing http tool");
        assert!(names.contains("skill"), "missing skill tool");
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
