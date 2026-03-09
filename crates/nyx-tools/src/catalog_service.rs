use std::sync::Arc;

use async_trait::async_trait;
use nyx_core::ToolSpec;
use nyx_core::{InvocationContext, KernelError, ToolCatalogService, ToolSelection};

use crate::Tool;

pub struct ToolCatalogServiceImpl {
    tools: Vec<Arc<dyn Tool>>,
}

impl ToolCatalogServiceImpl {
    pub fn new(tools: Vec<Arc<dyn Tool>>) -> Self {
        Self { tools }
    }

    fn allowed(selection: &ToolSelection, name: &str) -> bool {
        if selection.deny.iter().any(|item| item == name) {
            return false;
        }
        if selection.allow.is_empty() {
            return true;
        }
        selection.allow.iter().any(|item| item == name)
    }

    fn visible_to_actor(_ctx: &InvocationContext, _name: &str) -> bool {
        true
    }

    fn to_spec(tool: &Arc<dyn Tool>) -> ToolSpec {
        ToolSpec {
            name: tool.name().to_string(),
            description: tool.description().to_string(),
            schema: tool.schema(),
            meta: serde_json::Map::new(),
        }
    }
}

#[async_trait]
impl ToolCatalogService for ToolCatalogServiceImpl {
    async fn list_specs(
        &self,
        ctx: &InvocationContext,
        selection: &ToolSelection,
    ) -> Result<Vec<ToolSpec>, KernelError> {
        Ok(self
            .tools
            .iter()
            .filter(|tool| Self::visible_to_actor(ctx, tool.name()))
            .filter(|tool| Self::allowed(selection, tool.name()))
            .map(Self::to_spec)
            .collect())
    }

    async fn get_spec(
        &self,
        ctx: &InvocationContext,
        name: &str,
    ) -> Result<Option<ToolSpec>, KernelError> {
        Ok(self
            .tools
            .iter()
            .find(|tool| tool.name() == name)
            .filter(|tool| Self::visible_to_actor(ctx, tool.name()))
            .map(Self::to_spec))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    struct DummyTool {
        name: &'static str,
    }

    #[async_trait]
    impl Tool for DummyTool {
        fn name(&self) -> &str {
            self.name
        }

        fn description(&self) -> &str {
            self.name
        }

        fn schema(&self) -> serde_json::Value {
            json!({"type":"object"})
        }

        async fn invoke(
            &self,
            _input: serde_json::Value,
            _ctx: &crate::ToolContext,
        ) -> Result<crate::ToolResult, crate::ToolError> {
            Ok(crate::ToolResult::empty())
        }
    }

    fn ctx() -> InvocationContext {
        InvocationContext {
            bot_id: "runtime".to_string(),
            session_id: None,
            actor: nyx_core::Actor::System,
            request_id: "req".to_string(),
            deadline_epoch_ms: None,
        }
    }

    #[tokio::test]
    async fn list_specs_filters_by_selection() {
        let svc = ToolCatalogServiceImpl::new(vec![
            Arc::new(DummyTool { name: "a" }),
            Arc::new(DummyTool { name: "b" }),
        ]);
        let specs = svc
            .list_specs(
                &ctx(),
                &ToolSelection {
                    allow: vec!["a".to_string()],
                    deny: Vec::new(),
                },
            )
            .await
            .expect("list specs");
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].name, "a");
    }
}
