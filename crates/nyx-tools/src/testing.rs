use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::{Value, json};

use crate::{SubAgentError, SubAgentRunner, Tool, ToolContext, ToolError, ToolResult};

#[derive(Debug, Default)]
pub struct NoopTool {
    name: Option<String>,
}

impl NoopTool {
    pub fn named(name: impl Into<String>) -> Self {
        Self {
            name: Some(name.into()),
        }
    }
}

#[async_trait]
impl Tool for NoopTool {
    fn name(&self) -> &str {
        self.name.as_deref().unwrap_or("noop")
    }

    fn description(&self) -> &str {
        "No-op testing tool"
    }

    fn schema(&self) -> Value {
        json!({"type": "object"})
    }

    async fn invoke(&self, _input: Value, _ctx: &ToolContext) -> Result<ToolResult, ToolError> {
        Ok(ToolResult::empty())
    }
}

#[derive(Debug, Default)]
pub struct SpyTool {
    name: Option<String>,
    invocations: Arc<Mutex<Vec<Value>>>,
}

impl SpyTool {
    pub fn named(name: impl Into<String>) -> Self {
        Self {
            name: Some(name.into()),
            ..Self::default()
        }
    }

    pub fn invocations(&self) -> Vec<Value> {
        self.invocations
            .lock()
            .expect("invocations mutex poisoned")
            .clone()
    }
}

#[async_trait]
impl Tool for SpyTool {
    fn name(&self) -> &str {
        self.name.as_deref().unwrap_or("spy")
    }

    fn description(&self) -> &str {
        "Spy testing tool"
    }

    fn schema(&self) -> Value {
        json!({"type": "object"})
    }

    async fn invoke(&self, input: Value, _ctx: &ToolContext) -> Result<ToolResult, ToolError> {
        self.invocations
            .lock()
            .expect("invocations mutex poisoned")
            .push(input.clone());
        Ok(ToolResult::json(input))
    }
}

#[derive(Debug, Default)]
pub struct NoopSubAgentRunner;

#[async_trait]
impl SubAgentRunner for NoopSubAgentRunner {
    async fn run(
        &self,
        _prompt: String,
        _tools: Vec<Arc<dyn Tool>>,
        _max_turns: usize,
    ) -> Result<String, SubAgentError> {
        Ok("noop".to_string())
    }
}

#[derive(Debug, Clone)]
pub struct RecordedSubAgentCall {
    pub prompt: String,
    pub tool_names: Vec<String>,
    pub max_turns: usize,
}

#[derive(Debug, Default)]
pub struct RecordingSubAgentRunner {
    calls: Arc<Mutex<Vec<RecordedSubAgentCall>>>,
}

impl RecordingSubAgentRunner {
    pub fn calls(&self) -> Vec<RecordedSubAgentCall> {
        self.calls.lock().expect("calls mutex poisoned").clone()
    }
}

#[async_trait]
impl SubAgentRunner for RecordingSubAgentRunner {
    async fn run(
        &self,
        prompt: String,
        tools: Vec<Arc<dyn Tool>>,
        max_turns: usize,
    ) -> Result<String, SubAgentError> {
        let call = RecordedSubAgentCall {
            prompt,
            tool_names: tools.iter().map(|tool| tool.name().to_string()).collect(),
            max_turns,
        };
        self.calls.lock().expect("calls mutex poisoned").push(call);
        Ok("recorded".to_string())
    }
}
