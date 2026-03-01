use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::process::Command;

use crate::{
    Sandbox, SandboxError, SandboxPolicy, SandboxedChild, SandboxedCommand, SandboxedOutput,
    SecretStore, SecurityError,
};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SecurityConfig {
    #[serde(default = "default_sandbox_kind")]
    pub sandbox: String,
    #[serde(default = "default_secret_store_kind")]
    pub secret_store: String,
    #[serde(default = "default_operators")]
    pub operators: Vec<String>,
    #[serde(default)]
    pub policy: SandboxPolicy,
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            sandbox: default_sandbox_kind(),
            secret_store: default_secret_store_kind(),
            operators: default_operators(),
            policy: SandboxPolicy::default(),
        }
    }
}

pub fn default_sandbox_kind() -> String {
    "os".to_string()
}

pub fn default_secret_store_kind() -> String {
    "encrypted".to_string()
}

pub fn default_operators() -> Vec<String> {
    vec!["local".to_string()]
}

pub fn build_sandbox(cfg: &SecurityConfig) -> Result<Arc<dyn Sandbox>, SecurityError> {
    let root = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    build_sandbox_at_root(cfg, root)
}

pub fn build_sandbox_at_root(
    cfg: &SecurityConfig,
    root_dir: impl Into<PathBuf>,
) -> Result<Arc<dyn Sandbox>, SecurityError> {
    let root_dir = root_dir.into();
    #[cfg(not(feature = "os-sandbox"))]
    let _ = &root_dir;
    let sandbox: Arc<dyn Sandbox> = match cfg.sandbox.as_str() {
        "none" => {
            tracing::warn!("security.sandbox=none configured; sandbox protections are disabled");
            Arc::new(NoSandbox)
        }
        "noop" => {
            tracing::warn!(
                "security.sandbox=noop configured; shell/file tools will report empty outputs"
            );
            Arc::new(crate::testing::NoopSandbox)
        }
        #[cfg(feature = "os-sandbox")]
        "os" => Arc::new(crate::os_sandbox::OsSandbox::new(
            root_dir,
            cfg.policy.clone(),
        )?),
        other => return Err(SecurityError::UnknownKind(other.to_string())),
    };

    tracing::debug!(kind = cfg.sandbox.as_str(), "building sandbox");
    Ok(sandbox)
}

pub fn build_secret_store(cfg: &SecurityConfig) -> Result<Arc<dyn SecretStore>, SecurityError> {
    let store: Arc<dyn SecretStore> = match cfg.secret_store.as_str() {
        #[cfg(feature = "encrypted")]
        "encrypted" => {
            let key_path = std::env::var_os("HOME")
                .map(|h| PathBuf::from(h).join(".nyx").join(".secret_key"))
                .unwrap_or_else(|| PathBuf::from(".nyx").join(".secret_key"));
            Arc::new(crate::encrypted::EncryptedSecretStore::from_env_or_file(
                &key_path,
            )?)
        }
        "in-memory" => Arc::new(crate::testing::InMemorySecretStore::new()),
        other => return Err(SecurityError::UnknownKind(other.to_string())),
    };

    tracing::debug!(kind = cfg.secret_store.as_str(), "building secret store");
    Ok(store)
}

#[derive(Debug, Default)]
struct NoSandbox;

#[async_trait]
impl Sandbox for NoSandbox {
    async fn execute(&self, cmd: SandboxedCommand) -> Result<SandboxedOutput, SandboxError> {
        let mut command = Command::new(&cmd.program);
        command.args(&cmd.args);
        command.current_dir(&cmd.working_dir);
        command.envs(&cmd.env);
        let output = command.output().await?;
        Ok(SandboxedOutput {
            status: output.status.code().unwrap_or_default(),
            stdout: output.stdout,
            stderr: output.stderr,
        })
    }

    async fn spawn_piped(&self, cmd: SandboxedCommand) -> Result<SandboxedChild, SandboxError> {
        let mut command = Command::new(&cmd.program);
        command.args(&cmd.args);
        command.current_dir(&cmd.working_dir);
        command.envs(&cmd.env);
        command.stdin(std::process::Stdio::piped());
        command.stdout(std::process::Stdio::piped());
        command.stderr(std::process::Stdio::piped());

        let mut child = command.spawn()?;
        let stdin = child
            .stdin
            .take()
            .ok_or(SandboxError::UnsupportedInteractiveSpawn)?;
        let stdout = child
            .stdout
            .take()
            .ok_or(SandboxError::UnsupportedInteractiveSpawn)?;
        let stderr = child
            .stderr
            .take()
            .ok_or(SandboxError::UnsupportedInteractiveSpawn)?;

        Ok(SandboxedChild {
            child,
            stdin,
            stdout,
            stderr,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::SecurityConfig;

    #[test]
    fn deserializes_security_policy_section() {
        let raw = r#"
            [security]
            sandbox = "os"
            secret_store = "encrypted"

            [security.policy]
            command_allowlist = ["git", "cargo", "python3"]
            command_denylist = ["rm", "curl"]
            allowed_read_paths = ["/usr/share", "/tmp"]
            allowed_write_paths = ["/tmp/nyx-output"]
            env_passthrough = ["PATH", "HOME", "RUST_LOG"]
            max_execution_secs = 60
        "#;

        #[derive(serde::Deserialize)]
        struct Root {
            security: SecurityConfig,
        }

        let cfg: Root = toml::from_str(raw).expect("security section should deserialize");
        assert_eq!(
            cfg.security.policy.command_allowlist,
            Some(vec![
                "git".to_string(),
                "cargo".to_string(),
                "python3".to_string()
            ])
        );
        assert_eq!(
            cfg.security.policy.command_denylist,
            vec!["rm".to_string(), "curl".to_string()]
        );
        assert_eq!(cfg.security.policy.max_execution_secs, 60);
    }

    #[test]
    fn missing_policy_uses_defaults() {
        let raw = r#"
            [security]
            sandbox = "os"
            secret_store = "encrypted"
        "#;

        #[derive(serde::Deserialize)]
        struct Root {
            security: SecurityConfig,
        }

        let cfg: Root = toml::from_str(raw).expect("security section should deserialize");
        assert_eq!(cfg.security.policy.command_allowlist, None);
        assert!(cfg.security.policy.command_denylist.is_empty());
        assert_eq!(cfg.security.policy.max_execution_secs, 120);
    }

    #[test]
    fn accepts_none_sandbox_kind() {
        let raw = r#"
            [security]
            sandbox = "none"
            secret_store = "encrypted"
        "#;

        #[derive(serde::Deserialize)]
        struct Root {
            security: SecurityConfig,
        }

        let cfg: Root = toml::from_str(raw).expect("security section should deserialize");
        assert_eq!(cfg.security.sandbox, "none");
    }
}
