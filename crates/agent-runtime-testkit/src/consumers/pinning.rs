//! Neutral release-eligibility check for captured Cargo dependency tables.

use serde_json::Value;

/// Requires every runtime package in captured dependency tables to use the
/// same immutable Git rev/tag or exact registry version. A Cargo.lock commit
/// does not make a moving manifest pin an adoption contract. Path overrides
/// are local development aids and cannot qualify a released consumer.
pub fn check_shared_runtime_pin(dependencies: &Value) -> Result<String, String> {
    let tables = dependencies
        .as_object()
        .ok_or("dependency table required")?;
    let mut shared = None;
    for (name, table) in tables {
        if !matches!(
            name.as_str(),
            "agent-runtime" | "agent-runtime-core" | "agent-runtime-testkit"
        ) {
            continue;
        }
        if table.get("path").is_some() || table.get("branch").is_some() {
            return Err(format!(
                "{name}: local path or moving branch is not release eligible"
            ));
        }
        let pin = if let Some(git) = table.get("git").and_then(Value::as_str) {
            if let Some(rev) = table.get("rev").and_then(Value::as_str) {
                if !(7..=40).contains(&rev.len()) || !rev.bytes().all(|b| b.is_ascii_hexdigit()) {
                    return Err(format!("{name}: Git rev must be an exact commit"));
                }
                format!("{git}#rev:{rev}")
            } else if let Some(tag) = table.get("tag").and_then(Value::as_str) {
                if !tag.strip_prefix('v').is_some_and(exact_version) {
                    return Err(format!("{name}: immutable release tag required"));
                }
                format!("{git}#tag:{tag}")
            } else {
                return Err(format!("{name}: exact Git rev/tag required"));
            }
        } else {
            let version = table
                .as_str()
                .or_else(|| table.get("version").and_then(Value::as_str))
                .ok_or("version required")?;
            let exact = version
                .strip_prefix('=')
                .ok_or("exact registry version required")?;
            if !exact_version(exact) {
                return Err(format!("{name}: exact registry version required"));
            }
            format!("registry:{exact}")
        };
        if shared.as_ref().is_some_and(|previous| previous != &pin) {
            return Err("runtime packages must adopt one shared revision".into());
        }
        shared = Some(pin);
    }
    shared.ok_or_else(|| "no runtime dependency found".into())
}

fn exact_version(value: &str) -> bool {
    let components = value.split('.').collect::<Vec<_>>();
    components.len() == 3
        && components
            .iter()
            .all(|part| !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit()))
}
