//! Existing operation/preparation fingerprints serialize opaque JSON Value
//! maps. Preserve their order for hosts unifying serde_json/preserve_order,
//! while keeping actual envelope/payload object keys lexically canonical.
use super::{JournalObjectKind, RuntimeError, Value, conflict};
use serde::{Deserialize, Serialize};

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(untagged)]
enum PathPart {
    Key(String),
    Index(usize),
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ObjectOrder {
    path: Vec<PathPart>,
    keys: Vec<String>,
}

fn roots(kind: JournalObjectKind, value: &Value) -> Vec<Vec<PathPart>> {
    use PathPart::{Index, Key};
    match kind {
        JournalObjectKind::StateValue => vec![Vec::new()],
        JournalObjectKind::Extension => vec![vec![Key("value".to_owned())]],
        JournalObjectKind::RequestSettings => vec![
            vec![Key("vendor_extensions".to_owned())],
            vec![
                Key("structured_output".to_owned()),
                Key("schema".to_owned()),
            ],
        ],
        JournalObjectKind::ToolSchema => vec![vec![Key("input_schema".to_owned())]],
        JournalObjectKind::Message => {
            let mut roots = Vec::new();
            let mut pending = vec![(value.get("content"), vec![Key("content".to_owned())])];
            while let Some((Some(Value::Array(parts)), path)) = pending.pop() {
                for (i, part) in parts.iter().enumerate() {
                    let mut path = path.clone();
                    path.push(Index(i));
                    match part.get("type").and_then(Value::as_str) {
                        Some("tool_call") => {
                            path.push(Key("arguments".to_owned()));
                            roots.push(path);
                        }
                        Some("tool_result") => {
                            path.push(Key("content".to_owned()));
                            pending.push((part.get("content"), path));
                        }
                        _ => {}
                    }
                }
            }
            roots
        }
        _ => Vec::new(),
    }
}

fn at_path<'a>(mut value: &'a Value, path: &[PathPart]) -> Option<&'a Value> {
    for part in path {
        value = match part {
            PathPart::Key(key) => value.get(key)?,
            PathPart::Index(i) => value.get(*i)?,
        };
    }
    Some(value)
}
fn at_path_mut<'a>(mut value: &'a mut Value, path: &[PathPart]) -> Option<&'a mut Value> {
    for part in path {
        value = match part {
            PathPart::Key(key) => value.get_mut(key)?,
            PathPart::Index(i) => value.get_mut(*i)?,
        };
    }
    Some(value)
}

fn visit(value: &Value, path: &mut Vec<PathPart>, orders: &mut Vec<ObjectOrder>) {
    match value {
        Value::Object(map) => {
            let keys: Vec<_> = map.keys().cloned().collect();
            if keys.windows(2).any(|pair| pair[0] >= pair[1]) {
                orders.push(ObjectOrder {
                    path: path.clone(),
                    keys,
                });
            }
            let mut entries: Vec<_> = map.iter().collect();
            entries.sort_unstable_by_key(|(key, _)| *key);
            for (key, value) in entries {
                path.push(PathPart::Key(key.clone()));
                visit(value, path, orders);
                path.pop();
            }
        }
        Value::Array(values) => {
            for (i, value) in values.iter().enumerate() {
                path.push(PathPart::Index(i));
                visit(value, path, orders);
                path.pop();
            }
        }
        _ => {}
    }
}

pub(super) fn capture(kind: JournalObjectKind, value: &Value) -> Vec<ObjectOrder> {
    let mut orders = Vec::new();
    for mut path in roots(kind, value) {
        if let Some(value) = at_path(value, &path) {
            visit(value, &mut path, &mut orders);
        }
    }
    orders.sort_unstable_by(|a, b| a.path.cmp(&b.path));
    orders
}

pub(super) fn restore(
    kind: JournalObjectKind,
    value: &mut Value,
    orders: &[ObjectOrder],
) -> Result<(), RuntimeError> {
    let roots = roots(kind, value);
    let mut previous: Option<&[PathPart]> = None;
    for order in orders {
        if !roots.iter().any(|root| order.path.starts_with(root))
            || previous.is_some_and(|path| path >= order.path.as_slice())
            || order.keys.windows(2).all(|pair| pair[0] < pair[1])
        {
            return Err(conflict());
        }
        previous = Some(&order.path);
        let Some(Value::Object(map)) = at_path_mut(value, &order.path) else {
            return Err(conflict());
        };
        if map.len() != order.keys.len() {
            return Err(conflict());
        }
        let mut original = std::mem::take(map);
        for key in &order.keys {
            map.insert(key.clone(), original.remove(key).ok_or_else(conflict)?);
        }
        if !original.is_empty() || !map.keys().eq(order.keys.iter()) {
            // A host without preserve_order cannot execute this old ordering
            // equivalently. Reject rather than changing operation fingerprints.
            return Err(conflict());
        }
    }
    Ok(())
}
