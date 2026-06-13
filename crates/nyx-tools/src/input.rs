use serde_json::{Map, Value};

use crate::ToolError;

fn missing(field: &str) -> ToolError {
    ToolError::InvalidInput(format!("missing {field}"))
}

fn invalid_type(field: &str, expected: &str) -> ToolError {
    ToolError::InvalidInput(format!("{field} must be {expected}"))
}

pub(crate) fn require_str<'a>(input: &'a Value, field: &str) -> Result<&'a str, ToolError> {
    match input.get(field) {
        Some(value) => value
            .as_str()
            .ok_or_else(|| invalid_type(field, "a string")),
        None => Err(missing(field)),
    }
}

pub(crate) fn optional_str<'a>(
    input: &'a Value,
    field: &str,
) -> Result<Option<&'a str>, ToolError> {
    match input.get(field) {
        Some(Value::Null) | None => Ok(None),
        Some(value) => value
            .as_str()
            .map(Some)
            .ok_or_else(|| invalid_type(field, "a string")),
    }
}

pub(crate) fn optional_bool(input: &Value, field: &str) -> Result<Option<bool>, ToolError> {
    match input.get(field) {
        Some(Value::Null) | None => Ok(None),
        Some(value) => value
            .as_bool()
            .map(Some)
            .ok_or_else(|| invalid_type(field, "a boolean")),
    }
}

pub(crate) fn optional_u64(input: &Value, field: &str) -> Result<Option<u64>, ToolError> {
    match input.get(field) {
        Some(Value::Null) | None => Ok(None),
        Some(value) => value
            .as_u64()
            .map(Some)
            .ok_or_else(|| invalid_type(field, "a non-negative integer")),
    }
}

pub(crate) fn optional_object<'a>(
    input: &'a Value,
    field: &str,
) -> Result<Option<&'a Map<String, Value>>, ToolError> {
    match input.get(field) {
        Some(Value::Null) | None => Ok(None),
        Some(value) => value
            .as_object()
            .map(Some)
            .ok_or_else(|| invalid_type(field, "an object")),
    }
}

pub(crate) fn optional_string_array(
    input: &Value,
    field: &str,
) -> Result<Option<Vec<String>>, ToolError> {
    match input.get(field) {
        None => Ok(None),
        Some(value) => string_array(value, field).map(Some),
    }
}

pub(crate) fn nullable_string(
    input: &Value,
    field: &str,
) -> Result<Option<Option<String>>, ToolError> {
    match input.get(field) {
        None => Ok(None),
        Some(Value::Null) => Ok(Some(None)),
        Some(value) => value
            .as_str()
            .map(|value| Some(Some(value.to_string())))
            .ok_or_else(|| invalid_type(field, "a string or null")),
    }
}

pub(crate) fn nullable_string_array(
    input: &Value,
    field: &str,
) -> Result<Option<Option<Vec<String>>>, ToolError> {
    match input.get(field) {
        None => Ok(None),
        Some(Value::Null) => Ok(Some(None)),
        Some(value) => string_array(value, field).map(|items| Some(Some(items))),
    }
}

fn string_array(value: &Value, field: &str) -> Result<Vec<String>, ToolError> {
    let Some(items) = value.as_array() else {
        return Err(invalid_type(field, "an array of strings"));
    };
    let mut result = Vec::with_capacity(items.len());
    for item in items {
        let Some(value) = item.as_str() else {
            return Err(invalid_type(field, "an array of strings"));
        };
        result.push(value.to_string());
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        nullable_string, optional_bool, optional_str, optional_string_array, optional_u64,
        require_str,
    };
    use crate::ToolError;

    fn valid<T>(result: Result<T, ToolError>) -> T {
        match result {
            Ok(value) => value,
            Err(other) => panic!("expected valid input, got {other}"),
        }
    }

    fn invalid_message<T>(result: Result<T, ToolError>) -> String {
        match result {
            Ok(_) => panic!("expected invalid input"),
            Err(ToolError::InvalidInput(message)) => message,
            Err(other) => panic!("expected invalid input, got {other}"),
        }
    }

    #[test]
    fn require_str_reads_present_string() {
        let input = json!({ "field": "value" });
        assert_eq!(valid(require_str(&input, "field")), "value");
    }

    #[test]
    fn require_str_reports_missing_field() {
        let input = json!({});
        assert_eq!(
            invalid_message(require_str(&input, "field")),
            "missing field"
        );
    }

    #[test]
    fn require_str_reports_wrong_type() {
        let input = json!({ "field": 42 });
        assert_eq!(
            invalid_message(require_str(&input, "field")),
            "field must be a string"
        );
    }

    #[test]
    fn optional_accessors_read_values_and_absence() {
        let input = json!({
            "name": "nyx",
            "enabled": true,
            "limit": 12
        });

        assert_eq!(valid(optional_str(&input, "name")), Some("nyx"));
        assert_eq!(valid(optional_bool(&input, "enabled")), Some(true));
        assert_eq!(valid(optional_u64(&input, "limit")), Some(12));
        assert_eq!(valid(optional_u64(&input, "missing")), None);
    }

    #[test]
    fn optional_u64_rejects_negative_numbers() {
        let input = json!({ "limit": -1 });
        assert_eq!(
            invalid_message(optional_u64(&input, "limit")),
            "limit must be a non-negative integer"
        );
    }

    #[test]
    fn nullable_string_distinguishes_missing_null_and_value() {
        assert_eq!(valid(nullable_string(&json!({}), "label")), None);
        assert_eq!(
            valid(nullable_string(&json!({ "label": null }), "label")),
            Some(None)
        );
        assert_eq!(
            valid(nullable_string(&json!({ "label": "alpha" }), "label")),
            Some(Some("alpha".to_string()))
        );
    }

    #[test]
    fn optional_string_array_rejects_non_string_items() {
        let input = json!({ "tool_allow": ["read", 7] });
        assert_eq!(
            invalid_message(optional_string_array(&input, "tool_allow")),
            "tool_allow must be an array of strings"
        );
    }
}
