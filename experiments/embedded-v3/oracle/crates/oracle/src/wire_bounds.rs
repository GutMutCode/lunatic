use serde::de::{self, DeserializeOwned};

use crate::protocol::MAX_WIRE_STRING_BYTES;

pub(crate) fn decode_bounded<T: DeserializeOwned>(line: &str) -> Result<T, serde_json::Error> {
    let value: serde_json::Value = serde_json::from_str(line)?;
    validate_strings(&value)?;
    serde_json::from_value(value)
}

fn validate_strings(value: &serde_json::Value) -> Result<(), serde_json::Error> {
    match value {
        serde_json::Value::String(value) => validate_string(value),
        serde_json::Value::Array(values) => {
            for value in values {
                validate_strings(value)?;
            }
            Ok(())
        }
        serde_json::Value::Object(values) => {
            for (key, value) in values {
                validate_string(key)?;
                validate_strings(value)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn validate_string(value: &str) -> Result<(), serde_json::Error> {
    if value.len() > MAX_WIRE_STRING_BYTES {
        Err(<serde_json::Error as de::Error>::custom(format_args!(
            "wire string exceeds {MAX_WIRE_STRING_BYTES} bytes"
        )))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_oversized_nested_string_before_typed_decode() {
        let input = format!(
            r#"{{"nested":["{}"]}}"#,
            "x".repeat(MAX_WIRE_STRING_BYTES + 1)
        );
        assert!(decode_bounded::<serde_json::Value>(&input).is_err());
    }
}
