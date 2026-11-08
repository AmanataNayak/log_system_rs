use std::collections::HashMap;
use std::str::FromStr;
use chrono::Utc;
use common::{LogEvent, LogLevel};
use serde_json::Value;

pub struct EventParser {
    pub service_name: String
}

impl EventParser {
    pub fn new(service_name: impl Into<String>) -> Self {
        Self {
            service_name: service_name.into()
        }
    }

    // parse a raw line string into a structured LogEvent
    pub fn parse(&self, raw: &str) -> LogEvent {
        let now = Utc::now();

        // 1. try structured JSON parsing
        if let Ok(value) = serde_json::from_str::<Value>(raw) {
            if let Some(obj) = value.as_object() {
                let message = obj
                    .get("message")
                    .and_then(|v| v.as_str())
                    .unwrap_or(raw)
                    .to_string();

                let log_level = obj
                    .get("level")
                    .or_else(|| obj.get("log_level"))
                    .and_then(|v| v.as_str())
                    .and_then(|s| LogLevel::from_str(s).ok())
                    .unwrap_or(LogLevel::Info);

                let module = obj
                    .get("module")
                    .and_then(|v| v.as_str())
                    .map(String::from);

                let line_no = obj
                    .get("line_no")
                    .or_else(|| obj.get("line"))
                    .and_then(|v| v.as_u64())
                    .map(|v| v as u32);

                let error = obj
                    .get("error")
                    .and_then(|v| v.as_str())
                    .map(String::from);

                let function = obj
                    .get("function")
                    .and_then(|v| v.as_str())
                    .map(String::from);

                let known_keys = [
                    "message", "level", "log_level", "module", "module", "line_no",
                    "line", "error", "function"
                ];

                let mut metadata = HashMap::new();

                for (k, v) in obj {
                    if !known_keys.contains(&k.as_str()) {
                        let val_str = match v {
                            Value::String(s) => s.clone(),
                            other => other.to_string()
                        };

                        metadata.insert(k.clone(), val_str);
                    }
                }
                return LogEvent {
                    timestamp: now,
                    service_name: self.service_name.clone(),
                    log_level,
                    message,
                    error,
                    module,
                    function,
                    line_no,
                    metadata
                };
            }
        }
        // Unstructured plain text fallback
        LogEvent {
            timestamp: now,
            service_name: self.service_name.clone(),
            log_level: LogLevel::Info,
            message: raw.to_string(),
            error: None,
            module: None,
            function: None,
            line_no: None,
            metadata: HashMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::LogLevel;

    #[test]
    fn test_parse_structured_json() {
        let parser = EventParser::new("test-service");
        let raw = r#"{"level":"error","message":"DB connection failed","user_id":"123","module":"db.rs","line":42}"#;

        let event = parser.parse(raw);

        assert_eq!(event.service_name, "test-service");
        assert_eq!(event.log_level, LogLevel::Error);
        assert_eq!(event.message, "DB connection failed");
        assert_eq!(event.module.as_deref(), Some("db.rs"));
        assert_eq!(event.line_no, Some(42));
        assert_eq!(event.metadata.get("user_id").unwrap(), "123");
    }

    #[test]
    fn test_parse_unstructured_fallback() {
        let parser = EventParser::new("test-service");
        let raw = "Panicked at line 10 in main.rs";

        let event = parser.parse(raw);

        assert_eq!(event.log_level, LogLevel::Info);
        assert_eq!(event.message, raw);
        assert!(event.metadata.is_empty());
    }
}