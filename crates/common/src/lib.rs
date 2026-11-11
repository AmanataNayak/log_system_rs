use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::fmt::Formatter;
use std::str::FromStr;

mod client;
pub mod event;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum LogLevel{
    Trace,
    Debug,
    Info,
    Warn,
    Error,
    Fatal
}

impl fmt::Display for LogLevel {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            LogLevel::Trace => write!(f, "TRACE"),
            LogLevel::Debug => write!(f, "DEBUG"),
            LogLevel::Info => write!(f, "Info"),
            LogLevel::Warn => write!(f, "WARN"),
            LogLevel::Error => write!(f, "ERROR"),
            LogLevel::Fatal => write!(f, "FATAL")
        }
    }
}

impl FromStr for LogLevel {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "TRACE" => Ok(LogLevel::Trace),
            "DEBUG" => Ok(LogLevel::Debug),
            "INFO" => Ok(LogLevel::Info),
            "WARN" | "WARNING" => Ok(LogLevel::Warn),
            "ERROR" => Ok(LogLevel::Error),
            "FATAL" => Ok(LogLevel::Fatal),
            _ => Err(())
        }
    }
}

pub struct LogEvent {
    pub timestamp: DateTime<Utc>,
    pub service_name: String,
    pub log_level: LogLevel,
    pub message: String,
    pub error: Option<String>,
    pub module: Option<String>,
    pub function: Option<String>,
    pub line_no: Option<u32>,
    pub metadata: HashMap<String, String>
}


pub mod log_system {
    tonic::include_proto!("log_system");
}

pub mod pubsub {
    tonic::include_proto!("pubsub");
}