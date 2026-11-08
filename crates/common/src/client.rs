use crate::log_system::LogEvent as ProtoLogEvent;
use crate::log_system::LogLevel as ProtoLogLevel;

use crate::LogEvent;

impl From<LogEvent> for ProtoLogEvent {
    fn from(event: LogEvent) -> Self {
        let log_level = match event.log_level {
            crate::LogLevel::Trace => ProtoLogLevel::Trace,
            crate::LogLevel::Debug => ProtoLogLevel::Debug,
            crate::LogLevel::Info => ProtoLogLevel::Info,
            crate::LogLevel::Warn => ProtoLogLevel::Warn,
            crate::LogLevel::Error => ProtoLogLevel::Error,
            crate::LogLevel::Fatal => ProtoLogLevel::Fatal,
        };

        ProtoLogEvent {
            timestamp_micros: event.timestamp.timestamp_micros(),
            service_name: event.service_name,
            log_level: log_level as i32,
            message: event.message,
            error: event.error,
            module: event.module,
            function: event.function,
            line_no: event.line_no,
            metadata: event.metadata,
        }
    }
}