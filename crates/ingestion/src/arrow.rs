use arrow::datatypes::{Schema, Field, DataType, TimeUnit};
use std::sync::Arc;
use arrow::array::{StringBuilder, TimestampMicrosecondBuilder, Int32Builder, UInt32Builder};
use arrow::record_batch::RecordBatch;
use common::log_system::LogEvent;
use serde_json;

pub fn create_log_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false
        ),
        Field::new("service_name", DataType::Utf8, false),
        Field::new("log_level", DataType::Int32, false),
        Field::new("message", DataType::Utf8, false),
        Field::new("error", DataType::Utf8, true),
        Field::new("module", DataType::Utf8, true),
        Field::new("function", DataType::Utf8, true),
        Field::new("line_no", DataType::UInt32, true),
        Field::new("metadata_json", DataType::Utf8, true)
    ]))
}


pub fn events_record_batch(events: &[LogEvent]) -> Result<RecordBatch, arrow::error::ArrowError> {
    let capacity = events.len();

    // Initialize Arrow array builders with capacity
    let mut timestamp_builder = TimestampMicrosecondBuilder::with_capacity(capacity);
    let mut service_name_builder = StringBuilder::with_capacity(capacity, capacity * 16);
    let mut log_level_builder = Int32Builder::with_capacity(capacity);
    let mut message_builder = StringBuilder::with_capacity(capacity, capacity * 64);
    let mut error_builder = StringBuilder::with_capacity(capacity, capacity * 32);
    let mut module_builder = StringBuilder::with_capacity(capacity, capacity * 16);
    let mut function_builder = StringBuilder::with_capacity(capacity, capacity * 16);
    let mut line_no_builder = UInt32Builder::with_capacity(capacity);
    let mut metadata_builder = StringBuilder::with_capacity(capacity, capacity * 32);

    // Append row fields into column builders
    for event in events {
        timestamp_builder.append_value(event.timestamp_micros);
        service_name_builder.append_value(&event.service_name);
        log_level_builder.append_value(event.log_level);
        message_builder.append_value(&event.message);

        // optiona field
        error_builder.append_option(event.error.as_deref());
        module_builder.append_option(event.module.as_deref());
        function_builder.append_option(event.function.as_deref());
        line_no_builder.append_option(event.line_no);

        if event.metadata.is_empty() {
            metadata_builder.append_null();
        } else {
            let json_str = serde_json::to_string(&event.metadata).unwrap_or_default();
            metadata_builder.append_value(json_str);
        }
    }

    // Finish builder into Arrow arrays
    let schema = create_log_schema();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(timestamp_builder.finish().with_timezone("UTC")),
            Arc::new(service_name_builder.finish()),
            Arc::new(log_level_builder.finish()),
            Arc::new(message_builder.finish()),
            Arc::new(error_builder.finish()),
            Arc::new(module_builder.finish()),
            Arc::new(function_builder.finish()),
            Arc::new(line_no_builder.finish()),
            Arc::new(metadata_builder.finish()),
        ]
    )
}