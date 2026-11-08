use iceberg::spec::{DataContentType, DataFile, DataFileBuilder, DataFileFormat, NestedField, PrimitiveType, Schema, Struct, Type};

pub struct IcebergCatalogManager {
    output_dir: String
}


impl IcebergCatalogManager {
    pub fn new(output_dir: String) -> Self {
        Self {
            output_dir
        }
    }

    ///Construct Iceberg table schema
    pub fn build_iceberg_schema() -> Schema {
        Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::required(1, "timestamp", Type::Primitive(PrimitiveType::Timestamp)).into(),
                NestedField::required(2, "service_name", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(3, "log_level", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(4, "message", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::optional(5, "error", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::optional(6, "module", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::optional(7, "function", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::optional(8, "line_no", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::optional(9, "metadata_json", Type::Primitive(PrimitiveType::String)).into()
            ])
            .build()
            .unwrap()
    }

    /// Construct an Iceberg DataFile entry for committing to table snapshots
    pub fn create_data_file_entry(file_path: &str, file_size_bytes: u64, record_count: u64) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(file_path.to_string())
            .file_format(DataFileFormat::Parquet)
            .partition(Struct::empty())      // required; unpartitioned table
            .record_count(record_count)
            .file_size_in_bytes(file_size_bytes)
            .build()
            .unwrap()
    }
}