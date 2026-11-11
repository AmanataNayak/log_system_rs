use std::collections::HashMap;
use std::sync::Arc;
use datafusion::prelude::SessionContext;
use iceberg::{Catalog, CatalogBuilder};
use iceberg::io::LocalFsStorageFactory;
use iceberg_catalog_sql::{SqlCatalogBuilder, SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE};
use iceberg_datafusion::IcebergCatalogProvider;
use tracing::info;

pub async fn register_iceberg_tables(
    ctx: &SessionContext,
    db_url: &str,
    warehouse_dir: &str,
) -> Result<Arc<dyn Catalog>, Box<dyn std::error::Error + Send + Sync>> {
    sqlx::any::install_default_drivers();

    let props = HashMap::from([
        (SQL_CATALOG_PROP_URI.to_string(), db_url.to_string()),
        (SQL_CATALOG_PROP_WAREHOUSE.to_string(), warehouse_dir.to_string()),
    ]);

    info!(target: "query_engine", "Connecting SqlCatalog for Query Engine at: {}", db_url);
    let catalog = SqlCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load("postgres_catalog", props)
        .await?;


    let catalog_arc: Arc<dyn Catalog> = Arc::new(catalog);

    info!(target: "query_engine", "Initializing IcebergCatalogProvider for DataFusion...");
    let catalog_provider = IcebergCatalogProvider::try_new(Arc::clone(&catalog_arc)).await?;

    // Register full catalog under "iceberg" namespace in DataFusion
    ctx.register_catalog("iceberg", Arc::new(catalog_provider));

    info!(target: "query_engine", "Successfully registered Iceberg catalog into DataFusion SessionContext!");
    Ok(catalog_arc)
}