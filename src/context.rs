use std::sync::Arc;

use arrow::datatypes::Schema;

use crate::{
    fs::manager::StoreManager,
    record::Record,
    version::{set::VersionSet, timestamp::Timestamp, TransactionTs},
    ParquetLru,
};

pub(crate) struct Context<R: Record> {
    pub(crate) manager: Arc<StoreManager>,
    pub(crate) parquet_lru: ParquetLru,
    pub(crate) version_set: VersionSet<R>,
    pub(crate) arrow_schema: Arc<Schema>,
}

impl<R> Context<R>
where
    R: Record,
{
    pub(crate) fn new(
        manager: Arc<StoreManager>,
        parquet_lru: ParquetLru,
        version_set: VersionSet<R>,
        arrow_schema: Arc<Schema>,
    ) -> Self {
        Self {
            manager,
            parquet_lru,
            version_set,
            arrow_schema,
        }
    }

    pub(crate) fn version_set(&self) -> &VersionSet<R> {
        &self.version_set
    }

    pub(crate) fn storage_manager(&self) -> &StoreManager {
        &self.manager
    }

    pub(crate) fn cache(&self) -> &ParquetLru {
        &self.parquet_lru
    }

    pub(crate) fn arrow_schema(&self) -> &Arc<Schema> {
        &self.arrow_schema
    }

    pub(crate) fn load_ts(&self) -> Timestamp {
        self.version_set.load_ts()
    }

    pub(crate) fn increase_ts(&self) -> Timestamp {
        self.version_set.increase_ts()
    }
}
