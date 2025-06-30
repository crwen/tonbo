use std::sync::Arc;

use arrow::datatypes::Schema;

use crate::{
    compaction::FileMeta,
    fs::manager::StoreManager,
    record::Record,
    scope::Scope,
    timestamp::Timestamp,
    version::{self, set::VersionSet, TransactionTs, Version, MAX_LEVEL},
    ParquetLru, Schema as RecordSchema,
};

pub struct Context<R: Record> {
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

    pub async fn level_scopes<'a>(
        &self,
        level: usize,
    ) -> Vec<Scope<<R::Schema as RecordSchema>::Key>> {
        let version = self.version_set.current().await;
        version.level_slice[level].clone()
    }

    pub async fn scopes_filter<P>(&self, predicate: P) -> Vec<FileMeta<R>>
    where
        P: Fn(&Scope<<R::Schema as RecordSchema>::Key>) -> bool,
    {
        let version = self.version_set.current().await;
        let mut file_metas = vec![];
        for (level, level_scopes) in version.level_slice.iter().enumerate() {
            for scope in level_scopes.iter() {
                if predicate(scope) {
                    file_metas.push(FileMeta {
                        scope: scope.clone(),
                        level,
                    });
                }
            }
        }
        file_metas
    }

    pub async fn level_scopes_filter<P>(&self, level: usize, predicate: P) -> Vec<FileMeta<R>>
    where
        P: Fn(&FileMeta<R>) -> bool,
    {
        let version = self.version_set.current().await;
        version.level_slice[level]
            .iter()
            .map(|scope| FileMeta::<R> {
                scope: scope.clone(),
                level,
            })
            .filter(predicate)
            .collect::<Vec<FileMeta<R>>>()
    }

    /// return all scopes that ovelapped with \[min, max\]
    pub async fn level_overlapped<'a>(
        &self,
        level: usize,
        min: &'a <R::Schema as RecordSchema>::Key,
        max: &'a <R::Schema as RecordSchema>::Key,
    ) -> Vec<FileMeta<R>> {
        let version = self.version_set.current().await;
        let mut metas = Vec::new();
        let start_l = Version::<R>::scope_search(min, &version.level_slice[level]);
        // let option = version.option();

        for scope in version.level_slice[level][start_l..].iter() {
            if scope.contains(min) || scope.contains(max)
            // && metas.len() <= option.major_l_selection_table_max_num
            {
                metas.push(FileMeta {
                    scope: scope.clone(),
                    level,
                });
            } else {
                break;
            }
        }

        metas
    }

    pub async fn table_len(&self, level: usize) -> usize {
        let version = self.version_set.current().await;
        version.level_slice[level].len()
    }

    // pub const fn max_level() -> usize {
    //     MAX_LEVEL
    // }
}
