use std::{mem, sync::Arc};

use async_lock::{RwLock, RwLockUpgradableReadGuard};
use async_trait::async_trait;
use fusio::MaybeSend;

use super::{
    CompactionError, CompactionOutput, Compactor, Context, FileMeta, LeveledCompactor, MergeOutput,
    VersionEdit,
};
use crate::{
    inmem::mutable::MutableMemTable,
    record::{Record, Schema},
    stream::ScanStream,
    DbOption, DbStorage,
};

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait Compaction<R>: MaybeSend + Sync + 'static
where
    R: Record,
{
    async fn prepare(&self, ctx: Arc<Context<R>>) -> Result<(), CompactionError<R>>;

    /// Pick SSTables for compaction.
    async fn pick_compaction(&self, ctx: Arc<Context<R>>) -> Option<Vec<CompactionOutput<R>>>;

    /// Merge SSTables.
    async fn merge(
        &self,
        ctx: Arc<Context<R>>,
        option: &DbOption,
        compaction_output: Vec<CompactionOutput<R>>,
    ) -> Result<MergeOutput<R>, CompactionError<R>> {
        let mut adds = vec![];
        let mut deletes = vec![];

        for output in compaction_output.into_iter() {
            let level_path = option
                .level_fs_path(output.output_level)
                .unwrap_or(&option.base_path);
            let fs = ctx.manager.get_fs(level_path);
            let arrow_schema = ctx.arrow_schema.clone();

            let streams: Vec<ScanStream<R>> = vec![];
            // TODO: construct streams.

            for scope in
                Compactor::build_tables(option, output.output_level, streams, &arrow_schema, fs)
                    .await?
            {
                adds.push(FileMeta {
                    scope,
                    level: output.output_level,
                });
            }
            deletes.extend(output.inputs);
        }

        Ok(MergeOutput { adds, deletes })
    }

    /// Whether to trigger major compaction. If true, [`Compaction::pick_compaction`] and
    /// [`Compaction::merge`] will be used to do compaction
    async fn trigger_compaction(&self, ctx: Arc<Context<R>>, option: &DbOption) -> bool;

    async fn finish(&self, ctx: Arc<Context<R>>) -> Result<(), CompactionError<R>>;
}

pub(crate) struct CustomizedCompactor<R: Record> {
    option: Arc<DbOption>,
    schema: Arc<RwLock<DbStorage<R>>>,
    ctx: Arc<Context<R>>,
    record_schema: Arc<R::Schema>,
    compactor: Box<dyn Compaction<R>>,
}

impl<R> CustomizedCompactor<R>
where
    R: Record,
{
    pub(crate) fn new(
        schema: Arc<RwLock<DbStorage<R>>>,
        record_schema: Arc<R::Schema>,
        option: Arc<DbOption>,
        ctx: Arc<Context<R>>,
        compactor: Box<dyn Compaction<R>>,
    ) -> Self {
        Self {
            option,
            schema,
            ctx,
            record_schema,
            compactor,
        }
    }

    pub(crate) async fn check_then_compaction(
        &mut self,
        is_manual: bool,
    ) -> Result<(), CompactionError<R>> {
        let mut guard = self.schema.write().await;

        guard.trigger.reset();

        if !guard.mutable.is_empty() {
            let trigger_clone = guard.trigger.clone();

            let mutable = mem::replace(
                &mut guard.mutable,
                MutableMemTable::new(
                    &self.option,
                    trigger_clone,
                    self.ctx.manager.base_fs().clone(),
                    self.record_schema.clone(),
                )
                .await?,
            );
            let (file_id, immutable) = mutable.into_immutable().await?;
            guard.immutables.push((file_id, immutable));
        } else if !is_manual {
            return Ok(());
        }

        if (is_manual && !guard.immutables.is_empty())
            || guard.immutables.len() > self.option.immutable_chunk_max_num
        {
            let recover_wal_ids = guard.recover_wal_ids.take();
            drop(guard);

            let guard = self.schema.upgradable_read().await;
            let chunk_num = if is_manual {
                guard.immutables.len()
            } else {
                self.option.immutable_chunk_num
            };
            let excess = &guard.immutables[0..chunk_num];
            let mut version_edits = vec![];

            // do minor compaction
            self.compactor.prepare(self.ctx.clone()).await?;
            if let Some(_scope) = LeveledCompactor::minor_compaction(
                &self.option,
                recover_wal_ids,
                excess,
                guard.record_schema.arrow_schema(),
                &self.ctx.manager,
            )
            .await?
            {}

            if self
                .compactor
                .trigger_compaction(self.ctx.clone(), &self.option)
                .await
            {
                if let Some(compaction_output) =
                    self.compactor.pick_compaction(self.ctx.clone()).await
                {
                    let merge_output = self
                        .compactor
                        .merge(self.ctx.clone(), &self.option, compaction_output)
                        .await?;
                    for add in merge_output.adds {
                        version_edits.push(VersionEdit::Add {
                            level: add.level as u8,
                            scope: add.scope,
                        });
                    }
                    for delete in merge_output.deletes {
                        version_edits.push(VersionEdit::Remove {
                            level: delete.level as u8,
                            gen: delete.scope.gen,
                        });
                    }
                }
            }

            let mut guard = RwLockUpgradableReadGuard::upgrade(guard).await;
            let sources = guard.immutables.split_off(chunk_num);
            let _ = mem::replace(&mut guard.immutables, sources);

            // TODO: construct version edits and delete_gens
            let delete_gens = vec![];
            self.ctx
                .version_set
                .apply_edits(version_edits, Some(delete_gens), false)
                .await?;

            self.compactor.finish(self.ctx.clone()).await?;
        }

        if is_manual {
            self.ctx.version_set.rewrite().await.unwrap();
        }
        Ok(())
    }
}

#[cfg(all(feature = "tokio", test))]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use fusio_log::{FsOptions, Path};
    use parquet_lru::NoCache;
    use tempfile::TempDir;

    use super::{Compaction, Context};
    use crate::{
        compaction::{tests::build_version, CompactionOutput, FileMeta},
        fs::manager::StoreManager,
        inmem::immutable::tests::TestSchema,
        record::{Record, Schema},
        tests::Test,
        version::{cleaner::Cleaner, set::VersionSet, MAX_LEVEL},
        CompactionError, DbOption,
    };

    struct TestLeveledCompactor {
        level_base_num: usize,
    }

    #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
    #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
    impl<R> Compaction<R> for TestLeveledCompactor
    where
        R: Record,
    {
        async fn prepare(&self, _ctx: Arc<Context<R>>) -> Result<(), CompactionError<R>> {
            Ok(())
        }

        async fn trigger_compaction(&self, ctx: Arc<Context<R>>, option: &DbOption) -> bool {
            for level in 0..MAX_LEVEL - 2 {
                if ctx.table_len(level).await
                    >= (option.major_threshold_with_sst_size
                        * option.level_sst_magnification.pow(level as u32))
                {
                    return true;
                }
            }
            false
        }

        async fn pick_compaction(&self, ctx: Arc<Context<R>>) -> Option<Vec<CompactionOutput<R>>> {
            dbg!(ctx.table_len(0).await, self.level_base_num);
            if ctx.table_len(0).await > self.level_base_num {
                let scopes = ctx.level_scopes(0).await;
                let scope = scopes.last().unwrap();
                let overlapped = ctx.level_overlapped(0, &scope.min, &scope.max).await;
                let mut inputs = vec![FileMeta::new(0, scope.clone())];
                inputs.extend(overlapped);

                Some(vec![CompactionOutput::<R> {
                    inputs,
                    output_level: 1,
                }])
            } else {
                let mut best_scope = None;
                let mut best_scope_overlapped = 0;
                let mut best_level = 0;

                for level in 1..MAX_LEVEL - 1 {
                    if ctx.table_len(level).await >= self.level_base_num * 2usize.pow(level as u32)
                    {
                        let scopes = ctx.level_scopes(0).await;
                        for scope in scopes.into_iter() {
                            let overlapped = ctx
                                .level_overlapped(level + 1, &scope.min, &scope.max)
                                .await;
                            if overlapped.len() > best_scope_overlapped {
                                best_scope = Some(scope);
                                best_scope_overlapped = overlapped.len();
                                best_level = level;
                            }
                        }

                        break;
                    }
                }
                match best_scope {
                    Some(scope) => {
                        let inputs = ctx
                            .level_overlapped(best_level + 1, &scope.min, &scope.max)
                            .await
                            .into_iter()
                            .chain([FileMeta::new(0, scope.clone())])
                            .collect();

                        Some(vec![CompactionOutput::<R> {
                            inputs,
                            output_level: 1,
                        }])
                    }
                    None => None,
                }
            }
        }

        async fn finish(&self, _ctx: Arc<Context<R>>) -> Result<(), CompactionError<R>> {
            Ok(())
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn major_compaction() {
        let temp_dir = TempDir::new().unwrap();
        let temp_dir_l0 = TempDir::new().unwrap();
        let temp_dir_l1 = TempDir::new().unwrap();

        let mut option = DbOption::new(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
            &TestSchema,
        )
        .level_path(
            0,
            Path::from_filesystem_path(temp_dir_l0.path()).unwrap(),
            FsOptions::Local,
        )
        .unwrap()
        .level_path(
            1,
            Path::from_filesystem_path(temp_dir_l1.path()).unwrap(),
            FsOptions::Local,
        )
        .unwrap();
        option.major_threshold_with_sst_size = 2;
        let option = Arc::new(option);
        let manager = Arc::new(
            StoreManager::new(option.base_fs.clone(), option.level_paths.clone()).unwrap(),
        );

        manager
            .base_fs()
            .create_dir_all(&option.version_log_dir_path())
            .await
            .unwrap();
        manager
            .base_fs()
            .create_dir_all(&option.wal_dir_path())
            .await
            .unwrap();

        let ((_table_gen_1, table_gen_2, _table_gen_3, table_gen_4, _), _version) =
            build_version(&option, &manager, &Arc::new(TestSchema)).await;

        let (_, clean_sender) = Cleaner::new(option.clone(), manager.clone());
        let version_set: VersionSet<Test> =
            VersionSet::new(clean_sender, option.clone(), manager.clone())
                .await
                .unwrap();
        // todo update version set
        let _ctx = Context::new(
            manager.clone(),
            Arc::new(NoCache::default()),
            version_set,
            TestSchema.arrow_schema().clone(),
        );

        let _compactor = TestLeveledCompactor { level_base_num: 1 };
        // if let Some(output) = compactor.pick_compaction(Arc::new(ctx)).await {
        //     let level0 = &output.inputs[0].0;
        //     let scopes0 = &output.inputs[0].1;
        //     assert_eq!(*level0, 0);
        //     assert_eq!(scopes0.len(), 1);
        //     assert_eq!(scopes0[0].gen(), table_gen_2);
        //     let level1 = &output.inputs[1].0;
        //     let scopes1 = &output.inputs[1].1;
        //     assert_eq!(*level1, 1);
        //     assert_eq!(scopes1.len(), 1);
        //     assert_eq!(scopes1[0].gen(), table_gen_4);
        // } else {
        //     unreachable!()
        // }
    }
}
