use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use arrow::datatypes::Schema as ArrowSchema;
use futures_core::Stream;
use pin_project_lite::pin_project;

use crate::{
    record::{ArrowArrays, ArrowArraysBuilder, Record, Schema},
    stream::merge::MergeStream,
};

pin_project! {
    pub struct PackageStream<'package, R>
    where
        R: Record,
    {
        row_count: usize,
        batch_size: usize,
        inner: MergeStream<'package, R>,
        builder: <<R::Schema as Schema>::Columns as ArrowArrays>::Builder,
        projection_indices: Option<Vec<usize>>,
    }
}

impl<'package, R> PackageStream<'package, R>
where
    R: Record,
{
    pub(crate) fn new(
        batch_size: usize,
        merge: MergeStream<'package, R>,
        projection_indices: Option<Vec<usize>>,
        schema: Arc<ArrowSchema>,
    ) -> Self {
        Self {
            row_count: 0,
            batch_size,
            inner: merge,
            builder: <R::Schema as Schema>::Columns::builder(schema, batch_size),
            projection_indices,
        }
    }
}

impl<R> Stream for PackageStream<'_, R>
where
    R: Record,
{
    type Item = Result<<R::Schema as Schema>::Columns, parquet::errors::ParquetError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut project = self.project();

        while project.row_count <= project.batch_size {
            match Pin::new(&mut project.inner).poll_next(cx) {
                Poll::Ready(Some(Ok(entry))) => {
                    if let Some(record) = entry.value() {
                        // filter null
                        project.builder.push(entry.key(), Some(record));
                        *project.row_count += 1;
                    }
                }
                Poll::Ready(Some(Err(err))) => return Poll::Ready(Some(Err(err))),
                Poll::Ready(None) => break,
                Poll::Pending => return Poll::Pending,
            }
        }
        Poll::Ready((*project.row_count != 0).then(|| {
            *project.row_count = 0;
            Ok(project
                .builder
                .finish(project.projection_indices.as_ref().map(Vec::as_slice)))
        }))
    }
}

#[cfg(all(test, feature = "tokio"))]
mod tests {
    use std::{collections::Bound, sync::Arc};

    use arrow::array::{BooleanArray, RecordBatch, StringArray, UInt32Array};
    use fusio::{disk::TokioFs, path::Path, DynFs};
    use futures_util::StreamExt;
    use tempfile::TempDir;

    use crate::{
        inmem::{
            immutable::tests::{TestImmutableArrays, TestSchema},
            mutable::MutableMemTable,
        },
        record::{ArrowArrays, Schema},
        stream::{merge::MergeStream, package::PackageStream},
        tests::Test,
        trigger::TriggerFactory,
        wal::log::LogType,
        DbOption,
    };

    #[tokio::test]
    async fn iter() {
        let temp_dir = TempDir::new().unwrap();
        let fs = Arc::new(TokioFs) as Arc<dyn DynFs>;
        let option = DbOption::new(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
            &TestSchema,
        );

        fs.create_dir_all(&option.wal_dir_path()).await.unwrap();

        let trigger = TriggerFactory::create(option.trigger_type);

        let m1 = MutableMemTable::<Test>::new(&option, trigger, fs, Arc::new(TestSchema {}))
            .await
            .unwrap();
        m1.insert(
            LogType::Full,
            Test {
                vstring: "a".into(),
                vu32: 0,
                vbool: Some(true),
            },
            0.into(),
        )
        .await
        .unwrap();
        m1.insert(
            LogType::Full,
            Test {
                vstring: "b".into(),
                vu32: 1,
                vbool: Some(true),
            },
            1.into(),
        )
        .await
        .unwrap();
        m1.insert(
            LogType::Full,
            Test {
                vstring: "c".into(),
                vu32: 2,
                vbool: Some(true),
            },
            2.into(),
        )
        .await
        .unwrap();
        m1.insert(
            LogType::Full,
            Test {
                vstring: "d".into(),
                vu32: 3,
                vbool: Some(true),
            },
            3.into(),
        )
        .await
        .unwrap();
        m1.insert(
            LogType::Full,
            Test {
                vstring: "e".into(),
                vu32: 4,
                vbool: Some(true),
            },
            4.into(),
        )
        .await
        .unwrap();
        m1.insert(
            LogType::Full,
            Test {
                vstring: "f".into(),
                vu32: 5,
                vbool: Some(true),
            },
            5.into(),
        )
        .await
        .unwrap();

        let merge = MergeStream::<Test>::from_vec(
            vec![m1
                .scan((Bound::Unbounded, Bound::Unbounded), 6.into(), None)
                .into()],
            6.into(),
            None,
        )
        .await
        .unwrap();
        let projection_indices = vec![0, 1, 2, 3];

        let mut package = PackageStream {
            row_count: 0,
            batch_size: 8192,
            inner: merge,
            builder: TestImmutableArrays::builder(TestSchema {}.arrow_schema().clone(), 8192),
            projection_indices: Some(projection_indices.clone()),
        };

        let arrays = package.next().await.unwrap().unwrap();
        assert_eq!(
            arrays.as_record_batch(),
            &RecordBatch::try_new(
                Arc::new(
                    TestSchema {}
                        .arrow_schema()
                        .project(&projection_indices)
                        .unwrap(),
                ),
                vec![
                    Arc::new(BooleanArray::from(vec![
                        false, false, false, false, false, false
                    ])),
                    Arc::new(UInt32Array::from(vec![0, 1, 2, 3, 4, 5])),
                    Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e", "f"])),
                    Arc::new(UInt32Array::from(vec![0, 1, 2, 3, 4, 5])),
                ],
            )
            .unwrap()
        )
    }
}
