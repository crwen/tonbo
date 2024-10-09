use std::sync::Arc;

use fusio::local::TokioFs;
use pyo3::{
    prelude::*,
    pyclass, pymethods,
    types::{PyDict, PyMapping},
    Py, PyAny, PyResult, Python,
};
use pyo3_asyncio::tokio::{future_into_py, get_runtime};
use tonbo::{
    executor::tokio::TokioExecutor,
    fs::manager::StoreManager,
    record::{ColumnDesc, DynRecord},
    DB,
};

use crate::{
    column::Column,
    error::CommitError,
    options::DbOption,
    transaction::Transaction,
    utils::{to_col, to_dict},
};

type PyExecutor = TokioExecutor;

#[pyclass]
pub struct TonboDB {
    desc: Arc<Vec<Column>>,
    primary_key_index: usize,
    db: Arc<DB<DynRecord, PyExecutor>>,
}

#[pymethods]
impl TonboDB {
    #[new]
    fn new(py: Python<'_>, option: DbOption, record: Py<PyAny>) -> PyResult<Self> {
        let dict = record.getattr(py, "__dict__")?;
        let values = dict.downcast_bound::<PyMapping>(py)?.values()?;
        let mut desc = vec![];
        let mut cols = vec![];
        let mut primary_key_index = None;
        let mut primary_key_name = None;

        for i in 0..values.len()? {
            let value = values.get_item(i)?;
            if let Ok(bound_col) = value.downcast::<Column>() {
                let col = bound_col.extract::<Column>()?;
                if col.primary_key {
                    if primary_key_index.is_some() {
                        panic!("Multiple primary keys is not allowed!")
                    }
                    primary_key_index = Some(desc.len());
                    primary_key_name = Some(col.name.clone());
                }
                cols.push(col.clone());
                desc.push(ColumnDesc::from(col));
            }
        }
        let option = option.into_option(primary_key_index.unwrap(), primary_key_name.unwrap());
        let manager = StoreManager::new(Arc::new(TokioFs), vec![]);
        let db = get_runtime()
            .block_on(async {
                DB::with_schema(
                    option,
                    TokioExecutor::new(),
                    manager,
                    desc,
                    primary_key_index.unwrap(),
                )
                .await
            })
            .unwrap();
        Ok(Self {
            db: Arc::new(db),
            desc: Arc::new(cols),
            primary_key_index: primary_key_index.expect("Primary key not found"),
        })
    }

    fn insert<'py>(&'py self, py: Python<'py>, record: Py<PyAny>) -> PyResult<Bound<PyAny>> {
        let mut cols = vec![];
        let dict = record.getattr(py, "__dict__")?;
        let values = dict.downcast_bound::<PyMapping>(py)?.values()?;

        for i in 0..values.len()? {
            let value = values.get_item(i)?;
            if let Ok(bound_col) = value.downcast::<Column>() {
                let col = tonbo::record::Column::from(bound_col.extract::<Column>()?);
                cols.push(col);
            }
        }

        let db = self.db.clone();
        let primary_key_index = self.primary_key_index;

        future_into_py(py, async move {
            db.insert(DynRecord::new(cols, primary_key_index))
                .await
                .map_err(CommitError::from)?;
            Ok(Python::with_gil(|py| PyDict::new_bound(py).into_py(py)))
        })
    }

    fn get<'py>(&'py self, py: Python<'py>, key: Py<PyAny>) -> PyResult<Bound<'py, PyAny>> {
        let col_desc = self.desc.get(self.primary_key_index).unwrap();
        let col = to_col(py, col_desc, key);
        let db = self.db.clone();
        let primary_key_index = self.primary_key_index;
        future_into_py(py, async move {
            let record = db
                .get(&col, |e| e.get().columns)
                .await
                .map_err(CommitError::from)?;
            Ok(Python::with_gil(|py| match record {
                Some(record) => to_dict(py, primary_key_index, record).into_py(py),
                None => py.None(),
            }))
        })
    }

    fn remove<'py>(&'py self, py: Python<'py>, key: Py<PyAny>) -> PyResult<Bound<PyAny>> {
        let col_desc = self.desc.get(self.primary_key_index).unwrap();
        let col = to_col(py, col_desc, key);
        let db = self.db.clone();
        future_into_py(py, async move {
            let ret = db.remove(col).await.map_err(CommitError::from)?;
            Ok(Python::with_gil(|py| ret.into_py(py)))
        })
    }

    /// open an optimistic ACID transaction
    fn transaction<'py>(&'py self, py: Python<'py>) -> PyResult<Bound<PyAny>> {
        let db = self.db.clone();
        let desc = self.desc.clone();
        future_into_py(py, async move {
            let txn = db.transaction().await;
            Ok(Transaction::new(txn, desc.clone()))
        })
    }
}