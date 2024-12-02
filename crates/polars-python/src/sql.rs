use std::sync::Mutex;

use polars::sql::SQLContext;
use pyo3::prelude::*;

use crate::error::PyPolarsErr;
use crate::PyLazyFrame;

#[pyclass]
#[repr(transparent)]
pub struct PySQLContext {
    pub context: Mutex<SQLContext>,
}

#[pymethods]
#[allow(
    clippy::wrong_self_convention,
    clippy::should_implement_trait,
    clippy::len_without_is_empty
)]
impl PySQLContext {
    #[staticmethod]
    #[allow(clippy::new_without_default)]
    pub fn new() -> PySQLContext {
        PySQLContext {
            context: Mutex::new(SQLContext::new()),
        }
    }

    pub fn execute(&mut self, query: &str) -> PyResult<PyLazyFrame> {
        Ok(self
            .context
            .lock()
            .unwrap()
            .execute(query)
            .map_err(PyPolarsErr::from)?
            .into())
    }

    pub fn get_tables(&self) -> PyResult<Vec<String>> {
        Ok(self.context.lock().unwrap().get_tables())
    }

    pub fn register(&mut self, name: &str, lf: PyLazyFrame) {
        self.context.lock().unwrap().register(name, lf.ldf)
    }

    pub fn unregister(&mut self, name: &str) {
        self.context.lock().unwrap().unregister(name)
    }
}
