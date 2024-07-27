use opendal::Operator;
use pyo3::prelude::*;
use pyo3::types::PyList;

#[pyclass(subclass)]
pub struct OpendalFileSystem {
    op: Operator,
}

impl From<Operator> for OpendalFileSystem {
    fn from(op: Operator) -> Self {
        Self { op }
    }
}

#[pymethods]
impl OpendalFileSystem {
    fn ls<'py>(&self, py: Python<'py>, path: &str) -> PyResult<pyo3::Bound<'py, PyList>> {
        let entries = self
            .op
            .blocking()
            .list(path)
            .unwrap()
            .into_iter()
            .map(|v| v.path().to_string())
            .collect::<Vec<_>>();
        Ok(PyList::new_bound(py, &entries))
    }
}
