use opendal::Operator;
use opendal::Scheme;
use pyo3::prelude::*;
use pyo3::types::PyList;
use std::collections::HashMap;

#[pyclass]
pub struct OpendalFileSystem {
    op: Operator,
}

#[pymethods]
impl OpendalFileSystem {
    #[new]
    fn new() -> Self {
        Self {
            op: Operator::via_map(Scheme::Memory, HashMap::new()).unwrap(),
        }
    }

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
