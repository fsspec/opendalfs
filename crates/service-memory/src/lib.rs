use opendal::services::MemoryConfig;
use opendal::Operator;
use opendalfs_core::OpendalFileSystem;
use pyo3::prelude::*;

#[derive(Default)]
#[pyclass(extends=OpendalFileSystem)]
pub struct MemoryFileSystem;

#[pymethods]
impl MemoryFileSystem {
    #[new]
    #[pyo3(signature = (root=None))]
    pub fn new(root: Option<String>) -> (Self, OpendalFileSystem) {
        let mut cfg = MemoryConfig::default();
        cfg.root = root;

        let op = Operator::from_config(cfg).unwrap().finish();
        (MemoryFileSystem, OpendalFileSystem::from(op))
    }
}

#[pymodule]
fn opendalfs_service_memory(_: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<MemoryFileSystem>()?;

    Ok(())
}
