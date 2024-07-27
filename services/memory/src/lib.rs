use opendal::services::Memory;
use opendal::Operator;
use opendalfs::OpendalFileSystem;
use pyo3::prelude::*;
use pyo3::types::PyDict;

#[pyclass(extends=OpendalFileSystem)]
pub struct MemoryFileSystem;

#[derive(Default, Clone, FromPyObject)]
#[pyo3(from_item_all)]
pub struct MemoryConfig {
    root: Option<String>,
}

#[pymethods]
impl MemoryFileSystem {
    #[new]
    #[pyo3(signature = (**kwargs))]
    pub fn new(kwargs: Option<&Bound<'_, PyDict>>) -> (Self, OpendalFileSystem) {
        let cfg = kwargs
            .map(|arg| MemoryConfig::extract_bound(arg.as_any()))
            .transpose()
            .unwrap()
            .unwrap_or_default();

        let mut builder = Memory::default();
        builder.root(&cfg.root.unwrap_or_default());
        let op = Operator::new(builder).unwrap().finish();
        (MemoryFileSystem, OpendalFileSystem::from(op))
    }
}

#[pymodule]
fn opendalfs_service_memory(_: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<MemoryFileSystem>()?;

    Ok(())
}
