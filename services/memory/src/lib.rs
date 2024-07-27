use opendal::services::Memory;
use opendal::Operator;
use opendalfs::OpendalFileSystem;
use pyo3::prelude::*;

#[derive(Default)]
#[pyclass(extends=OpendalFileSystem)]
pub struct MemoryFileSystem {
    root: Option<String>,
}

#[pymethods]
impl MemoryFileSystem {
    #[new]
    #[pyo3(signature = (root=None))]
    pub fn new(root: Option<String>) -> (Self, OpendalFileSystem) {
        let cfg = MemoryFileSystem { root };

        let mut builder = Memory::default();
        builder.root(&cfg.root.clone().unwrap_or_default());
        let op = Operator::new(builder).unwrap().finish();
        (cfg, OpendalFileSystem::from(op))
    }
}

#[pymodule]
fn opendalfs_service_memory(_: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<MemoryFileSystem>()?;

    Ok(())
}
