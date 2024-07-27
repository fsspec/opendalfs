use opendal::services::Memory;
use opendal::Operator;
use opendalfs::OpendalFileSystem;
use pyo3::prelude::*;

#[pyclass]
#[derive(Default)]
pub struct MemoryConfig {}

#[pymethods]
impl MemoryConfig {
    #[new]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn build(&self) -> PyResult<OpendalFileSystem> {
        let builder = Memory::default();
        let op = Operator::new(builder).unwrap().finish();
        Ok(OpendalFileSystem::from(op))
    }
}

#[pymodule]
fn opendalfs_service_memory(_: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<MemoryConfig>()?;

    Ok(())
}
