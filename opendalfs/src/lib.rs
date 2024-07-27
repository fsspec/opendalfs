mod fs;
pub use fs::OpendalFileSystem;

use pyo3::prelude::*;

#[pymodule]
fn _opendalfs(_: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<OpendalFileSystem>()?;

    Ok(())
}
