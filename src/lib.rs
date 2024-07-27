mod fs;

use pyo3::prelude::*;

#[pymodule]
fn opendalfs(_: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<fs::OpendalFileSystem>()?;

    Ok(())
}
