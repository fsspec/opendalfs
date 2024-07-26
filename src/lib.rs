mod spec;

use pyo3::prelude::*;

#[pymodule]
fn _opendalfs(_: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<spec::OpendalFileSystem>()?;

    Ok(())
}
