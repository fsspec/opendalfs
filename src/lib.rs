use pyo3::prelude::*;

#[pyfunction]
pub fn hello_world() {
    println!("Hello, world!");
}

#[pymodule]
fn _opendalfs(_: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(hello_world, m)?)?;

    Ok(())
}
