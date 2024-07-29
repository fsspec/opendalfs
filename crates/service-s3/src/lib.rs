use opendal::services::S3Config;
use opendal::Operator;
use opendalfs_core::OpendalFileSystem;
use pyo3::prelude::*;

#[derive(Default)]
#[pyclass(extends=OpendalFileSystem)]
pub struct S3FileSystem;

#[pymethods]
impl S3FileSystem {
    #[new]
    #[pyo3(signature = (
        bucket,
        region,
        root=None,
        endpoint=None,
        access_key_id=None,
        secret_access_key=None,
    ))]
    pub fn new(
        bucket: String,
        region: String,
        root: Option<String>,
        endpoint: Option<String>,
        access_key_id: Option<String>,
        secret_access_key: Option<String>,
    ) -> (Self, OpendalFileSystem) {
        let mut cfg = S3Config::default();
        cfg.bucket = bucket;
        cfg.region = Some(region);
        cfg.root = root;
        cfg.endpoint = endpoint;
        cfg.access_key_id = access_key_id;
        cfg.secret_access_key = secret_access_key;

        let op = Operator::from_config(cfg).unwrap().finish();
        (S3FileSystem, OpendalFileSystem::from(op))
    }
}

#[pymodule]
fn opendalfs_service_s3(_: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<S3FileSystem>()?;

    Ok(())
}
