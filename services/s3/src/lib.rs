use opendal::services::S3;
use opendal::Operator;
use opendalfs::OpendalFileSystem;
use pyo3::prelude::*;

#[derive(Default)]
#[pyclass(extends=OpendalFileSystem)]
pub struct S3FileSystem {
    bucket: String,
    region: String,
    root: Option<String>,
    endpoint: Option<String>,
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
}

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
        let cfg = S3FileSystem {
            root,
            bucket,
            region,
            endpoint,
            access_key_id,
            secret_access_key,
        };

        let mut builder = S3::default();
        builder.root(&cfg.root.clone().unwrap_or_default());
        builder.endpoint(&cfg.endpoint.clone().unwrap_or_default());
        builder.bucket(&cfg.bucket);
        builder.region(&cfg.region);
        builder.access_key_id(&cfg.access_key_id.clone().unwrap_or_default());
        builder.secret_access_key(&cfg.secret_access_key.clone().unwrap_or_default());

        let op = Operator::new(builder).unwrap().finish();
        (cfg, OpendalFileSystem::from(op))
    }
}

#[pymodule]
fn opendalfs_service_s3(_: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<S3FileSystem>()?;

    Ok(())
}
