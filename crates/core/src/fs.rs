use opendal::raw::{build_rooted_abs_path, normalize_path, normalize_root};
use opendal::{EntryMode, ErrorKind, Operator, Scheme};
use pyo3::exceptions::{PyException, PyFileNotFoundError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDateTime, PyDict};
use pyo3_async_runtimes::tokio::future_into_py;
use std::collections::HashMap;
use std::str::FromStr;

#[pyclass(subclass)]
pub struct OpendalFileSystem {
    op: Operator,
}

impl From<Operator> for OpendalFileSystem {
    fn from(op: Operator) -> Self {
        Self { op }
    }
}

#[pymethods]
impl OpendalFileSystem {
    #[new]
    #[pyo3(signature = (**kwargs))]
    fn new(kwargs: Option<HashMap<String, String>>) -> PyResult<Self> {
        // Get scheme from class name (e.g., MemoryFileSystem -> "memory")
        let scheme = Python::with_gil(|py| {
            let cls = py.get_type_bound::<Self>();
            let cls_name = cls.name()?; // Store the temporary value
            let name = cls_name.to_string_lossy(); // Now name borrows from cls_name
            let scheme = name
                .strip_suffix("FileSystem")
                .ok_or_else(|| PyValueError::new_err("Invalid class name"))?
                .to_lowercase();
            Ok::<_, PyErr>(scheme)
        })?;

        // Convert scheme string to OpenDAL Scheme
        let scheme = Scheme::from_str(&scheme)
            .map_err(|e| PyValueError::new_err(format!("Invalid scheme: {}", e)))?;

        // Build operator using via_iter
        let op = if let Some(kwargs) = kwargs {
            Operator::via_iter(scheme, kwargs.into_iter())
                .map_err(|e| PyValueError::new_err(e.to_string()))?
        } else {
            Operator::via_iter(scheme, std::iter::empty::<(String, String)>())
                .map_err(|e| PyValueError::new_err(e.to_string()))?
        };

        Ok(Self { op })
    }

    /// List contents of a path
    fn ls<'p>(&self, py: Python<'p>, path: &str) -> PyResult<Bound<'p, PyAny>> {
        let path = normalize_path(path);
        let op = self.op.clone();

        future_into_py(py, async move {
            op.list(&path)
                .await
                .map(|entries| {
                    entries
                        .iter()
                        .map(|entry| entry.path().to_string())
                        .collect::<Vec<String>>()
                })
                .map_err(|e| PyException::new_err(e.to_string()))
        })
    }

    /// Create a directory
    fn mkdir<'p>(
        &self,
        py: Python<'p>,
        path: &str,
        create_parents: bool,
    ) -> PyResult<Bound<'p, PyAny>> {
        let path = normalize_path(path);
        if path.is_empty() || !path.ends_with('/') {
            return Err(PyValueError::new_err("Path is not a valid directory"));
        }

        let (_parent, leaf) = path.split_once('/').unwrap_or(("", &path));
        if !leaf.is_empty() && !create_parents {
            return Err(PyValueError::new_err(
                "Cannot create parents directory without creating_parents=True",
            ));
        }

        let op = self.op.clone();
        future_into_py(py, async move {
            op.create_dir(&path)
                .await
                .map_err(|e| PyValueError::new_err(e.to_string()))
        })
    }

    fn mkdirs<'p>(&self, py: Python<'p>, path: &str, exist_ok: bool) -> PyResult<()> {
        let path = normalize_path(path);
        let exists = self.check_path_exists(py, &path)?;
        if exists && !exist_ok {
            return Err(pyo3::exceptions::PyFileExistsError::new_err(
                "Path already exists",
            ));
        }
        match self.mkdir(py, &path, true) {
            Ok(_) => Ok(()),
            Err(e) => Err(pyo3::exceptions::PyValueError::new_err(e.to_string())),
        }
    }

    fn rmdir<'p>(&self, py: Python<'p>, path: &str, recursive: bool) -> PyResult<Bound<'p, PyAny>> {
        let path = normalize_path(path);
        if !path.ends_with('/') {
            return Err(PyFileNotFoundError::new_err("Path does not exist"));
        }

        let op = self.op.clone();
        future_into_py(py, async move {
            // Check if path exists and is a directory
            if !op
                .exists(&path)
                .await
                .map_err(|e| PyException::new_err(e.to_string()))?
            {
                return Err(PyFileNotFoundError::new_err("Path does not exist"));
            }

            let metadata = op
                .stat(&path)
                .await
                .map_err(|e| PyException::new_err(e.to_string()))?;

            if !metadata.is_dir() {
                return Err(PyException::new_err("Path is not a directory"));
            }

            if recursive {
                op.remove_all(&path)
                    .await
                    .map_err(|e| PyException::new_err(e.to_string()))
            } else {
                // Check if directory is empty
                let entries = op
                    .list(&path)
                    .await
                    .map_err(|e| PyException::new_err(e.to_string()))?;

                let real_entries: Vec<_> = entries
                    .into_iter()
                    .filter(|entry| entry.path() != path)
                    .collect();

                if real_entries.is_empty() {
                    op.delete(&path)
                        .await
                        .map_err(|e| PyException::new_err(e.to_string()))
                } else {
                    Err(pyo3::exceptions::PyFileExistsError::new_err(format!(
                        "Directory is not empty. Contains: {:?}",
                        real_entries.iter().map(|e| e.path()).collect::<Vec<_>>()
                    )))
                }
            }
        })
    }

    fn check_path_exists<'p>(&self, py: Python<'p>, path: &str) -> PyResult<bool> {
        let path = normalize_path(path);
        let op = self.op.clone();

        let fut = future_into_py(py, async move {
            op.exists(&path)
                .await
                .map_err(|e| PyException::new_err(e.to_string()))
        })?;

        // Extract bool from the Python future result
        fut.extract()
    }

    fn info<'py>(&self, py: Python<'py>, path: &str) -> PyResult<Bound<'py, PyAny>> {
        let path = normalize_path(path);
        let root = normalize_root(self.op.info().root());
        let abs_path = build_rooted_abs_path(&root, &path);
        let op = self.op.clone();

        future_into_py(py, async move {
            let metadata = op.stat(&path).await.map_err(|e| match e.kind() {
                ErrorKind::NotFound => PyFileNotFoundError::new_err(e.to_string()),
                _ => PyException::new_err(e.to_string()),
            })?;

            Python::with_gil(|py| {
                let dict = PyDict::new_bound(py);
                dict.set_item("size", metadata.content_length())?;
                dict.set_item("path", abs_path)?;
                dict.set_item(
                    "type",
                    match metadata.mode() {
                        EntryMode::FILE => "file",
                        EntryMode::DIR => "directory",
                        EntryMode::Unknown => "unknown",
                    },
                )?;
                Ok(dict.into_py(py))
            })
        })
    }
    fn rm_file<'p>(&self, py: Python<'p>, path: &str) -> PyResult<Bound<'p, PyAny>> {
        let path = normalize_path(path);
        let op = self.op.clone();

        future_into_py(py, async move {
            op.delete(&path)
                .await
                .map_err(|e| PyException::new_err(e.to_string()))?;
            Python::with_gil(|py| Ok(py.None()))
        })
    }

    /// Private helper method to read file contents
    fn _read<'p>(&self, py: Python<'p>, path: &str) -> PyResult<Bound<'p, PyAny>> {
        let path = normalize_path(path);
        let op = self.op.clone();

        future_into_py(py, async move {
            match op.read(&path).await {
                Ok(data) => {
                    let bytes = data.to_vec();
                    Python::with_gil(|py| Ok(bytes.into_py(py)))
                }
                Err(e) => Err(PyException::new_err(e.to_string())),
            }
        })
    }

    /// Private helper method to write file contents
    fn _write<'p>(&self, py: Python<'p>, path: &str, data: Vec<u8>) -> PyResult<Bound<'p, PyAny>> {
        let path = normalize_path(path);
        let op = self.op.clone();

        future_into_py(py, async move {
            op.write(&path, data)
                .await
                .map_err(|e| PyException::new_err(e.to_string()))?;
            Python::with_gil(|py| Ok(py.None()))
        })
    }
    fn modified<'py>(&self, py: Python<'py>, path: &str) -> PyResult<Bound<'py, PyAny>> {
        let path = normalize_path(path);
        let op = self.op.clone();

        future_into_py(py, async move {
            let metadata = op.stat(&path).await.map_err(|e| match e.kind() {
                ErrorKind::NotFound => PyFileNotFoundError::new_err(e.to_string()),
                _ => PyException::new_err(e.to_string()),
            })?;

            if let Some(time) = metadata.last_modified() {
                let timestamp = time.timestamp() as f64;
                // Create the datetime in the outer scope where we have the right lifetime
                Python::with_gil(|py| {
                    let dt = PyDateTime::from_timestamp_bound(py, timestamp, None)?;
                    Ok(dt.into_py(py))
                })
            } else {
                Err(PyException::new_err("Last modified time not available"))
            }
        })
    }

    /// Check if a path exists in the storage system
    /// # Why three methods?
    /// - S3-like storage systems can be inconsistent in path handling
    /// - Different operations may have different permissions
    /// - Need to handle both files and directories
    fn exists<'p>(&self, py: Python<'p>, path: &str) -> PyResult<Bound<'p, PyAny>> {
        let path = normalize_path(path);
        let op = self.op.clone();

        future_into_py(py, async move {
            // Try direct existence check first
            if let Ok(exists) = op.exists(&path).await {
                return Ok(exists);
            }

            // Try stat as fallback
            match op.stat(&path).await {
                Ok(_) => Ok(true),
                Err(e) if e.kind() == ErrorKind::NotFound => Ok(false),
                Err(e) => Err(PyException::new_err(e.to_string())),
            }
        })
    }
}
