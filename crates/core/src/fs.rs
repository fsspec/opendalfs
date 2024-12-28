use opendal::raw::{build_rooted_abs_path, normalize_path, normalize_root};
use opendal::{EntryMode, ErrorKind, Operator};
use pyo3::exceptions::{PyException, PyFileNotFoundError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDateTime, PyDict};
use tokio::runtime::Runtime;

#[pyclass(subclass)]
pub struct OpendalFileSystem {
    op: Operator,
    rt: Runtime,
}

impl From<Operator> for OpendalFileSystem {
    fn from(op: Operator) -> Self {
        Self {
            op,
            rt: Runtime::new().unwrap(),
        }
    }
}

#[pymethods]
impl OpendalFileSystem {
    /// List contents of a path
    fn ls(&self, _py: Python, path: &str) -> PyResult<Vec<String>> {
        let path = normalize_path(path);

        // Use runtime to execute async list for S3
        self.rt.block_on(async {
            match self.op.list(&path).await {
                Ok(entries) => {
                    let paths: Vec<String> = entries
                        .iter()
                        .map(|entry| entry.path().to_string())
                        .collect();
                    Ok(paths)
                }
                Err(e) => Err(PyException::new_err(e.to_string())),
            }
        })
    }

    /// Create a directory
    fn mkdir(&self, _py: Python, path: &str, create_parents: bool) -> PyResult<()> {
        let path = normalize_path(path);
        if path.is_empty() || !path.ends_with('/') {
            return Err(PyValueError::new_err("Path is not a valid directory"));
        }

        let (_parent, leaf) = path.split_once('/').unwrap_or(("", &path));
        if leaf.is_empty() || create_parents {
            // Use async create_dir instead of blocking
            self.rt.block_on(async {
                match self.op.create_dir(&path).await {
                    Ok(_) => Ok(()),
                    Err(e) => Err(PyValueError::new_err(e.to_string())),
                }
            })
        } else {
            Err(PyValueError::new_err(
                "Cannot create parents directory without creating_parents=True",
            ))
        }
    }

    fn mkdirs(&self, _py: Python, path: &str, exist_ok: bool) -> PyResult<()> {
        let path = normalize_path(path);
        let exists = self.check_path_exists(&path)?;
        if exists && !exist_ok {
            return Err(pyo3::exceptions::PyFileExistsError::new_err(
                "Path already exists",
            ));
        }
        match self.mkdir(_py, &path, true) {
            Ok(_) => Ok(()),
            Err(e) => Err(pyo3::exceptions::PyValueError::new_err(e.to_string())),
        }
    }

    fn rmdir(&self, _py: Python, path: &str, recursive: bool) -> PyResult<()> {
        let path = normalize_path(path);
        if !path.ends_with('/') {
            return Err(PyFileNotFoundError::new_err("Path does not exist"));
        }

        // Use async operations for path existence check and metadata
        self.rt.block_on(async {
            // Check if path exists
            if !self
                .op
                .exists(&path)
                .await
                .map_err(|e| PyException::new_err(e.to_string()))?
            {
                return Err(PyFileNotFoundError::new_err("Path does not exist"));
            }

            // Check if it's a directory
            let metadata = match self.op.stat(&path).await {
                Ok(metadata) => metadata,
                Err(e) => return Err(PyException::new_err(e.to_string())),
            };
            if !metadata.is_dir() {
                return Err(PyException::new_err("Path is not a directory"));
            }

            if recursive {
                match self.op.remove_all(&path).await {
                    Ok(_) => Ok(()),
                    Err(e) => Err(PyException::new_err(e.to_string())),
                }
            } else {
                // List only immediate children
                let entries = match self.op.list(&path).await {
                    Ok(entries) => entries,
                    Err(e) => return Err(PyException::new_err(e.to_string())),
                };

                // Filter out the current directory entry if it exists
                let real_entries: Vec<_> = entries
                    .into_iter()
                    .filter(|entry| entry.path() != path)
                    .collect();

                if real_entries.is_empty() {
                    match self.op.delete(&path).await {
                        Ok(_) => Ok(()),
                        Err(e) => Err(PyException::new_err(e.to_string())),
                    }
                } else {
                    Err(pyo3::exceptions::PyFileExistsError::new_err(format!(
                        "Directory is not empty. Contains: {:?}",
                        real_entries.iter().map(|e| e.path()).collect::<Vec<_>>()
                    )))
                }
            }
        })
    }

    fn check_path_exists(&self, path: &str) -> PyResult<bool> {
        self.rt.block_on(async {
            match self.op.exists(path).await {
                Ok(exists) => Ok(exists),
                Err(e) => Err(PyException::new_err(e.to_string())),
            }
        })
    }

    fn info<'py>(&self, py: Python<'py>, path: &str) -> PyResult<Bound<'py, PyDict>> {
        let path = normalize_path(path);
        let root = normalize_root(self.op.info().root());
        let abs_path = build_rooted_abs_path(&root, &path);

        // Use async stat instead of blocking
        let metadata = self
            .rt
            .block_on(async { self.op.stat(&path).await })
            .map_err(|e| match e.kind() {
                ErrorKind::NotFound => PyFileNotFoundError::new_err(e.to_string()),
                _ => PyException::new_err(e.to_string()),
            })?;

        let mode = match metadata.mode() {
            EntryMode::FILE => "file",
            EntryMode::DIR => "directory",
            EntryMode::Unknown => "other",
        };

        let dict = PyDict::new_bound(py);
        dict.set_item("name", abs_path)?;
        dict.set_item("size", metadata.content_length())?;
        dict.set_item("type", mode)?;

        Ok(dict)
    }

    fn rm_file(&self, _py: Python, path: &str) -> PyResult<()> {
        let path = normalize_path(path);
        let path_with_slash = format!("{}/", path);

        // Check if path exists (either with or without trailing slash)
        let exists = self
            .rt
            .block_on(async {
                Ok(self.op.exists(&path).await? || self.op.exists(&path_with_slash).await?)
            })
            .map_err(|e: opendal::Error| PyException::new_err(e.to_string()))?;

        if !exists {
            return Err(PyFileNotFoundError::new_err("File does not exist"));
        }

        // Check if it's actually a file
        let metadata = self
            .rt
            .block_on(async {
                match self.op.stat(&path).await {
                    Ok(metadata) => Ok(metadata),
                    Err(_) => self.op.stat(&path_with_slash).await,
                }
            })
            .map_err(|e| PyException::new_err(e.to_string()))?;

        if !metadata.is_file() {
            return Err(PyException::new_err("Path is not a file"));
        }

        // Delete the file
        self.rt
            .block_on(async { self.op.delete(&path).await })
            .map_err(|e| PyException::new_err(e.to_string()))
    }

    /// Private helper method to read file contents
    fn _read(&self, path: &str) -> PyResult<Vec<u8>> {
        let path = normalize_path(path);
        self.rt.block_on(async {
            match self.op.read(&path).await {
                Ok(data) => {
                    let bytes = data.to_vec();
                    Ok(bytes)
                }
                Err(e) => Err(PyException::new_err(e.to_string())),
            }
        })
    }

    /// Private helper method to write file contents
    fn _write(&self, path: &str, data: Vec<u8>) -> PyResult<()> {
        let path = normalize_path(path);
        self.rt.block_on(async {
            match self.op.write(&path, data).await {
                Ok(_) => Ok(()),
                Err(e) => Err(PyException::new_err(e.to_string())),
            }
        })
    }

    fn modified<'py>(&self, py: Python<'py>, path: &str) -> PyResult<Bound<'py, PyDateTime>> {
        let path = normalize_path(path);
        let metadata = self
            .rt
            .block_on(async { self.op.stat(&path).await })
            .map_err(|e| match e.kind() {
                ErrorKind::NotFound => PyFileNotFoundError::new_err(e.to_string()),
                _ => PyException::new_err(e.to_string()),
            })?;

        if let Some(time) = metadata.last_modified() {
            let timestamp = time.timestamp() as f64;
            PyDateTime::from_timestamp_bound(py, timestamp, None)
        } else {
            Err(PyException::new_err("Last modified time not available"))
        }
    }

    /// Check if a path exists in the storage system
    /// # Why three methods?
    /// - S3-like storage systems can be inconsistent in path handling
    /// - Different operations may have different permissions
    /// - Need to handle both files and directories
    fn exists(&self, path: &str) -> PyResult<bool> {
        let path = normalize_path(path);
        self.rt.block_on(async {
            // Method 1: List directory contents
            // Reason: Most reliable for S3, handles virtual directories
            if let Ok(entries) = self.op.list("/").await {
                for entry in &entries {
                    let entry_path = entry.path();
                    if entry_path == path
                        || entry_path == format!("/{}", path)
                        || entry_path.trim_start_matches('/') == path.trim_start_matches('/')
                    {
                        return Ok(true);
                    }
                }
            }

            // Method 2: Direct existence check
            // Reason: Faster when available, but may not work for all paths
            for test_path in &[path.to_string(), format!("/{}", path)] {
                if let Ok(exists) = self.op.exists(test_path).await {
                    if exists {
                        return Ok(true);
                    }
                }
            }

            // Method 3: Metadata check
            // Reason: Fallback method, works for both files and directories
            for test_path in &[path.to_string(), format!("/{}", path)] {
                if self.op.stat(test_path).await.is_ok() {
                    return Ok(true);
                }
            }

            Ok(false)
        })
    }
}
