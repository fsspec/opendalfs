use opendal::raw::{build_rooted_abs_path, normalize_path, normalize_root};
use opendal::{EntryMode, ErrorKind, Operator};
use pyo3::exceptions::{PyException, PyFileNotFoundError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

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
    fn ls<'py>(&self, py: Python<'py>, path: &str) -> PyResult<pyo3::Bound<'py, PyList>> {
        let entries = self
            .op
            .blocking()
            .list(path)
            .unwrap()
            .into_iter()
            .map(|v| v.path().to_string())
            .collect::<Vec<_>>();
        Ok(PyList::new_bound(py, &entries))
    }

    fn mkdir(&self, _py: Python, path: &str, create_parents: bool) -> PyResult<()> {
        let path = normalize_path(path);
        // check if the path is valid
        if path.is_empty() || !path.ends_with('/') {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "Path is not a valid directory",
            ));
        }
        // split path into parent and leaf
        let (_parent, leaf) = path.split_once('/').unwrap_or(("", &path));
        if leaf.is_empty() || create_parents {
            match self.op.blocking().create_dir(&path) {
                Ok(_) => Ok(()),
                Err(e) => Err(pyo3::exceptions::PyValueError::new_err(e.to_string())),
            }
        } else {
            Err(pyo3::exceptions::PyValueError::new_err(
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
        if !self.check_path_exists(&path)? {
            return Err(pyo3::exceptions::PyFileNotFoundError::new_err(
                "Path does not exist",
            ));
        }
        // we need to check if the path is a directory
        let metadata = match self.op.blocking().stat(&path) {
            Ok(metadata) => metadata,
            Err(e) => return Err(pyo3::exceptions::PyValueError::new_err(e.to_string())),
        };
        if !metadata.is_dir() {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "Path is not a directory",
            ));
        }

        if recursive {
            self.op.blocking().remove_all(&path).unwrap();
        } else {
            let entries = self
                .op
                .blocking()
                .list_with(&path)
                .recursive(true)
                .call()
                .unwrap();
            let entries_without_path_itself = entries
                .iter()
                .filter(|entry| entry.path() != path)
                .collect::<Vec<_>>();
            if entries_without_path_itself.is_empty() {
                self.op.blocking().delete(&path).unwrap();
            } else {
                return Err(pyo3::exceptions::PyFileExistsError::new_err(
                    "Directory is not empty",
                ));
            }
        }
        Ok(())
    }

    fn check_path_exists(&self, path: &str) -> PyResult<bool> {
        match self.op.blocking().exists(path) {
            Ok(exists) => Ok(exists),
            Err(e) => Err(pyo3::exceptions::PyException::new_err(e.to_string())),
        }
    }

    fn info<'py>(&self, py: Python<'py>, path: &str) -> PyResult<Bound<'py, PyDict>> {
        let path = normalize_path(path);
        let root = normalize_root(self.op.info().root());
        let abs_path = build_rooted_abs_path(&root, &path);

        let metadata = self.op.blocking().stat(&path).map_err(|e| match e.kind() {
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
}
