# DVC Objects

`dvc-objects` can wrap an `opendalfs` instance through its existing fsspec
filesystem entry point.

## Transfer and discover files

```python
from pathlib import Path

from dvc_objects.fs.memory import MemoryFileSystem

from opendalfs import OpendalFileSystem

opendal_fs = OpendalFileSystem("memory", skip_instance_cache=True)
fs = MemoryFileSystem(fs=opendal_fs)

source = Path("source.txt")
source.write_text("hello from DVC Objects")
remote_path = "dvc-objects/source.txt"
fs.put([str(source)], [remote_path])

assert list(fs.find("dvc-objects")) == [remote_path]

download = Path("downloads/download.txt")
fs.get([remote_path], [str(download)])
assert download.read_text() == "hello from DVC Objects"
```

## Test coverage

The repository tests `put`, `get`, `find`, and `walk` with nested paths.

See
[`tests/integration/dvc_objects/test_filesystem.py`](https://github.com/fsspec/opendalfs/blob/main/tests/integration/dvc_objects/test_filesystem.py).
