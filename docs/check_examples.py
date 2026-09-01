import copy
import os
import re
from pathlib import Path

import fsspec.config
import pytest

from tests.utils.s3 import S3Config, cleanup_bucket, create_test_bucket

DOCS_ROOT = Path(__file__).parent
PYTHON_BLOCK = re.compile(r"^```python\n(.*?)^```$", re.MULTILINE | re.DOTALL)
EXAMPLE_GROUP = re.compile(r"<!-- docs-example-group: ([a-z0-9-]+) -->")


def python_example_files():
    active_group = os.environ.get("DOCS_EXAMPLE_GROUP")
    paths = []

    for path in sorted(DOCS_ROOT.rglob("*.md")):
        content = path.read_text()
        if not PYTHON_BLOCK.search(content):
            continue

        marker = EXAMPLE_GROUP.search(content)
        if (marker.group(1) if marker else None) == active_group:
            paths.append(path)

    return paths


@pytest.fixture(scope="session", autouse=True)
def s3_example_bucket():
    paths = python_example_files()
    if not any("opendal+s3://" in path.read_text() for path in paths):
        yield
        return

    config = S3Config.from_env()
    create_test_bucket(config)
    yield
    cleanup_bucket(config)


@pytest.mark.parametrize(
    "path",
    python_example_files(),
    ids=lambda path: str(path.relative_to(DOCS_ROOT)),
)
def test_python_examples(path, tmp_path, monkeypatch):
    """Run a page's examples in order, sharing the page's Python namespace."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(fsspec.config, "conf", copy.deepcopy(fsspec.config.conf))
    namespace = {"__name__": "__docs_example__"}

    for index, match in enumerate(PYTHON_BLOCK.finditer(path.read_text()), start=1):
        code = compile(match.group(1), f"{path} example {index}", "exec")
        exec(code, namespace)  # noqa: S102 - executing documentation is the test
