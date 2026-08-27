import re
from pathlib import Path

import pytest

DOCS_ROOT = Path(__file__).parent
PYTHON_BLOCK = re.compile(r"^```python\n(.*?)^```$", re.MULTILINE | re.DOTALL)


def python_example_files():
    return [
        path
        for path in sorted(DOCS_ROOT.rglob("*.md"))
        if PYTHON_BLOCK.search(path.read_text())
    ]


@pytest.mark.parametrize(
    "path",
    python_example_files(),
    ids=lambda path: str(path.relative_to(DOCS_ROOT)),
)
def test_python_examples(path, tmp_path, monkeypatch):
    """Run a page's examples in order, sharing the page's Python namespace."""
    monkeypatch.chdir(tmp_path)
    namespace = {"__name__": "__docs_example__"}

    for index, match in enumerate(PYTHON_BLOCK.finditer(path.read_text()), start=1):
        code = compile(match.group(1), f"{path} example {index}", "exec")
        exec(code, namespace)  # noqa: S102 - executing documentation is the test
