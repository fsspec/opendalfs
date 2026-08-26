"""universal_pathlib compatibility cases migrated for issue #51.

Source: universal-pathlib 0.3.10,
``docs/usage.md`` (the public ``protocol=`` constructor) and
``upath/tests/cases.py`` (``make_test_files``, ``test_iterdir``,
``test_write_text`` and ``test_write_bytes``).
"""

from upath import UPath

from opendalfs import register_opendal_service


def test_upath_protocol_read_write_and_listing():
    protocol = register_opendal_service("memory")
    root = UPath("universal-pathlib", protocol=protocol)

    folder = root / "folder1"
    folder.mkdir(parents=True)
    (folder / "file1.txt").write_text("hello world")
    (folder / "file2.txt").write_bytes(b"hello bytes")

    assert (folder / "file1.txt").read_text() == "hello world"
    assert (folder / "file2.txt").read_bytes() == b"hello bytes"
    assert {path.name for path in folder.iterdir()} == {
        "file1.txt",
        "file2.txt",
    }
