"""universal_pathlib compatibility cases migrated for issue #51.

Source: universal-pathlib 0.3.10,
``docs/usage.md`` (the public ``protocol=`` constructor) and
``upath/tests/cases.py`` (``make_test_files``, ``test_iterdir``,
``test_write_text`` and ``test_write_bytes``).
"""

from upath import UPath


def test_upath_protocol_read_write_and_listing(opendal_fs, opendal_root):
    root = UPath(
        f"{opendal_root}/universal-pathlib",
        protocol=opendal_fs.protocol,
        **opendal_fs.storage_options,
    )

    folder = root / "folder1"
    folder.mkdir(parents=True)
    text_path = folder / "file1.txt"
    bytes_path = folder / "file2.txt"
    text_path.write_text("hello world")
    bytes_path.write_bytes(b"hello bytes")

    assert text_path.read_text() == "hello world"
    assert bytes_path.read_bytes() == b"hello bytes"

    children = set(folder.iterdir())
    assert children == {text_path, bytes_path}
    assert all(path.exists() for path in children)
