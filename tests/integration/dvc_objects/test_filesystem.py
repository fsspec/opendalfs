"""dvc-objects compatibility cases migrated for issue #51.

Source: dvc-objects 5.2.0,
``tests/fs/test_generic.py::test_copy`` and
``tests/fs/test_localfs.py::test_walk``. The operations pass an opendalfs
instance through dvc-objects' existing ``MemoryFileSystem(fs=...)`` entry point.
"""

from dvc_objects.fs.memory import MemoryFileSystem

from opendalfs import OpendalFileSystem


def test_dvc_objects_find_walk_put_and_get(tmp_path):
    fs = MemoryFileSystem(
        fs=OpendalFileSystem(
            scheme="memory",
            asynchronous=False,
            skip_instance_cache=True,
        )
    )
    sources = {
        tmp_path / "sources" / "one.txt": b"one",
        tmp_path / "sources" / "nested" / "two.txt": b"two",
    }
    for path, content in sources.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)

    remote_paths = ["dataset/one.txt", "dataset/nested/two.txt"]
    fs.put([str(path) for path in sources], remote_paths)

    assert set(fs.find("dataset")) == set(remote_paths)
    assert [
        (root, set(dirs), set(files)) for root, dirs, files in fs.walk("dataset")
    ] == [
        ("dataset", {"nested"}, {"one.txt"}),
        ("dataset/nested", set(), {"two.txt"}),
    ]

    downloads = [tmp_path / "downloads" / path for path in ("one.txt", "two.txt")]
    fs.get(remote_paths, [str(path) for path in downloads])

    assert [path.read_bytes() for path in downloads] == [b"one", b"two"]
