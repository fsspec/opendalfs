"""Exercise the dvc-objects fsspec entry point tracked by issue #51.

dvc-objects 5.2.0 delegates these ``FileSystem`` operations to the wrapped
fsspec filesystem. The test passes opendalfs through the existing
``MemoryFileSystem(fs=...)`` entry point.
"""

from dvc_objects.fs.memory import MemoryFileSystem


def test_dvc_objects_find_walk_put_and_get(tmp_path, opendal_fs, opendal_root):
    fs = MemoryFileSystem(fs=opendal_fs)
    sources = {
        tmp_path / "sources" / "one.txt": b"one",
        tmp_path / "sources" / "nested" / "two.txt": b"two",
    }
    for path, content in sources.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)

    remote_root = f"{opendal_root}/dvc-objects/dataset"
    remote_paths = [f"{remote_root}/one.txt", f"{remote_root}/nested/two.txt"]
    fs.put([str(path) for path in sources], remote_paths)

    assert set(fs.find(remote_root)) == set(remote_paths)
    assert [
        (root, set(dirs), set(files)) for root, dirs, files in fs.walk(remote_root)
    ] == [
        (remote_root, {"nested"}, {"one.txt"}),
        (f"{remote_root}/nested", set(), {"two.txt"}),
    ]

    downloads = [tmp_path / "downloads" / path for path in ("one.txt", "two.txt")]
    fs.get(remote_paths, [str(path) for path in downloads])

    assert [path.read_bytes() for path in downloads] == [b"one", b"two"]
