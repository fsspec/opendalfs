def _directory_behavior(fs, *, url: str, path: str, name_prefix: str) -> dict:
    fs.pipe_file(f"{url}/part.txt", b"one row")

    def relative_name(info: dict) -> str:
        return info["name"].removeprefix(name_prefix)

    return {
        "exists": fs.exists(url),
        "exists_with_slash": fs.exists(f"{url}/"),
        "isdir": fs.isdir(url),
        "isfile": fs.isfile(url),
        "info": {
            "name": relative_name(fs.info(url)),
            "size": fs.info(url)["size"],
            "type": fs.info(url)["type"],
        },
        "info_with_slash": {
            "name": relative_name(fs.info(f"{url}/")),
            "size": fs.info(f"{url}/")["size"],
            "type": fs.info(f"{url}/")["type"],
        },
        "listing": [
            {
                "name": relative_name(info),
                "size": info["size"],
                "type": info["type"],
            }
            for info in fs.ls(path, detail=True)
        ],
    }


def test_implicit_directory_behavior_matches_s3fs(s3_fs, s3fs_fs, s3_config):
    bucket = s3_config.bucket
    expected = _directory_behavior(
        s3fs_fs,
        url=f"s3://{bucket}/parity-dir",
        path=f"{bucket}/parity-dir",
        name_prefix=f"{bucket}/",
    )
    actual = _directory_behavior(
        s3_fs,
        url=f"opendal+s3://{bucket}/parity-dir",
        path="parity-dir",
        name_prefix="",
    )

    assert actual == expected


def test_arrow_reads_dataset_without_trailing_slash(s3_fs):
    import pyarrow as pa
    import pyarrow.dataset as ds
    import pyarrow.fs as pafs
    import pyarrow.parquet as pq

    arrow_fs = pafs.PyFileSystem(pafs.FSSpecHandler(s3_fs))
    pq.write_table(
        pa.table({"value": [1, 2, 3]}),
        "arrow-dataset/part-0.parquet",
        filesystem=arrow_fs,
    )

    table = ds.dataset(
        "arrow-dataset", filesystem=arrow_fs, format="parquet"
    ).to_table()

    assert table.to_pydict() == {"value": [1, 2, 3]}
