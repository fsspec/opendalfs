"""Compatibility cases migrated from Hugging Face datasets.

Source repository: https://github.com/huggingface/datasets
Source release: datasets 5.0.1 (PyPI and Git tag 5.0.1)
Source cases:
- tests/test_load.py:1017-1039, test_load_dataset_zip_jsonl
- tests/test_load.py:1258-1266, test_remote_data_files
"""

import fsspec
import pytest
from datasets import IterableDataset, load_dataset

from opendalfs import register_opendal_service


@pytest.mark.parametrize("streaming", [False, True])
def test_load_json_from_opendal_url(streaming, tmp_path):
    """Load records through the URL entry point used by Hugging Face datasets."""
    protocol = register_opendal_service("fs")
    storage_root = tmp_path / "storage"
    storage_root.mkdir()
    storage_options = {"root": str(storage_root)}
    data_url = f"{protocol}:///DataSet/records.jsonl"
    records = b'{"text":"first","label":0}\n{"text":"second","label":1}\n'
    with fsspec.open(data_url, "wb", **storage_options) as data_file:
        data_file.write(records)

    dataset = load_dataset(
        "json",
        data_files=data_url,
        split="train",
        streaming=streaming,
        cache_dir=tmp_path / "cache",
        storage_options={protocol: storage_options},
    )

    assert isinstance(dataset, IterableDataset) is streaming
    assert list(dataset) == [
        {"text": "first", "label": 0},
        {"text": "second", "label": 1},
    ]
