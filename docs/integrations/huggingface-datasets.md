# Hugging Face Datasets

Hugging Face Datasets can load data from an installed `opendal+s3` URL in both
eager and streaming modes.

## Load JSON Lines from a URL

```python
from pathlib import Path

import fsspec
from datasets import load_dataset

protocol = "opendal+s3"
storage_options = {
    "endpoint": "http://127.0.0.1:9000",
    "region": "us-east-1",
    "access_key_id": "minioadmin",
    "secret_access_key": "minioadmin",
}
data_url = f"{protocol}://test-bucket/datasets/records.jsonl"

with fsspec.open(data_url, "wb", **storage_options) as stream:
    stream.write(b'{"text":"first","label":0}\n')
    stream.write(b'{"text":"second","label":1}\n')

dataset = load_dataset(
    "json",
    data_files=data_url,
    split="train",
    cache_dir=Path("cache"),
    storage_options={protocol: storage_options},
)

assert list(dataset) == [
    {"text": "first", "label": 0},
    {"text": "second", "label": 1},
]
```

## Test coverage

The repository runs this case in eager and streaming modes against MinIO.

See
[`tests/integration/huggingface_datasets/test_load_dataset.py`](https://github.com/fsspec/opendalfs/blob/main/tests/integration/huggingface_datasets/test_load_dataset.py).
