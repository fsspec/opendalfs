# Intake

Intake catalogs can point to data using installed OpenDAL URLs, including paths
relative to the catalog itself.

## Open a CSV catalog

```python
import fsspec
import intake
import pandas as pd

protocol = "opendal+s3"
fsspec.config.conf[protocol] = {
    "endpoint": "http://127.0.0.1:9000",
    "region": "us-east-1",
    "access_key_id": "minioadmin",
    "secret_access_key": "minioadmin",
}
catalog_url = f"{protocol}://test-bucket/intake/catalog.yml"
data_url = f"{protocol}://test-bucket/intake/data.csv"

with fsspec.open(data_url, "wt") as stream:
    pd.DataFrame({"value": [1, 2]}).to_csv(stream, index=False)

with fsspec.open(catalog_url, "wt") as stream:
    stream.write(
        """\
sources:
  data:
    driver: csv
    args:
      urlpath: '{{CATALOG_DIR}}/data.csv'
"""
    )

catalog = intake.open_catalog(catalog_url)
assert catalog.data.read()["value"].tolist() == [1, 2]
```

## Test coverage

The repository tests relative and full `opendal+s3://` URLs in one catalog.

See
[`tests/integration/intake/test_catalog.py`](https://github.com/fsspec/opendalfs/blob/main/tests/integration/intake/test_catalog.py).
