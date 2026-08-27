"""Intake catalog coverage adapted from Intake 2.0.9 CSV tests."""

import fsspec
import intake
import pandas as pd
import pandas.testing as tm


def test_catalog_csv_source_reads_opendal_url(tmp_path, opendal_url):
    data_url = f"{opendal_url}/intake/table.csv"
    expected = pd.DataFrame({"value": [1, 2, 3]})
    with fsspec.open(data_url, "wb") as stream:
        stream.write(expected.to_csv(index=False).encode())

    catalog_path = tmp_path / "catalog.yml"
    catalog_path.write_text(
        f"""\
metadata: {{}}
sources:
  table:
    driver: csv
    args:
      urlpath: {data_url}
"""
    )

    result = intake.open_catalog(catalog_path).table.read()

    tm.assert_frame_equal(result, expected)
