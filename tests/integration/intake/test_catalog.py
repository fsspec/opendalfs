"""Intake catalog coverage adapted from Intake 2.0.9 CSV tests."""

import fsspec
import intake
import pandas as pd
import pandas.testing as tm


def test_catalog_reads_csv_from_opendal(opendal_url):
    catalog_url = f"{opendal_url}/intake/catalog.yml"
    data_url = f"{opendal_url}/intake/file.csv"
    expected = pd.DataFrame({"a": [0], "b": [1]})

    with fsspec.open(data_url, "wt") as stream:
        expected.to_csv(stream, index=False)

    with fsspec.open(catalog_url, "wt") as stream:
        stream.write(
            f"""\
sources:
  implicit:
    driver: csv
    description: relative URL
    args:
      urlpath: '{{{{CATALOG_DIR}}}}/file.csv'
  explicit:
    driver: csv
    description: full URL
    args:
      urlpath: '{data_url}'
"""
        )

    catalog = intake.open_catalog(catalog_url)

    tm.assert_frame_equal(catalog.implicit.read(), expected)
    tm.assert_frame_equal(catalog.explicit.read(), expected)
