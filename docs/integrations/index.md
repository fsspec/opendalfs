# Integrations

Libraries can reach `opendalfs` through several fsspec entry forms. The table
below records the form exercised by this repository's integration suite.

"CI-tested" means that the named workflow passes with dependencies from
`uv.lock` against the memory, local filesystem, or MinIO-backed S3 fixtures used
by that test. It is not a claim about every feature or every OpenDAL service.

| Integration | Tested entry form | Tested workflow |
| --- | --- | --- |
| {doc}`pandas` | URL, filesystem | CSV read; Parquet read and write |
| {doc}`dask` | URL, filesystem | CSV glob and tokenization; Parquet read |
| {doc}`pyarrow` | filesystem adapter | Parquet dataset read and write |
| {doc}`xarray` | mapper | Zarr dataset read and write |
| {doc}`zarr` | URL-backed store | Zarr array read and write |
| {doc}`airflow` | URL path | `ObjectStoragePath` read, write, and unlink |
| {doc}`dvc-objects` | filesystem | find, walk, put, and get |
| {doc}`huggingface-datasets` | URL | JSON dataset loading |
| {doc}`intake` | URL | Catalog and CSV loading |
| {doc}`kerchunk` | URL, file-like object | HDF5 to Zarr reference generation |
| {doc}`pytorch-lightning` | URL | Checkpoint save and restore |
| {doc}`prefect` | URL | Remote filesystem read and write |
| {doc}`rechunker` | mapper | Rechunk between fsspec mappings |
| {doc}`torchdata` | URL | File listing and opening data pipes |
| {doc}`universal-pathlib` | URL path | Read, write, listing, and existence checks |

## Choosing an entry form

Use a URL when the library delegates protocol resolution to fsspec. Pass
`storage_options` alongside the URL when the service needs configuration.

Use an explicit filesystem when the library accepts one. This avoids repeated
URL resolution and gives the application direct control over instance caching
and the service root.

Use `fs.get_mapper(path)` for libraries that expect a mutable mapping, notably
Zarr-based workflows. PyArrow has its own filesystem interface and provides an
`FSSpecHandler` adapter.

## Adding an integration

An integration page should follow a passing test, not precede one. Add the
smallest test that exercises the library's public fsspec entry point, then
document the same construction style and state what the test covers.

See {doc}`../contributing/documentation` for the page checklist.

```{toctree}
:hidden:

pandas
dask
pyarrow
xarray
zarr
airflow
dvc-objects
huggingface-datasets
intake
kerchunk
prefect
pytorch-lightning
rechunker
torchdata
universal-pathlib
```
