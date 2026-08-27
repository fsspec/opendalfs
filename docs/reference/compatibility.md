# Compatibility reference

The test suite checks compatibility at three levels.

## fsspec contract

The upstream fsspec abstract tests exercise copy, get, put, pipe, and open
behavior against memory and S3 fixtures. Local tests add coverage for listing,
metadata shape, directory mutation, recursive operations, checksums, range
reads, and path-like inputs.

## s3fs parity

Selected tests run `opendalfs` and `s3fs` against the same MinIO bucket. They
compare URL behavior, local transfers, and modified timestamps where the two
implementations should expose the same fsspec semantics.

This is targeted parity, not an assertion that every `s3fs` extension exists in
`opendalfs`.

## Downstream integrations

The integration suite exercises public entry points from data libraries rather
than calling adapter internals. See {doc}`../integrations/index` for the current
matrix.

Each result is scoped to:

- the dependency version resolved by `uv.lock`
- the entry form used by the test
- the memory, local filesystem, or MinIO-backed S3 fixture selected by CI

Other service and provider combinations may behave differently, particularly
for optional operations such as copy, rename, and multipart upload.

## Python versions

The unit suite runs on Python 3.12, 3.13, and 3.14. Some downstream projects
have narrower Python support, so their integration dependency groups may skip
unsupported interpreter combinations.
