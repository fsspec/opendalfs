# Capabilities

Compatibility has more than one layer. A method can exist in fsspec while the
selected storage service lacks the native operation needed to implement it.

```text
OpenDAL service capability
  -> OpenDAL Python binding
  -> opendalfs method
  -> downstream library usage
```

## fsspec methods

fsspec defines common filesystem methods such as `open`, `ls`, `find`, `copy`,
and `rm`. It also implements composite methods in terms of smaller operations.
For example, a recursive operation may combine listing, directory creation,
and individual file transfers.

`opendalfs` implements the primitive operations needed by those fsspec flows
and runs the upstream abstract filesystem contract tests against memory and S3.

## Service operations

OpenDAL services do not all support the same operations. Read, write, list,
copy, rename, multipart upload, and metadata behavior can differ. OpenDAL
reports unsupported native operations as errors.

Consult the [OpenDAL service directory](https://opendal.apache.org/services/)
when choosing a backend. Test optional operations against the configured
service before using them in a production workflow.

## Integration evidence

A passing pandas test proves that the tested pandas entry point works with the
locked dependency version and the backends covered by CI. It does not prove
every pandas storage workflow on every OpenDAL service.

The {doc}`../integrations/index` labels this evidence as "CI-tested" and names
the entry form used by each library. This keeps the claim tied to a concrete
test instead of treating compatibility as all or nothing.

## Where to report a mismatch

If a valid fsspec usage fails before reaching an unsupported OpenDAL operation,
report it to `opendalfs`. Include the service, URL or filesystem construction,
operation, and a small reproducer. Backend-specific failures may also require
an OpenDAL issue.
