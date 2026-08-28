# Working with files

The examples below use an in-memory filesystem:

```python
from opendalfs import OpendalFileSystem

fs = OpendalFileSystem("memory")
```

The methods come from fsspec. They work with other OpenDAL services when the
service supports the required storage operation.

## Read and write

```python
fs.pipe_file("data/example.txt", b"hello\n")
assert fs.cat_file("data/example.txt") == b"hello\n"

with fs.open("data/example.txt", "rb") as stream:
    assert stream.read(5) == b"hello"
```

## Inspect paths

```python
assert fs.exists("data/example.txt")
assert fs.isfile("data/example.txt")

info = fs.info("data/example.txt")
print(info["name"], info["size"], info["type"])
```

## List and find

```python
fs.pipe_file("data/nested/part-0", b"0")
fs.pipe_file("data/nested/part-1", b"1")

print(fs.ls("data", detail=False))
print(fs.glob("data/**/*.txt"))
print(fs.find("data"))
```

## Transfer files

```python
fs.copy("data/example.txt", "data/copy.txt")
fs.mv("data/copy.txt", "archive/example.txt")

fs.get("archive/example.txt", "example.txt")
fs.put("example.txt", "uploads/example.txt")
```

`get` and `put` cross the local filesystem boundary. `copy` and `mv` operate
inside one OpenDAL-backed filesystem.

## Use a mapping interface

Zarr and Xarray accept mutable mappings:

```python
mapper = fs.get_mapper("arrays/example.zarr")
mapper["metadata.json"] = b"{}"
assert mapper["metadata.json"] == b"{}"
```

See the {doc}`../integrations/index` section for library-specific entry points.

## Use the async implementation

`OpendalFileSystem` subclasses {class}`fsspec.asyn.AsyncFileSystem`. Code that
already runs an event loop can call the fsspec coroutine implementations:

```python
import asyncio

from opendalfs import OpendalFileSystem


async def main():
    async_fs = OpendalFileSystem("memory", asynchronous=True)
    await async_fs._pipe_file("result.bin", b"result")
    assert await async_fs._cat_file("result.bin") == b"result"


asyncio.run(main())
```

The underscore-prefixed methods follow the fsspec `AsyncFileSystem` contract.
Libraries that consume fsspec filesystem objects usually select the sync or
async path themselves.

## Control caching

fsspec can cache directory listings and filesystem instances:

```python
fs = OpendalFileSystem(
    "memory",
    use_listings_cache=True,
    listings_expiry_time=30,
    max_paths=1000,
)
```

Call `fs.invalidate_cache("data")` after an out-of-band change. Pass
`skip_instance_cache=True` when each construction needs an isolated filesystem,
such as in a test.
