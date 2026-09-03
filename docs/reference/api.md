# API reference

The public API contains the generic filesystem and the opt-in S3 adapter.
Inherited filesystem operations follow the
{class}`fsspec.spec.AbstractFileSystem` and
{class}`fsspec.asyn.AsyncFileSystem` contracts.

## Filesystem

```{eval-rst}
.. autoclass:: opendalfs.OpendalFileSystem
   :members:
   :show-inheritance:

.. autoclass:: opendalfs.S3FileSystem
```
