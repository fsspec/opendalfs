# API reference

The public API contains the filesystem class and runtime registration helpers.
Inherited filesystem operations follow the
{class}`fsspec.spec.AbstractFileSystem` and
{class}`fsspec.asyn.AsyncFileSystem` contracts.

## Filesystem

```{eval-rst}
.. autoclass:: opendalfs.OpendalFileSystem
   :members:
   :show-inheritance:
```

## Protocol registration

```{eval-rst}
.. autofunction:: opendalfs.register_opendal_service

.. autofunction:: opendalfs.register_opendal_protocols
```
