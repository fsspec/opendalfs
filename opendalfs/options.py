from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def build_write_options(
    *,
    defaults: Mapping[str, Any] | None = None,
    write_options: Mapping[str, Any] | None = None,
    write_chunk: Any | None = None,
    write_concurrent: Any | None = None,
) -> dict[str, Any]:
    opts: dict[str, Any] = {}
    if defaults:
        opts.update(defaults)
    if write_options is not None:
        if not isinstance(write_options, Mapping):
            raise TypeError("opendal_write_options must be a mapping")
        opts.update(write_options)
    if write_chunk is not None:
        opts["chunk"] = write_chunk
    if write_concurrent is not None:
        opts["concurrent"] = write_concurrent
    return opts


def pop_write_options(
    kwargs: dict[str, Any], *, defaults: Mapping[str, Any] | None = None
) -> dict[str, Any]:
    if "write_chunk" in kwargs or "write_concurrent" in kwargs:
        raise TypeError(
            "Use opendal_write_chunk/opendal_write_concurrent instead of "
            "write_chunk/write_concurrent."
        )
    write_options = kwargs.pop("opendal_write_options", None)
    write_chunk = kwargs.pop("opendal_write_chunk", None)
    write_concurrent = kwargs.pop("opendal_write_concurrent", None)
    return build_write_options(
        defaults=defaults,
        write_options=write_options,
        write_chunk=write_chunk,
        write_concurrent=write_concurrent,
    )
