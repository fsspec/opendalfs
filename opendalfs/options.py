from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class WriteOptions:
    chunk: int | None = None
    concurrent: int | None = None
    extra: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.extra, Mapping):
            raise TypeError("WriteOptions.extra must be a mapping")

    def to_opendal_kwargs(self) -> dict[str, Any]:
        opts: dict[str, Any] = dict(self.extra)
        if self.chunk is not None:
            opts["chunk"] = self.chunk
        if self.concurrent is not None:
            opts["concurrent"] = self.concurrent
        return opts

    def merge(self, override: "WriteOptions | None") -> "WriteOptions":
        if override is None:
            return self
        extra = dict(self.extra)
        extra.update(override.extra)
        chunk = override.chunk if override.chunk is not None else self.chunk
        concurrent = (
            override.concurrent
            if override.concurrent is not None
            else self.concurrent
        )
        return WriteOptions(chunk=chunk, concurrent=concurrent, extra=extra)


def _ensure_write_options(value: WriteOptions | None) -> WriteOptions | None:
    if value is None:
        return None
    if not isinstance(value, WriteOptions):
        raise TypeError("write_options must be WriteOptions")
    return value


def pop_write_options(
    kwargs: dict[str, Any],
    *,
    defaults: WriteOptions | None = None,
) -> dict[str, Any]:
    override = _ensure_write_options(kwargs.pop("write_options", None))
    if defaults is None:
        merged = override or WriteOptions()
    else:
        merged = defaults.merge(override)
    return merged.to_opendal_kwargs()
