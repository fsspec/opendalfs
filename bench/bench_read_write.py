from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import fsspec
import pyarrow.fs as pafs


class _OpenOptionsFS:
    def __init__(
        self,
        fs,
        *,
        block_size: int | None,
        cache_type: str | None = None,
        cache_options: dict | None = None,
    ) -> None:
        self._fs = fs
        self._block_size = block_size
        self._cache_type = cache_type
        self._cache_options = cache_options

    @property
    def protocol(self):
        return self._fs.protocol

    def open(self, path, mode="rb", **kwargs):
        if self._block_size is not None and "block_size" not in kwargs:
            kwargs["block_size"] = self._block_size
        if self._cache_type is not None and "cache_type" not in kwargs:
            kwargs["cache_type"] = self._cache_type
        if self._cache_options is not None and "cache_options" not in kwargs:
            kwargs["cache_options"] = self._cache_options
        return self._fs.open(path, mode=mode, **kwargs)

    def info(self, *args, **kwargs):
        return self._fs.info(*args, **kwargs)

    def exists(self, *args, **kwargs):
        return self._fs.exists(*args, **kwargs)

    def isdir(self, *args, **kwargs):
        return self._fs.isdir(*args, **kwargs)

    def isfile(self, *args, **kwargs):
        return self._fs.isfile(*args, **kwargs)

    def find(self, *args, **kwargs):
        return self._fs.find(*args, **kwargs)

    def mkdir(self, *args, **kwargs):
        return self._fs.mkdir(*args, **kwargs)

    def rm(self, *args, **kwargs):
        return self._fs.rm(*args, **kwargs)

    def listdir(self, *args, **kwargs):
        return self._fs.listdir(*args, **kwargs)

    def mv(self, *args, **kwargs):
        return self._fs.mv(*args, **kwargs)

    def copy(self, *args, **kwargs):
        return self._fs.copy(*args, **kwargs)


def _env_first(*names: str) -> str | None:
    for name in names:
        value = os.environ.get(name)
        if value:
            return value
    return None


def _parse_sizes(value: str) -> list[int]:
    sizes: list[int] = []
    for raw in value.split(","):
        item = raw.strip()
        if not item:
            continue
        sizes.append(int(item))
    if not sizes:
        raise ValueError("sizes must contain at least one entry")
    return sizes


def _parse_cache_options(value: str | None) -> dict | None:
    if value is None:
        return None
    payload = json.loads(value)
    if not isinstance(payload, dict):
        raise ValueError("fsspec-cache-options must be a JSON object")
    return payload


def _resolve_mapped_args(args) -> None:
    """Map baseline knobs into backend-specific options unless overridden."""
    if args.fsspec_block_size is None:
        args.fsspec_block_size = args.block_size
    if args.fsspec_cache_type is None:
        args.fsspec_cache_type = args.cache_type
    if args.fsspec_cache_options is None:
        args.fsspec_cache_options = args.cache_options

    if args.write_chunk is None:
        args.write_chunk = args.io_chunk
    if args.s3fs_block_size is None:
        args.s3fs_block_size = args.io_chunk

    if args.write_concurrent is None:
        args.write_concurrent = args.io_concurrency
    if args.s3fs_max_concurrency is None:
        args.s3fs_max_concurrency = args.io_concurrency

    if args.s3fs_cache_type is None:
        args.s3fs_cache_type = args.cache_type


def _validate_args(args) -> None:
    if any(size_mb <= 0 for size_mb in args.sizes):
        raise SystemExit("--sizes entries must all be positive integers")
    if args.files <= 0:
        raise SystemExit("--files must be a positive integer")
    if args.workers <= 0:
        raise SystemExit("--workers must be a positive integer")
    if args.fsspec_workers <= 0:
        raise SystemExit("--fsspec-workers must be a positive integer")
    if args.stream_buffer_size is not None and args.stream_buffer_size < 0:
        raise SystemExit("--stream-buffer-size must be >= 0")
    if args.block_size is not None and args.block_size <= 0:
        raise SystemExit("--block-size must be a positive integer")
    if args.io_chunk is not None and args.io_chunk <= 0:
        raise SystemExit("--io-chunk must be a positive integer")
    if args.io_concurrency is not None and args.io_concurrency <= 0:
        raise SystemExit("--io-concurrency must be a positive integer")
    if args.fsspec_block_size is not None and args.fsspec_block_size <= 0:
        raise SystemExit("--fsspec-block-size must be a positive integer")
    if args.write_chunk is not None and args.write_chunk <= 0:
        raise SystemExit("--write-chunk must be a positive integer")
    if args.write_concurrent is not None and args.write_concurrent <= 0:
        raise SystemExit("--write-concurrent must be a positive integer")
    if args.s3fs_block_size is not None and args.s3fs_block_size <= 0:
        raise SystemExit("--s3fs-block-size must be a positive integer")
    if args.s3fs_max_concurrency is not None and args.s3fs_max_concurrency <= 0:
        raise SystemExit("--s3fs-max-concurrency must be a positive integer")
    if args.rounds <= 0:
        raise SystemExit("--rounds must be a positive integer")
    if args.warmup_rounds < 0:
        raise SystemExit("--warmup-rounds must be >= 0")


def _time_rounds(run_once, rounds: int, warmup_rounds: int) -> list[float]:
    for _ in range(warmup_rounds):
        run_once()
    timings: list[float] = []
    for _ in range(rounds):
        timings.append(run_once())
    return timings


def _round_base(base: str, round_index: int) -> str:
    return f"{base}-r{round_index}"


def _required_labels(args) -> list[str]:
    labels = ["arrow-direct", "arrow-fsspec-opendalfs"]
    if not args.skip_s3fs:
        labels.append("arrow-fsspec-s3")
    return labels


def _validate_manifest(manifest: dict, args) -> None:
    required_labels = _required_labels(args)
    for size_mb in args.sizes:
        key = str(size_mb)
        entry = manifest.get(key)
        if not isinstance(entry, dict):
            raise SystemExit(f"manifest missing size entry: {size_mb}")
        missing = [label for label in required_labels if label not in entry]
        if missing:
            present = ", ".join(sorted(entry.keys()))
            missing_s = ", ".join(missing)
            raise SystemExit(
                f"manifest missing labels for size {size_mb}: {missing_s}; present: {present}",
            )


def _load_config(args) -> dict[str, str]:
    bucket = (
        args.bucket or _env_first("OPENDAL_S3_BUCKET", "AWS_S3_BUCKET") or "opendal"
    )
    region = (
        args.region
        or _env_first("OPENDAL_S3_REGION", "AWS_REGION", "AWS_DEFAULT_REGION")
        or "us-east-1"
    )
    endpoint = (
        args.endpoint
        or _env_first("OPENDAL_S3_ENDPOINT", "AWS_ENDPOINT")
        or "http://127.0.0.1:9000"
    )
    access_key_id = (
        args.access_key_id
        or _env_first("OPENDAL_S3_ACCESS_KEY_ID", "AWS_ACCESS_KEY_ID")
        or "minioadmin"
    )
    secret_access_key = (
        args.secret_access_key
        or _env_first("OPENDAL_S3_SECRET_ACCESS_KEY", "AWS_SECRET_ACCESS_KEY")
        or "minioadmin"
    )

    return {
        "bucket": bucket,
        "region": region,
        "endpoint": endpoint,
        "access_key_id": access_key_id,
        "secret_access_key": secret_access_key,
    }


def _run_write(
    fs: pafs.FileSystem,
    base: str,
    size_mb: int,
    files: int,
    workers: int,
    stream_buffer_size: int | None,
) -> float:
    payload = b"x" * (size_mb * 1024 * 1024)
    paths = [f"{base}/file-{i}.bin" for i in range(files)]

    def write_one(path: str) -> None:
        with fs.open_output_stream(path, buffer_size=stream_buffer_size) as writer:
            writer.write(payload)

    start = time.perf_counter()
    if workers > 1:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            list(pool.map(write_one, paths))
    else:
        for path in paths:
            write_one(path)
    return time.perf_counter() - start


def _run_read(
    fs: pafs.FileSystem,
    base: str,
    size_mb: int,
    files: int,
    workers: int,
    stream_buffer_size: int | None,
) -> float:
    payload = b"x" * (size_mb * 1024 * 1024)
    paths = [f"{base}/file-{i}.bin" for i in range(files)]

    def read_one(path: str) -> None:
        with fs.open_input_stream(path, buffer_size=stream_buffer_size) as reader:
            data = reader.read()
        if data != payload:
            raise RuntimeError(f"data mismatch for {path}")

    start = time.perf_counter()
    if workers > 1:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            list(pool.map(read_one, paths))
    else:
        for path in paths:
            read_one(path)
    return time.perf_counter() - start


def _report(
    label: str, size_mb: int, files: int, phase: str, timings: list[float]
) -> None:
    total_mb = size_mb * files
    mean_seconds = statistics.fmean(timings)
    stdev_seconds = statistics.pstdev(timings) if len(timings) > 1 else 0.0
    mbps = total_mb / mean_seconds if mean_seconds else 0.0
    print(f"[{label}] size {size_mb}MB x {files}")
    if len(timings) == 1:
        seconds = timings[0]
        print(f"[{label}] {phase} {total_mb}MB in {seconds:.3f}s ({mbps:.1f} MB/s)")
        return
    rounds = ", ".join(f"{t:.3f}s" for t in timings)
    print(
        f"[{label}] {phase} mean={mean_seconds:.3f}s stdev={stdev_seconds:.3f}s "
        f"({mbps:.1f} MB/s)",
    )
    print(f"[{label}] {phase} rounds: {rounds}")


def _arrow_direct_fs(
    config: dict[str, str],
    *,
    allow_bucket_creation: bool,
    background_writes: bool,
) -> pafs.FileSystem:
    return pafs.S3FileSystem(
        access_key=config["access_key_id"],
        secret_key=config["secret_access_key"],
        region=config["region"],
        endpoint_override=config["endpoint"],
        allow_bucket_creation=allow_bucket_creation,
        background_writes=background_writes,
    )


def _arrow_fsspec_opendalfs_fs(config: dict[str, str], args) -> pafs.FileSystem:
    import opendalfs

    write_options = opendalfs.WriteOptions(
        chunk=args.write_chunk,
        concurrent=args.write_concurrent,
    )
    backend = opendalfs.OpendalFileSystem(
        "s3",
        bucket=config["bucket"],
        region=config["region"],
        endpoint=config["endpoint"],
        access_key_id=config["access_key_id"],
        secret_access_key=config["secret_access_key"],
        write_options=write_options,
    )
    handler_fs = _OpenOptionsFS(
        backend,
        block_size=args.fsspec_block_size,
        cache_type=args.fsspec_cache_type,
        cache_options=args.fsspec_cache_options,
    )
    return pafs.PyFileSystem(pafs.FSSpecHandler(handler_fs))


def _arrow_fsspec_s3_fs(config: dict[str, str], args) -> pafs.FileSystem | None:
    try:
        import s3fs  # noqa: F401
    except Exception:
        print("s3fs not installed, skip arrow-fsspec-s3", file=sys.stderr)
        return None

    backend = fsspec.filesystem(
        "s3",
        key=config["access_key_id"],
        secret=config["secret_access_key"],
        client_kwargs={
            "endpoint_url": config["endpoint"],
            "region_name": config["region"],
        },
        config_kwargs={"s3": {"addressing_style": "path"}},
        default_block_size=args.s3fs_block_size,
        default_cache_type=args.s3fs_cache_type,
        default_fill_cache=args.s3fs_fill_cache,
        max_concurrency=args.s3fs_max_concurrency,
    )
    handler_fs = _OpenOptionsFS(
        backend,
        block_size=args.fsspec_block_size,
        cache_type=args.fsspec_cache_type,
        cache_options=args.fsspec_cache_options,
    )
    return pafs.PyFileSystem(pafs.FSSpecHandler(handler_fs))


def _default_base(config: dict[str, str], args, size_mb: int, label: str) -> str:
    suffix = f"{args.prefix}-{size_mb}mb-{uuid4()}"
    if label in ("arrow-direct", "arrow-fsspec-s3"):
        return f"{config['bucket']}/{suffix}"
    return suffix


def _get_manifest_base(manifest: dict, size_mb: int, label: str) -> str:
    try:
        return manifest[str(size_mb)][label]
    except KeyError as exc:
        raise SystemExit(
            f"missing manifest entry for size {size_mb} and {label}"
        ) from exc


def _record_manifest_base(manifest: dict, size_mb: int, label: str, base: str) -> None:
    manifest.setdefault(str(size_mb), {})[label] = base


def _run_write_backends(config: dict[str, str], args, manifest: dict) -> None:
    fs_arrow = _arrow_direct_fs(
        config,
        allow_bucket_creation=True,
        background_writes=args.arrow_background_writes,
    )
    fs_opendal = _arrow_fsspec_opendalfs_fs(config, args)
    fs_s3 = None if args.skip_s3fs else _arrow_fsspec_s3_fs(config, args)

    for size_mb in args.sizes:
        base_root = _default_base(config, args, size_mb, "arrow-direct")
        round_index = 0

        def arrow_direct_once() -> float:
            nonlocal round_index
            base = _round_base(base_root, round_index)
            round_index += 1
            elapsed = _run_write(
                fs_arrow,
                base,
                size_mb,
                args.files,
                args.workers,
                args.stream_buffer_size,
            )
            _record_manifest_base(manifest, size_mb, "arrow-direct", base)
            return elapsed

        timings = _time_rounds(arrow_direct_once, args.rounds, args.warmup_rounds)
        _report("arrow-direct", size_mb, args.files, "write", timings)

        base_root = _default_base(config, args, size_mb, "arrow-fsspec-opendalfs")
        round_index = 0

        def opendal_once() -> float:
            nonlocal round_index
            base = _round_base(base_root, round_index)
            round_index += 1
            elapsed = _run_write(
                fs_opendal,
                base,
                size_mb,
                args.files,
                args.fsspec_workers,
                args.stream_buffer_size,
            )
            _record_manifest_base(manifest, size_mb, "arrow-fsspec-opendalfs", base)
            return elapsed

        timings = _time_rounds(opendal_once, args.rounds, args.warmup_rounds)
        _report("arrow-fsspec-opendalfs", size_mb, args.files, "write", timings)

        if fs_s3 is not None:
            base_root = _default_base(config, args, size_mb, "arrow-fsspec-s3")
            round_index = 0

            def s3_once() -> float:
                nonlocal round_index
                base = _round_base(base_root, round_index)
                round_index += 1
                elapsed = _run_write(
                    fs_s3,
                    base,
                    size_mb,
                    args.files,
                    args.fsspec_workers,
                    args.stream_buffer_size,
                )
                _record_manifest_base(manifest, size_mb, "arrow-fsspec-s3", base)
                return elapsed

            timings = _time_rounds(s3_once, args.rounds, args.warmup_rounds)
            _report("arrow-fsspec-s3", size_mb, args.files, "write", timings)


def _run_read_backends(config: dict[str, str], args, manifest: dict) -> None:
    fs_arrow = _arrow_direct_fs(
        config,
        allow_bucket_creation=False,
        background_writes=args.arrow_background_writes,
    )
    fs_opendal = _arrow_fsspec_opendalfs_fs(config, args)
    fs_s3 = None if args.skip_s3fs else _arrow_fsspec_s3_fs(config, args)

    for size_mb in args.sizes:
        base = _get_manifest_base(manifest, size_mb, "arrow-direct")
        timings = _time_rounds(
            lambda: _run_read(
                fs_arrow,
                base,
                size_mb,
                args.files,
                args.workers,
                args.stream_buffer_size,
            ),
            args.rounds,
            args.warmup_rounds,
        )
        _report("arrow-direct", size_mb, args.files, "read", timings)

        base = _get_manifest_base(manifest, size_mb, "arrow-fsspec-opendalfs")
        timings = _time_rounds(
            lambda: _run_read(
                fs_opendal,
                base,
                size_mb,
                args.files,
                args.fsspec_workers,
                args.stream_buffer_size,
            ),
            args.rounds,
            args.warmup_rounds,
        )
        _report("arrow-fsspec-opendalfs", size_mb, args.files, "read", timings)

        if fs_s3 is not None:
            base = _get_manifest_base(manifest, size_mb, "arrow-fsspec-s3")
            timings = _time_rounds(
                lambda: _run_read(
                    fs_s3,
                    base,
                    size_mb,
                    args.files,
                    args.fsspec_workers,
                    args.stream_buffer_size,
                ),
                args.rounds,
                args.warmup_rounds,
            )
            _report("arrow-fsspec-s3", size_mb, args.files, "read", timings)


def _add_common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--sizes",
        type=_parse_sizes,
        default="16,32,64",
        help="Comma-separated sizes in MB (default: 16,32,64)",
    )
    parser.add_argument("--files", type=int, default=4)
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Workers for arrow-direct tests",
    )
    parser.add_argument(
        "--fsspec-workers",
        type=int,
        default=None,
        help="Override workers for fsspec-based tests (default: same as --workers)",
    )
    parser.add_argument(
        "--rounds",
        type=int,
        default=1,
        help="Timed rounds per size/backend (default: 1)",
    )
    parser.add_argument(
        "--warmup-rounds",
        type=int,
        default=0,
        help="Warmup rounds per size/backend (default: 0)",
    )
    parser.add_argument(
        "--stream-buffer-size",
        type=int,
        default=0,
        help="Buffer size for Arrow input/output streams (default: 0)",
    )
    parser.add_argument(
        "--block-size",
        type=int,
        default=None,
        help="Baseline fsspec open() block_size applied to both fsspec backends",
    )
    parser.add_argument(
        "--io-chunk",
        type=int,
        default=8 * 1024 * 1024,
        help="Baseline multipart chunk size in bytes for opendalfs and s3fs",
    )
    parser.add_argument(
        "--io-concurrency",
        type=int,
        default=4,
        help="Baseline per-file concurrency for opendalfs and s3fs",
    )
    parser.add_argument(
        "--cache-type",
        default="none",
        help="Baseline cache type for fsspec/s3fs open() (default: none)",
    )
    parser.add_argument(
        "--cache-options",
        type=_parse_cache_options,
        default=None,
        help="Baseline cache options as JSON object",
    )
    parser.add_argument(
        "--fsspec-block-size",
        type=int,
        default=None,
        help="Override fsspec open() block_size",
    )
    parser.add_argument(
        "--fsspec-cache-type",
        default=None,
        help="Override fsspec open() cache_type",
    )
    parser.add_argument(
        "--fsspec-cache-options",
        type=_parse_cache_options,
        default=None,
        help="Override fsspec cache options as JSON object",
    )
    parser.add_argument("--bucket")
    parser.add_argument("--region")
    parser.add_argument("--endpoint")
    parser.add_argument("--access-key-id")
    parser.add_argument("--secret-access-key")
    parser.add_argument(
        "--write-chunk",
        type=int,
        default=None,
        help="Override OpenDAL write chunk size in bytes",
    )
    parser.add_argument(
        "--write-concurrent",
        type=int,
        default=None,
        help="Override OpenDAL write concurrent setting",
    )
    parser.add_argument(
        "--s3fs-block-size",
        type=int,
        default=None,
        help="Override s3fs default block size in bytes",
    )
    parser.add_argument(
        "--s3fs-cache-type",
        default=None,
        help="Override s3fs default cache type",
    )
    parser.add_argument(
        "--s3fs-fill-cache",
        action="store_true",
        help="Enable s3fs fill_cache (default: disabled)",
    )
    parser.add_argument(
        "--s3fs-max-concurrency",
        type=int,
        default=None,
        help="Override s3fs max_concurrency for multipart uploads",
    )
    parser.add_argument(
        "--arrow-background-writes",
        action="store_true",
        help="Enable Arrow S3 background writes (default: disabled)",
    )
    parser.add_argument(
        "--skip-s3fs",
        action="store_true",
        help="Skip fsspec+s3fs comparison",
    )


def _add_write_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--prefix", default="opendalfs-repro")
    parser.add_argument(
        "--manifest",
        default="/tmp/opendalfs_bench_manifest.json",
        help="Path to write manifest for read runs",
    )


def _add_read_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--manifest",
        default="/tmp/opendalfs_bench_manifest.json",
        help="Path to manifest produced by write runs",
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compare Arrow direct vs opendalfs(fsspec) on MinIO.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    write_parser = subparsers.add_parser("write", help="Run write benchmark")
    _add_common_args(write_parser)
    _add_write_args(write_parser)

    read_parser = subparsers.add_parser("read", help="Run read benchmark")
    _add_common_args(read_parser)
    _add_read_args(read_parser)

    args = parser.parse_args()

    if args.fsspec_workers is None:
        args.fsspec_workers = args.workers

    _resolve_mapped_args(args)
    _validate_args(args)
    config = _load_config(args)

    if args.command == "write":
        manifest: dict[str, object] = {}
        _run_write_backends(config, args, manifest)
        with open(args.manifest, "w", encoding="utf-8") as fh:
            json.dump(manifest, fh, indent=2, sort_keys=True)
        return

    with open(args.manifest, "r", encoding="utf-8") as fh:
        manifest = json.load(fh)
    _validate_manifest(manifest, args)
    _run_read_backends(config, args, manifest)


if __name__ == "__main__":
    main()
