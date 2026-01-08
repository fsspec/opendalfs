from __future__ import annotations

import argparse
import os
import time
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import fsspec
import pyarrow.fs as pafs

import opendal


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


def _load_config(args) -> dict[str, str]:
    bucket = args.bucket or _env_first("OPENDAL_S3_BUCKET", "AWS_S3_BUCKET") or "opendal"
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


def _ensure_bucket(config: dict[str, str]) -> None:
    try:
        import boto3
        from botocore.exceptions import ClientError
    except Exception:
        print("boto3 not installed, skip bucket creation")
        return

    client = boto3.client(
        "s3",
        endpoint_url=config["endpoint"],
        region_name=config["region"],
        aws_access_key_id=config["access_key_id"],
        aws_secret_access_key=config["secret_access_key"],
    )
    try:
        client.head_bucket(Bucket=config["bucket"])
    except ClientError:
        client.create_bucket(Bucket=config["bucket"])


def _run_benchmark(
    fs: pafs.FileSystem,
    base: str,
    size_mb: int,
    files: int,
    workers: int,
) -> tuple[float, float]:
    payload = b"x" * (size_mb * 1024 * 1024)
    paths = [f"{base}/file-{i}.bin" for i in range(files)]

    def write_one(path: str) -> None:
        with fs.open_output_stream(path) as writer:
            writer.write(payload)

    def read_one(path: str) -> None:
        with fs.open_input_stream(path) as reader:
            data = reader.read()
        if data != payload:
            raise RuntimeError(f"data mismatch for {path}")

    start = time.perf_counter()
    if workers > 1:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            list(pool.map(write_one, paths))
    else:
        for path in paths:
            write_one(path)
    write_s = time.perf_counter() - start

    start = time.perf_counter()
    if workers > 1:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            list(pool.map(read_one, paths))
    else:
        for path in paths:
            read_one(path)
    read_s = time.perf_counter() - start

    return write_s, read_s


def _report(label: str, size_mb: int, files: int, write_s: float, read_s: float) -> None:
    total_mb = size_mb * files
    write_mbps = total_mb / write_s if write_s else 0.0
    read_mbps = total_mb / read_s if read_s else 0.0
    print(f"[{label}] size {size_mb}MB x {files}")
    print(f"[{label}] write {total_mb}MB in {write_s:.3f}s ({write_mbps:.1f} MB/s)")
    print(f"[{label}] read  {total_mb}MB in {read_s:.3f}s ({read_mbps:.1f} MB/s)")


def _run_arrow_direct(config: dict[str, str], args, size_mb: int) -> None:
    fs = pafs.S3FileSystem(
        access_key=config["access_key_id"],
        secret_key=config["secret_access_key"],
        region=config["region"],
        endpoint_override=config["endpoint"],
        allow_bucket_creation=True,
    )
    base = f'{config["bucket"]}/{args.prefix}-{size_mb}mb-{uuid4()}'
    write_s, read_s = _run_benchmark(fs, base, size_mb, args.files, args.workers)
    _report("arrow-direct", size_mb, args.files, write_s, read_s)


def _ensure_opendal_file_types() -> None:
    if hasattr(opendal, "AsyncFile") and hasattr(opendal, "File"):
        return
    try:
        import opendal.file as opendal_file
    except Exception:
        class _ShimFile:
            pass

        opendal.AsyncFile = _ShimFile
        opendal.File = _ShimFile
        return

    if not hasattr(opendal, "AsyncFile"):
        opendal.AsyncFile = opendal_file.AsyncFile
    if not hasattr(opendal, "File"):
        opendal.File = opendal_file.File


def _run_arrow_fsspec_opendalfs(config: dict[str, str], args, size_mb: int) -> None:
    _ensure_opendal_file_types()

    if args.opendalfs_path:
        import sys

        sys.path.insert(0, args.opendalfs_path)

    import opendalfs

    opendalfs.register_opendal_protocols(["s3"])
    backend = opendalfs.OpendalFileSystem(
        "s3",
        bucket=config["bucket"],
        region=config["region"],
        endpoint=config["endpoint"],
        access_key_id=config["access_key_id"],
        secret_access_key=config["secret_access_key"],
        opendal_write_chunk=args.opendal_write_chunk,
        opendal_write_concurrent=args.opendal_write_concurrent,
    )
    fs = pafs.PyFileSystem(pafs.FSSpecHandler(backend))
    base = f'{args.prefix}-{size_mb}mb-{uuid4()}'
    write_s, read_s = _run_benchmark(
        fs,
        base,
        size_mb,
        args.files,
        args.fsspec_workers,
    )
    _report("arrow-fsspec-opendalfs", size_mb, args.files, write_s, read_s)


def _run_arrow_fsspec_s3(config: dict[str, str], args, size_mb: int) -> None:
    try:
        import s3fs  # noqa: F401
    except Exception:
        print("s3fs not installed, skip arrow-fsspec-s3")
        return

    backend = fsspec.filesystem(
        "s3",
        key=config["access_key_id"],
        secret=config["secret_access_key"],
        client_kwargs={
            "endpoint_url": config["endpoint"],
            "region_name": config["region"],
        },
        config_kwargs={"s3": {"addressing_style": "path"}},
    )
    fs = pafs.PyFileSystem(pafs.FSSpecHandler(backend))
    base = f'{config["bucket"]}/{args.prefix}-{size_mb}mb-{uuid4()}'
    write_s, read_s = _run_benchmark(
        fs,
        base,
        size_mb,
        args.files,
        args.fsspec_workers,
    )
    _report("arrow-fsspec-s3", size_mb, args.files, write_s, read_s)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compare Arrow direct vs opendalfs(fsspec) on MinIO."
    )
    parser.add_argument(
        "--sizes",
        type=_parse_sizes,
        default="16,32,64",
        help="Comma-separated sizes in MB (default: 16,32,64)",
    )
    parser.add_argument("--files", type=int, default=4)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument(
        "--fsspec-workers",
        type=int,
        default=1,
        help="Override workers for fsspec-based tests",
    )
    parser.add_argument("--prefix", default="opendalfs-repro")
    parser.add_argument("--bucket")
    parser.add_argument("--region")
    parser.add_argument("--endpoint")
    parser.add_argument("--access-key-id")
    parser.add_argument("--secret-access-key")
    parser.add_argument(
        "--opendalfs-path",
        help="Optional local path to opendalfs repo (adds to sys.path)",
    )
    parser.add_argument(
        "--opendal-write-chunk",
        type=int,
        default=8 * 1024 * 1024,
    )
    parser.add_argument(
        "--opendal-write-concurrent",
        type=int,
        default=4,
    )
    parser.add_argument(
        "--skip-s3fs",
        action="store_true",
        help="Skip fsspec+s3fs comparison",
    )
    args = parser.parse_args()

    config = _load_config(args)
    _ensure_bucket(config)

    for size_mb in args.sizes:
        _run_arrow_direct(config, args, size_mb)
        _run_arrow_fsspec_opendalfs(config, args, size_mb)
        if not args.skip_s3fs:
            _run_arrow_fsspec_s3(config, args, size_mb)


if __name__ == "__main__":
    main()
