from __future__ import annotations

import argparse
import os
import statistics
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from uuid import uuid4

import boto3
import fsspec
import pyarrow.fs as pafs
from botocore.exceptions import ClientError

MIB = 1024 * 1024


@dataclass(frozen=True)
class BenchmarkProfile:
    chunk_size: int | None
    write_concurrent: int | None
    arrow_background_writes: bool | None


@dataclass(frozen=True)
class Backend:
    label: str
    filesystem: pafs.FileSystem
    bucket: str | None
    settings: tuple[str, ...]

    def path(self, key: str) -> str:
        if self.bucket is None:
            return key
        return f"{self.bucket}/{key}"


PROFILES = {
    "defaults": BenchmarkProfile(
        chunk_size=None,
        write_concurrent=None,
        arrow_background_writes=None,
    ),
    "controlled": BenchmarkProfile(
        chunk_size=8 * MIB,
        write_concurrent=1,
        arrow_background_writes=False,
    ),
}


def _env_first(*names: str) -> str | None:
    for name in names:
        value = os.environ.get(name)
        if value:
            return value
    return None


def _parse_sizes(value: str) -> list[int]:
    sizes = [int(raw.strip()) for raw in value.split(",") if raw.strip()]
    if not sizes or any(size <= 0 for size in sizes):
        raise argparse.ArgumentTypeError("sizes must be positive integers")
    return sizes


def _load_config(args: argparse.Namespace) -> dict[str, str]:
    return {
        "bucket": args.bucket
        or _env_first("OPENDAL_S3_BUCKET", "AWS_S3_BUCKET")
        or "test-bucket",
        "region": args.region
        or _env_first("OPENDAL_S3_REGION", "AWS_REGION", "AWS_DEFAULT_REGION")
        or "us-east-1",
        "endpoint": args.endpoint
        or _env_first("OPENDAL_S3_ENDPOINT", "AWS_ENDPOINT")
        or "http://127.0.0.1:9000",
        "access_key_id": args.access_key_id
        or _env_first("OPENDAL_S3_ACCESS_KEY_ID", "AWS_ACCESS_KEY_ID")
        or "minioadmin",
        "secret_access_key": args.secret_access_key
        or _env_first("OPENDAL_S3_SECRET_ACCESS_KEY", "AWS_SECRET_ACCESS_KEY")
        or "minioadmin",
    }


def _s3_client(config: dict[str, str]):
    return boto3.client(
        "s3",
        endpoint_url=config["endpoint"],
        region_name=config["region"],
        aws_access_key_id=config["access_key_id"],
        aws_secret_access_key=config["secret_access_key"],
    )


def _ensure_bucket(client, bucket: str) -> None:
    try:
        client.head_bucket(Bucket=bucket)
    except ClientError as error:
        if error.response.get("Error", {}).get("Code") not in {"404", "NoSuchBucket"}:
            raise
        client.create_bucket(Bucket=bucket)


def _delete_prefix(client, bucket: str, prefix: str) -> None:
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        objects = [{"Key": item["Key"]} for item in page.get("Contents", [])]
        if objects:
            client.delete_objects(Bucket=bucket, Delete={"Objects": objects})


def _run_benchmark(
    filesystem: pafs.FileSystem,
    base: str,
    payload: bytes,
    files: int,
    workers: int,
) -> tuple[float, float]:
    paths = [f"{base}/file-{index}.bin" for index in range(files)]

    def write_one(path: str) -> None:
        with filesystem.open_output_stream(path) as writer:
            writer.write(payload)

    def read_one(path: str) -> None:
        with filesystem.open_input_stream(path) as reader:
            data = reader.read()
        if data != payload:
            raise RuntimeError(f"data mismatch for {path}")

    def run(operation) -> None:
        if workers == 1:
            for path in paths:
                operation(path)
            return
        with ThreadPoolExecutor(max_workers=workers) as pool:
            list(pool.map(operation, paths))

    started = time.perf_counter()
    run(write_one)
    write_seconds = time.perf_counter() - started

    started = time.perf_counter()
    run(read_one)
    read_seconds = time.perf_counter() - started
    return write_seconds, read_seconds


def _report(
    backend: Backend,
    size_mib: int,
    files: int,
    timings: list[tuple[float, float]],
) -> None:
    total_mib = size_mib * files
    write_rates = [total_mib / write for write, _ in timings]
    read_rates = [total_mib / read for _, read in timings]
    write_samples = ", ".join(f"{rate:.1f}" for rate in write_rates)
    read_samples = ", ".join(f"{rate:.1f}" for rate in read_rates)

    print(f"[{backend.label}] write median {statistics.median(write_rates):.1f} MiB/s")
    print(f"[{backend.label}] read  median {statistics.median(read_rates):.1f} MiB/s")
    print(f"[{backend.label}] write samples {write_samples}")
    print(f"[{backend.label}] read  samples {read_samples}")


def _arrow_backend(config: dict[str, str], profile: BenchmarkProfile) -> Backend:
    options = {
        "access_key": config["access_key_id"],
        "secret_key": config["secret_access_key"],
        "region": config["region"],
        "endpoint_override": config["endpoint"],
        "allow_bucket_creation": True,
    }
    if profile.arrow_background_writes is not None:
        options["background_writes"] = profile.arrow_background_writes
    filesystem = pafs.S3FileSystem(**options)
    background = (
        True
        if profile.arrow_background_writes is None
        else profile.arrow_background_writes
    )
    return Backend(
        label="arrow-direct",
        filesystem=filesystem,
        bucket=config["bucket"],
        settings=(f"background_writes={str(background).lower()}",),
    )


def _opendalfs_backend(
    config: dict[str, str],
    profile: BenchmarkProfile,
    opendalfs_path: str | None,
) -> Backend:
    if opendalfs_path:
        import sys

        sys.path.insert(0, opendalfs_path)

    import opendalfs

    if profile.write_concurrent is None:
        backend = opendalfs.OpendalFileSystem(
            "s3",
            bucket=config["bucket"],
            region=config["region"],
            endpoint=config["endpoint"],
            access_key_id=config["access_key_id"],
            secret_access_key=config["secret_access_key"],
        )
    else:
        backend = opendalfs.OpendalFileSystem(
            "s3",
            bucket=config["bucket"],
            region=config["region"],
            endpoint=config["endpoint"],
            access_key_id=config["access_key_id"],
            secret_access_key=config["secret_access_key"],
            write_chunk=profile.chunk_size,
            write_concurrent=profile.write_concurrent,
        )
    filesystem = pafs.PyFileSystem(pafs.FSSpecHandler(backend))
    return Backend(
        label="arrow-fsspec-opendalfs",
        filesystem=filesystem,
        bucket=None,
        settings=(
            f"write_chunk={backend.write_chunk}",
            f"write_concurrent={backend.write_concurrent}",
        ),
    )


def _s3fs_backend(config: dict[str, str], profile: BenchmarkProfile) -> Backend:
    options = {
        "key": config["access_key_id"],
        "secret": config["secret_access_key"],
        "client_kwargs": {
            "endpoint_url": config["endpoint"],
            "region_name": config["region"],
        },
        "config_kwargs": {"s3": {"addressing_style": "path"}},
    }
    if profile.chunk_size is not None:
        options["default_block_size"] = profile.chunk_size
    if profile.write_concurrent is not None:
        options["max_concurrency"] = profile.write_concurrent
        options["default_cache_type"] = "none"
        options["default_fill_cache"] = False
    options["skip_instance_cache"] = True
    backend = fsspec.filesystem("s3", **options)
    filesystem = pafs.PyFileSystem(pafs.FSSpecHandler(backend))
    return Backend(
        label="arrow-fsspec-s3",
        filesystem=filesystem,
        bucket=config["bucket"],
        settings=(
            f"default_block_size={backend.default_block_size}",
            f"max_concurrency={backend.max_concurrency}",
            f"cache_type={backend.default_cache_type}",
        ),
    )


def _run_size(
    backends: list[Backend],
    args: argparse.Namespace,
    size_mib: int,
    run_prefix: str,
) -> None:
    payload = b"x" * (size_mib * MIB)
    timings = {backend.label: [] for backend in backends}
    rounds = args.warmups + args.repeats

    for round_index in range(rounds):
        offset = round_index % len(backends)
        ordered = backends[offset:] + backends[:offset]
        phase = "warmup" if round_index < args.warmups else "sample"
        for backend in ordered:
            key = f"{run_prefix}/{backend.label}/{size_mib}mib/{phase}-{round_index}"
            result = _run_benchmark(
                backend.filesystem,
                backend.path(key),
                payload,
                args.files,
                args.workers,
            )
            if phase == "sample":
                timings[backend.label].append(result)

    print(f"\nsize={size_mib} MiB files={args.files}")
    for backend in backends:
        _report(backend, size_mib, args.files, timings[backend.label])


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare Arrow direct, opendalfs, and s3fs on MinIO."
    )
    parser.add_argument(
        "--profile",
        choices=tuple(PROFILES),
        default="defaults",
        help="Use backend defaults or comparable controlled settings",
    )
    parser.add_argument(
        "--sizes",
        type=_parse_sizes,
        default="1,16,64",
        help="Comma-separated object sizes in MiB (default: 1,16,64)",
    )
    parser.add_argument("--files", type=int, default=2)
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--prefix", default="opendalfs-bench")
    parser.add_argument("--bucket")
    parser.add_argument("--region")
    parser.add_argument("--endpoint")
    parser.add_argument("--access-key-id")
    parser.add_argument("--secret-access-key")
    parser.add_argument(
        "--opendalfs-path",
        help="Load opendalfs from another checkout",
    )
    parser.add_argument(
        "--skip-s3fs",
        action="store_true",
        help="Skip the fsspec+s3fs comparison",
    )
    args = parser.parse_args()
    for option, value in {
        "--files": args.files,
        "--workers": args.workers,
        "--repeats": args.repeats,
    }.items():
        if value <= 0:
            parser.error(f"{option} must be positive")
    if args.warmups < 0:
        parser.error("--warmups must be non-negative")
    return args


def main() -> None:
    args = _parse_args()
    config = _load_config(args)
    profile = PROFILES[args.profile]
    client = _s3_client(config)
    _ensure_bucket(client, config["bucket"])
    run_prefix = f"{args.prefix}-{uuid4().hex}"

    backends = [
        _arrow_backend(config, profile),
        _opendalfs_backend(config, profile, args.opendalfs_path),
    ]
    if not args.skip_s3fs:
        backends.append(_s3fs_backend(config, profile))

    print(
        f"profile={args.profile} endpoint={config['endpoint']} "
        f"bucket={config['bucket']} sizes={args.sizes} files={args.files} "
        f"workers={args.workers} warmups={args.warmups} repeats={args.repeats}"
    )
    for backend in backends:
        print(f"{backend.label}: {', '.join(backend.settings)}")

    try:
        for size_mib in args.sizes:
            _run_size(backends, args, size_mib, run_prefix)
    finally:
        _delete_prefix(client, config["bucket"], run_prefix)


if __name__ == "__main__":
    main()
