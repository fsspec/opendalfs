import pytest

from opendalfs import WriteOptions


class OperatorOpenCaptureProxy:
    def __init__(self, operator):
        self._operator = operator
        self.captured: dict[str, int] = {}

    def open(self, path, mode, **kwargs):
        if "chunk" in kwargs:
            self.captured["chunk"] = int(kwargs["chunk"])
        return self._operator.open(path, mode, **kwargs)

    def __getattr__(self, name):
        return getattr(self._operator, name)


def _attach_open_capture_proxy(fs) -> OperatorOpenCaptureProxy:
    proxy = OperatorOpenCaptureProxy(fs.operator)
    fs.operator = proxy
    return proxy


def test_open_read_seek(any_fs):
    data = b"0123456789"
    any_fs.pipe_file("readseek.txt", data)

    with any_fs.open("readseek.txt", "rb") as f:
        assert f.read(3) == b"012"
        assert f.tell() == 3

        f.seek(2, 1)
        assert f.tell() == 5

        f.seek(5)
        assert f.read(2) == b"56"

        f.seek(-3, 2)
        assert f.read() == b"789"


def test_open_write_chunked(any_fs):
    with any_fs.open("chunked.txt", "wb", block_size=3) as f:
        f.write(b"abc")
        f.write(b"def")
        f.write(b"gh")

    assert any_fs.cat_file("chunked.txt") == b"abcdefgh"


def test_open_write_tell_tracks_position(memory_fs):
    with memory_fs.open("tell.txt", "wb") as f:
        assert f.tell() == 0
        f.write(b"abc")
        assert f.tell() == 3
        f.write(b"de")
        assert f.tell() == 5

    assert memory_fs.cat_file("tell.txt") == b"abcde"


def test_fsspec_minimal_file_contract(memory_fs):
    memory_fs.pipe_file("contract.txt", b"abc")

    with memory_fs.open("contract.txt", "rb") as f:
        assert f.fs is memory_fs
        assert f.path == "contract.txt"
        assert f.size == 3
        assert f.read(1) == b"a"
        f.seek(0)
        assert f.tell() == 0

    with memory_fs.open("contract-write.txt", "wb") as f:
        f.write(b"x")
        f.commit()
    assert memory_fs.cat_file("contract-write.txt") == b"x"


def test_open_append_tell_starts_at_end(memory_fs):
    memory_fs.pipe_file("append-tell.txt", b"hello")

    with memory_fs.open("append-tell.txt", "ab") as f:
        assert f.tell() == 5
        f.write(b"world")
        assert f.tell() == 10

    assert memory_fs.cat_file("append-tell.txt") == b"helloworld"


def test_block_size_maps_to_write_chunk(memory_fs):
    proxy = _attach_open_capture_proxy(memory_fs)

    with memory_fs.open("chunk-map.txt", "wb", block_size=7):
        pass

    assert proxy.captured.get("chunk") == 7


def test_block_size_does_not_override_explicit_chunk(memory_fs):
    proxy = _attach_open_capture_proxy(memory_fs)

    with memory_fs.open(
        "chunk-explicit.txt",
        "wb",
        block_size=7,
        write_options=WriteOptions(chunk=3),
    ):
        pass

    assert proxy.captured.get("chunk") == 3


def test_open_write_with_options(memory_fs):
    data = b"hello-opendal"
    with memory_fs.open(
        "opt-write.txt",
        "wb",
        write_options=WriteOptions(chunk=4, concurrent=2),
    ) as f:
        f.write(data)

    assert memory_fs.cat_file("opt-write.txt") == data


def test_open_write_with_mapping_write_options(memory_fs):
    data = b"hello-mapping"
    with memory_fs.open(
        "opt-write-map.txt",
        "wb",
        write_options={"chunk": 4, "concurrent": 2, "content_type": "text/plain"},
    ) as f:
        f.write(data)

    assert memory_fs.cat_file("opt-write-map.txt") == data


def test_open_write_with_invalid_write_options(memory_fs):
    with pytest.raises(TypeError):
        with memory_fs.open(
            "opt-write-invalid.txt",
            "wb",
            write_options={"chunk": "4"},
        ) as f:
            f.write(b"noop")


def test_open_exclusive_create(any_fs):
    any_fs.pipe_file("exists.txt", b"x")

    with pytest.raises(FileExistsError):
        with any_fs.open("exists.txt", "xb") as f:
            f.write(b"y")


@pytest.mark.asyncio
async def test_open_async_read_seek():
    from opendalfs import OpendalFileSystem

    fs = OpendalFileSystem(scheme="memory", asynchronous=True, skip_instance_cache=True)
    data = b"0123456789"
    await fs._pipe_file("readseek.txt", data)

    async with await fs.open_async("readseek.txt", "rb") as f:
        assert await f.read(3) == b"012"
        assert f.tell() == 3

        f.seek(5)
        assert await f.read(2) == b"56"

        f.seek(-3, 2)
        assert await f.read() == b"789"


@pytest.mark.asyncio
async def test_open_async_write_chunked():
    from opendalfs import OpendalFileSystem

    fs = OpendalFileSystem(scheme="memory", asynchronous=True, skip_instance_cache=True)

    async with await fs.open_async("chunked.txt", "wb", block_size=3) as f:
        await f.write(b"abc")
        await f.write(b"def")
        await f.write(b"gh")

    assert await fs._cat_file("chunked.txt") == b"abcdefgh"


@pytest.mark.asyncio
async def test_open_async_exclusive_create():
    from opendalfs import OpendalFileSystem

    fs = OpendalFileSystem(scheme="memory", asynchronous=True, skip_instance_cache=True)
    await fs._pipe_file("exists.txt", b"x")

    with pytest.raises(FileExistsError):
        async with await fs.open_async("exists.txt", "xb") as f:
            await f.write(b"y")


@pytest.mark.asyncio
async def test_open_async_append_emulated():
    from opendalfs import OpendalFileSystem

    fs = OpendalFileSystem(scheme="memory", asynchronous=True, skip_instance_cache=True)
    await fs._pipe_file("append.txt", b"hello")

    async with await fs.open_async("append.txt", "ab", block_size=2) as f:
        assert f.tell() == 5
        await f.write(b"world")

    assert await fs._cat_file("append.txt") == b"helloworld"


@pytest.mark.asyncio
async def test_cat_file_ranges_async():
    from opendalfs import OpendalFileSystem

    fs = OpendalFileSystem(scheme="memory", asynchronous=True, skip_instance_cache=True)
    await fs._pipe_file("range.txt", b"0123456789")

    assert await fs._cat_file("range.txt", start=2, end=5) == b"234"
    assert await fs._cat_file("range.txt", start=-4) == b"6789"
    assert await fs._cat_file("range.txt", end=-1) == b"012345678"
    assert await fs._cat_file("range.txt", start=-4, end=-1) == b"678"
    assert await fs._cat_file("range.txt", start=5, end=5) == b""
