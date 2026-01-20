import pytest

from opendalfs import WriteOptions


def test_open_read_seek(any_fs):
    data = b"0123456789"
    any_fs.pipe_file("readseek.txt", data)

    with any_fs.open("readseek.txt", "rb") as f:
        assert f.read(3) == b"012"
        assert f.tell() == 3

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


def test_open_write_with_options(memory_fs):
    data = b"hello-opendal"
    with memory_fs.open(
        "opt-write.txt",
        "wb",
        write_options=WriteOptions(chunk=4, concurrent=2),
    ) as f:
        f.write(data)

    assert memory_fs.cat_file("opt-write.txt") == data


def test_open_write_with_invalid_write_options(memory_fs):
    with pytest.raises(TypeError):
        with memory_fs.open(
            "opt-write-map.txt",
            "wb",
            write_options={"chunk": 4, "concurrent": 2},
        ) as f:
            f.write(b"noop")


def test_write_bypasses_fsspec_buffer(monkeypatch, memory_fs):
    from fsspec.spec import AbstractBufferedFile

    called = False
    original_write = AbstractBufferedFile.write

    def tracking_write(self, payload):
        nonlocal called
        called = True
        return original_write(self, payload)

    monkeypatch.setattr(AbstractBufferedFile, "write", tracking_write)
    with memory_fs.open("no-buffer.txt", "wb", block_size=4) as f:
        f.write(b"a")
        f.write(b"b")

    assert memory_fs.cat_file("no-buffer.txt") == b"ab"
    assert not called

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
async def test_async_write_bypasses_fsspec_buffer(monkeypatch):
    from fsspec.asyn import AbstractAsyncStreamedFile
    from opendalfs import OpendalFileSystem

    called = False
    original_write = AbstractAsyncStreamedFile.write

    async def tracking_write(self, payload):
        nonlocal called
        called = True
        return await original_write(self, payload)

    monkeypatch.setattr(AbstractAsyncStreamedFile, "write", tracking_write)

    fs = OpendalFileSystem(scheme="memory", asynchronous=True, skip_instance_cache=True)
    async with await fs.open_async("no-buffer.txt", "wb", block_size=4) as f:
        await f.write(b"a")
        await f.write(b"b")

    assert await fs._cat_file("no-buffer.txt") == b"ab"
    assert not called

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
