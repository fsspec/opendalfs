import pytest


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
