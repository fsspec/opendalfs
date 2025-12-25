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
