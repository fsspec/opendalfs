import pytest


def test_exclusive_writes_do_not_replace_existing_content(any_fs):
    any_fs.pipe_file("exclusive.txt", b"original")

    with pytest.raises(FileExistsError):
        any_fs.pipe_file("exclusive.txt", b"replacement", mode="create")
    with pytest.raises(FileExistsError), any_fs.open("exclusive.txt", "xb") as file:
        file.write(b"replacement")

    assert any_fs.cat_file("exclusive.txt") == b"original"
