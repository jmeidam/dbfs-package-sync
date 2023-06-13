import os
import pytest
from dbfsps.syncer.file import File, sort_list_of_files, calculate_file_hash


def test_sort_list_of_files():
    paths = [
        "notebook.py",
        "file1.py",
        "module/file2.py",
        "module/submodule/z.py",
        "module/b.py",
        "module/submodule/x.py",
        "module/submodule/y.py",
        "module/a_submodule/a.py",
    ]
    expected_paths = [
        "file1.py",
        "notebook.py",
        "module/b.py",
        "module/file2.py",
        "module/a_submodule/a.py",
        "module/submodule/x.py",
        "module/submodule/y.py",
        "module/submodule/z.py",
    ]
    files = [File(path, "package", "rootdir", hashstr="x") for path in paths]

    sorted_files = sort_list_of_files(files)

    assert expected_paths == [file.path for file in sorted_files]


def test_calculate_file_hash(mocker, tmpdir):
    """Ensure that sha256 is called with the file contents and that the hexdigest string is returned"""
    sha_object = mocker.Mock()
    sha_object.hexdigest.return_value = "hash_result"
    sha_func = mocker.patch("dbfsps.syncer.file.sha256", return_value=sha_object)
    filepath = os.path.join(str(tmpdir), "testfile.txt")
    with open(filepath, "w") as f:
        f.write("123\n")

    returned_hash = calculate_file_hash(filepath)

    assert returned_hash == "hash_result"
    sha_func.assert_called_once_with(b"123\n")


def test_calculate_file_hash_fnf():
    with pytest.raises(FileNotFoundError) as err:
        calculate_file_hash("/not/a/path")

    assert "Unable to calculate hash. File /not/a/path does not exist" in str(err.value)


def test_file(mocker):
    """Ensure that normal flow, when not manually passing a file hash, sets up the proper File attributes"""
    mocker.patch("dbfsps.syncer.file.calculate_file_hash", return_value="file_hash")
    path_rel = os.path.join("rel", "path", "file.py")
    path_package = "packagename"
    path_root = os.path.join(os.sep + "the", "root", "dir")

    f = File(path_rel, path_package, path_root)

    assert f.hash == "file_hash"
    assert f.path_abs == os.path.join(path_root, path_package, path_rel)
    assert f.path == path_rel
    assert f.path_remote == path_rel


def test_file_manual_hash(mocker):
    """Ensure that proper attributes are set up when manually passing a file hash and remote path"""
    calc = mocker.patch("dbfsps.syncer.file.calculate_file_hash")
    path_rel = os.path.join("rel", "path", "file.py")
    path_package = "packagename"
    path_root = os.path.join(os.sep + "the", "root", "dir")
    path_remote = "rel/remote/path"

    f = File(path_rel, path_package, path_root, relpath_remote=path_remote, hashstr="manual")

    calc.assert_not_called()
    assert f.hash == "manual"
    assert f.path == path_rel
    assert f.path_abs == os.path.join(path_root, path_package, path_rel)
    assert f.path_remote == path_remote


def test_file_equate():
    f1 = File("path_rel", "path_package", "path_root", hashstr="hash1")
    f2 = File("path_rel", "path_package", "path_root", hashstr="hash2")
    f1b = File("path_rel", "path_package", "path_root", hashstr="hash1")

    should_be_false = f1 == f2
    should_be_true = f1 == f1b
    should_be_true2 = f1 != f2
    should_be_false2 = f1 != f1b

    assert not should_be_false
    assert not should_be_false2
    assert should_be_true
    assert should_be_true2
