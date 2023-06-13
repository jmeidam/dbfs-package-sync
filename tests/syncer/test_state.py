import os
from dbfsps.syncer.state import State


def create_statefile(path: str):
    with open(path, "w") as f:
        f.writelines(
            [
                "file1.py,123\n",
                "file2.py,124\n",
                "path/file3.py,125\n",
                "path/file4.py,126\n",
            ]
        )


def test_state_init_with_file(tmpdir):
    statefilepath = os.path.join(tmpdir, ".dbfsps_file_status")
    create_statefile(statefilepath)
    path_package = os.path.join("rel", "path", "to", "package")

    s = State(tmpdir, path_package)

    assert s.root == tmpdir
    assert s.package == path_package
    assert s.packagepath == os.path.join(tmpdir, path_package)
    assert ["file1.py", "file2.py", "path/file3.py", "path/file4.py"] == sorted(s.files.keys())


def test_state_init_no_file():
    path_package = os.path.join("rel", "path", "to", "package")

    s = State("/does/not/exist", path_package)

    assert not s.files


def test_state_store(tmpdir):
    path_package = os.path.join("rel", "path", "to", "package")
    statefilepath = os.path.join(tmpdir, ".dbfsps_file_status")
    create_statefile(statefilepath)

    s = State(tmpdir, path_package)
    s.files["file1.py"].hash = "anewhash"
    s.files["path/file3.py"].hash = "alsoanewhash"

    s.store_state()

    with open(statefilepath, "r") as f:
        for line in f.readlines():
            if "file1.py" in line:
                assert line.split(",")[1].strip() == "anewhash"
            if "path/file3.p" in line:
                assert line.split(",")[1].strip() == "alsoanewhash"
