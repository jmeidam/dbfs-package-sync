import os
import shutil
import pytest
from pathlib import Path
from dbfsps.syncer.state import State
from dbfsps.syncer.plan import Plan, get_requirements_relative_path


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


def plan_apply(tmpdir, remote_path, mock_dbfs):
    s = State(tmpdir, "package")
    p = Plan(s, remote_path)
    p.apply_plan(mock_dbfs)


class PlanTester:
    def __init__(self, tmpdir: Path, mocker):
        self.root = tmpdir
        self.lockfile = "poetry.lock"
        self.files = [
            tmpdir / self.lockfile,
            tmpdir / "package" / "__init__.py",
            tmpdir / "package" / "utils.py",
            tmpdir / "package" / "subdir" / "one.py",
            tmpdir / "package" / "subdir" / "two.py",
            tmpdir / "package" / "__pycache__" / "something.pyc",
        ]

        self.mock_create_requirements_file = mocker.patch(
            "dbfsps.syncer.plan.create_requirements_file", side_effect=self.create_req_file
        )
        self.mock_dbfs = mocker.Mock()

    def create_files(self):
        os.makedirs(self.root / "package" / "subdir", exist_ok=True)
        os.makedirs(self.root / "package" / "__pycache__", exist_ok=True)

        for file in self.files:
            with open(file, "w") as f:
                f.write("line1\n")

    def create_req_file(self):
        with open(self.root / "poetry.lock", "r") as f:
            text = f.read()
        with open(self.root / "requirements.txt", "w") as f:
            f.write(text)

    @staticmethod
    def change_file(path: Path, string: str):
        with open(path, "w") as f:
            f.write(f"{string}")


def test_get_requirements_relative_path():
    package = "python/src/somepackage"
    rel_path = get_requirements_relative_path(package)
    assert rel_path == "../../../requirements.txt"


def test_plan_new(mocker, tmpdir):
    """Verifies that a plan created without a statefile copies over all files in the package
    except those under the __pycache__ directory"""
    remote_path = "dbfs:/FileStore/packages/packagename"
    pt = PlanTester(tmpdir, mocker)
    pt.create_files()

    s = State(tmpdir, "package")
    p = Plan(s, remote_path)
    mock_dbfs = mocker.Mock()
    p.apply_plan(mock_dbfs)

    expected_calls = [
        mocker.call(
            os.path.join(str(tmpdir), "package", "..", "requirements.txt"),
            os.path.join(remote_path, "requirements.txt"),
            overwrite=True,
        ),
        mocker.call(str(tmpdir / "package" / "__init__.py"), os.path.join(remote_path, "__init__.py"), overwrite=True),
        mocker.call(str(tmpdir / "package" / "utils.py"), os.path.join(remote_path, "utils.py"), overwrite=True),
        mocker.call(
            str(tmpdir / "package" / "subdir" / "one.py"), os.path.join(remote_path, "subdir/one.py"), overwrite=True
        ),
        mocker.call(
            str(tmpdir / "package" / "subdir" / "two.py"), os.path.join(remote_path, "subdir/two.py"), overwrite=True
        ),
    ]

    mock_dbfs.cp.assert_has_calls(expected_calls, any_order=True)

    assert os.path.isfile(os.path.join(tmpdir, ".dbfsps_file_status"))


def test_plan_remove_files(mocker, tmpdir):
    """Verifies that a plan created without a statefile copies over all files in the package
    except those under the __pycache__ directory"""
    remote_path = "dbfs:/FileStore/packages/packagename"
    pt = PlanTester(tmpdir, mocker)
    pt.create_files()

    plan_apply(tmpdir, remote_path, mocker.Mock())

    # Delete the module with 2 files
    shutil.rmtree(tmpdir / "package" / "subdir")

    # Simulate a new session
    mock_dbfs = mocker.Mock()
    plan_apply(tmpdir, remote_path, mock_dbfs)

    expected_calls = [
        mocker.call(os.path.join(remote_path, "subdir/one.py")),
        mocker.call(os.path.join(remote_path, "subdir/two.py")),
    ]

    mock_dbfs.rm.assert_has_calls(expected_calls, any_order=True)

    with open(tmpdir / ".dbfsps_file_status", "r") as f:
        assert len(f.readlines()) == 3


def test_plan_update_lockfile(mocker, tmpdir):
    """Verifies that a plan created without a statefile copies over all files in the package
    except those under the __pycache__ directory"""
    remote_path = "dbfs:/FileStore/packages/packagename"
    pt = PlanTester(tmpdir, mocker)
    pt.create_files()

    plan_apply(tmpdir, remote_path, mocker.Mock())

    # Update lockfile
    with open(tmpdir / "poetry.lock", "w") as f:
        f.write("different contents\n")

    # Simulate a new session
    mock_dbfs = mocker.Mock()
    plan_apply(tmpdir, remote_path, mock_dbfs)

    expected_calls = [
        mocker.call(
            os.path.join(str(tmpdir), "package", "..", "requirements.txt"),
            os.path.join(remote_path, "requirements.txt"),
            overwrite=True,
        )
    ]

    mock_dbfs.cp.assert_has_calls(expected_calls, any_order=True)

    with open(tmpdir / "requirements.txt", "r") as f:
        assert f.read() == "different contents\n"


def test_print_plan(mocker, tmpdir, capfd):
    remote_path = "dbfs:/FileStore/packages/packagename"
    pt = PlanTester(tmpdir, mocker)
    pt.create_files()

    plan_apply(tmpdir, remote_path, mocker.Mock())

    # Delete two files
    shutil.rmtree(tmpdir / "package" / "subdir")

    # Add new file
    with open(tmpdir / "package" / "newfile.py", "w") as f:
        f.write("import os\n")

    # Update file
    with open(tmpdir / "package" / "utils.py", "w") as f:
        f.write("different contents\n")

    # Simulate a new session
    s = State(tmpdir, "package")
    p = Plan(s, remote_path)
    p.print_plan()

    out, err = capfd.readouterr()

    assert "File newfile.py will be added" in out
    assert "File utils.py will be updated" in out
    assert "File subdir/one.py will be removed" in out
    assert "File subdir/two.py will be removed" in out
    assert "2 files will be deleted; 1 files will be added; 1 files will be updated."
