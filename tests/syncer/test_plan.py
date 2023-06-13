import os
import shutil
import pytest
from pathlib import Path
from dbfsps.syncer.state import State
from dbfsps.syncer.plan import Plan, get_requirements_relative_path


def create_statefile(path: str):
    with open(path, "w") as f:
        f.writelines([
            "file1.py,123\n",
            "file2.py,124\n",
            "path/file3.py,125\n",
            "path/file4.py,126\n",
        ])


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
            tmpdir / "package" / "__pycache__" / "something.pyc"
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
