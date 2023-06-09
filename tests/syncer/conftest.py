import shutil
import pytest


class Dbfs:
    @classmethod
    def dbfs_cp(cls, path_local: str, path_remote: str):
        shutil.copy(path_local, path_remote)

    @classmethod
    def dbfs_rm(cls, path_remote: str):
        shutil.rmtree(path_remote)


@pytest.fixture(scope="module", autouse=True)
def dbfs(module_mocker):
    module_mocker.patch("dbfsps.sdk.dbfs.Dbfs", return_value=Dbfs)
    return Dbfs
