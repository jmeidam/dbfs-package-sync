from requests.exceptions import HTTPError

from databricks_cli.dbfs.api import DbfsApi
from databricks_cli.dbfs.api import TempDir
from databricks_cli.dbfs.dbfs_path import DbfsPath
from databricks_cli.sdk import ApiClient

from dbfsps.sdk.errors import DatabricksApiError

__all__ = ["Dbfs"]


class DbfsPathNoClicks(DbfsPath):
    def __init__(self, absolute_path, validate=True):
        super().__init__(absolute_path, validate=validate)

    # Some Databricks CLI methods have a clicks function, which causes trouble for us
    def validate(self):
        """
        Checks that the path is a proper DbfsPath. it must have a prefix of
        "dbfs:" and must be an absolute path.
        """
        if self.absolute_path.startswith("dbfs://"):
            raise ValueError(f"The path {repr(self)} cannot start with dbfs://. " "It must start with dbfs:/")
        if not self.is_absolute_path:
            raise ValueError(f'The path {repr(self)} must start with "dbfs:/"')

    def __repr__(self):
        return str(self.absolute_path)


class Dbfs:
    """Creates a Python-native implementation for the following dbfs CLI commands

        cp
            Copy files to and from DBFS.
        ls
            List files in DBFS.
        rm
            Remove files from DBFS.
        mkdirs
            Make directories in DBFS.
        mv
            Moves a file between two DBFS paths.
        cat
            Show the contents of a file.

    :param host:
        example: https://adb-8302248809552723.3.azuredatabricks.net or adb-8302248809552723.3.azuredatabricks.net
    :param token:
    :param kwargs:
        Any arguments aside from host and token that ApiClient accepts
    """

    def __init__(self, host: str, token: str, **kwargs):
        if not host.startswith("https://"):
            host = "https://" + host

        self._client = ApiClient(host=host, token=token, **kwargs)
        self._api = DbfsApi(self._client)

    def cp(self, source: str, destination: str, recursive: bool = False, overwrite: bool = False):
        """Copy files to and from DBFS

        :param source:
        :param destination:
        :param recursive:
        :param overwrite:
        """
        try:
            self._api.cp(recursive, overwrite, source, destination)
        except HTTPError as exc:
            raise DatabricksApiError(exc, message_prefix=f"Failed to copy {source} to {destination}")

    def ls(self, dbfs_path: str, strings_only: bool = False) -> list:
        """List files in DBFS

        :param dbfs_path:
            Path on databricks file system starting with "dbfs:"
        :param strings_only:
            If True, will return a list of paths as strings only.
            Default, False, will return list of databricks file objects (which have more information)
        """
        try:
            paths = self._api.list_files(DbfsPathNoClicks(dbfs_path))
        except HTTPError as exc:
            raise DatabricksApiError(exc, message_prefix=f"Failed to list {dbfs_path}")

        if strings_only:
            paths_strings = []
            for path_obj in paths:
                paths_strings.append(path_obj.dbfs_path)
            paths = paths_strings

        return paths

    def rm(self, dbfs_path: str, recursive: bool = False):
        """Remove files from DBFS

        :param dbfs_path:
            Path on databricks file system starting with "dbfs:"
        :param recursive:
            Set recursive to True, for removing non-empty directories
        """
        try:
            self._api.delete(DbfsPathNoClicks(dbfs_path), recursive=recursive)
        except HTTPError as exc:
            raise DatabricksApiError(exc, message_prefix=f"Failed to remove {dbfs_path}")

    def mkdirs(self, dbfs_path: str):
        """Make directories in DBFS

        :param dbfs_path:
            Path on databricks file system starting with "dbfs:"
        """
        try:
            self._api.mkdirs(DbfsPathNoClicks(dbfs_path))
        except HTTPError as exc:
            raise DatabricksApiError(exc, message_prefix=f"Failed to create {dbfs_path}")

    def mv(self, source: str, destination: str):
        """Moves a file between two DBFS paths

        :param source:
        :param destination:
        """
        try:
            self._api.move(DbfsPathNoClicks(source), DbfsPathNoClicks(destination))
        except HTTPError as exc:
            raise DatabricksApiError(exc, message_prefix=f"Failed to move {source} to {destination}")

    def cat(self, dbfs_path: str) -> str:
        """Retrieve the contents of a file

        :param dbfs_path:
            Path on databricks file system starting with "dbfs:"
        """
        with TempDir() as temp_dir:
            temp_path = temp_dir.path("temp")
            self.cp(dbfs_path, temp_path, recursive=False, overwrite=True)
            with open(temp_path) as f:
                contents = f.read()

        return contents
