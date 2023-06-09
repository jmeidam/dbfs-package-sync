import os
import logging
from dbfsps.syncer.state import State
from dbfsps.syncer.file import File, sort_list_of_files
from dbfsps.sdk.dbfs import Dbfs


class Plan:
    """
    Creates an execution plan based on the statefile and the local files.
    Resulting actions per file are delete, add or update.

    Use print_plan to view the files and corresponding planned operations.

    :param state:
    :param remote_path:
        Path, including dbfs: prefix to the directory to which the package should be uploaded
    """
    def __init__(self, state: State, remote_path: str):
        self.logger = logging.getLogger(__name__)
        self.state = state
        self.remote_path = remote_path
        self._skip_dirs = ["__pycache__"]
        self.local_files = {}

        self.files_deleted = []
        self.files_new = []
        self.files_updated = []

        self._get_local_files()
        self._plan()

    def _get_local_files(self):
        for root, dirs, files in os.walk(self.state.packagepath):
            if os.path.basename(root) not in self._skip_dirs:
                for file_name in files:
                    rel_file_path = os.path.join(root.replace(self.state.packagepath, '').lstrip('/'), file_name)
                    self.logger.debug(f"Scanning {rel_file_path}")

                    file_obj = File(rel_file_path, self.state.package, self.state.root)
                    self.local_files[file_obj.path] = file_obj

    def _plan(self):
        set_local = set(self.local_files.keys())
        set_remote = set(self.state.files.keys())
        set_both = set_local.intersection(set_remote)
        list_new = list(set_local - set_remote)
        list_delete = list(set_remote - set_local)
        self.logger.debug(f"List new: {list_new}")
        self.logger.debug(f"List delete: {list_delete}")
        list_update = []
        for path in set_both:
            file_local = self.local_files[path]
            file_remote = self.state.files[path]
            if file_local.hash != file_remote.hash:
                self.logger.debug(f"Hash of {path} differs")
                list_update.append(file_local)
        self.files_updated = sort_list_of_files(list_update)
        self.files_new = sort_list_of_files([self.local_files[k] for k in list_new])
        self.files_deleted = sort_list_of_files([self.state.files[k] for k in list_delete])

    def print_plan(self):
        """Prints the plan to standard output"""
        n_upd = len(self.files_updated)
        n_new = len(self.files_new)
        n_del = len(self.files_deleted)
        for file in self.files_updated:
            print(f"File {file.path} will be updated")
        for file in self.files_new:
            print(f"File {file.path} will be added")
        for file in self.files_deleted:
            print(f"File {file.path} will be removed")
        print(f"{n_del} files will be deleted; {n_new} files will be added; {n_upd} files will be updated.")

    def apply_plan(self, dbfs: Dbfs):
        """Executes the delete/add/update operations from the plan and updates the statefile

        :param dbfs:
            An instance of the dbfs client to connect to Databricks
        """
        all_files = self.files_updated+self.files_new
        files_uploaded = []
        files_deleted = []

        for file in all_files:
            dbfs_path = os.path.join(self.remote_path, file.path)
            self.logger.debug(f"Copying {file} to {dbfs_path}")
            try:
                dbfs.cp(file.path_abs, dbfs_path, overwrite=True)
                files_uploaded.append(file)
            except Exception as exc:
                self.logger.error(f"Exception encountered while copying {file.path}: {exc}")
        for file in self.files_deleted:
            dbfs_path = os.path.join(self.remote_path, file.path)
            self.logger.debug(f"Removing {dbfs_path}")
            try:
                dbfs.rm(dbfs_path)
                files_deleted.append(file)
            except Exception as exc:
                self.logger.error(f"Exception encountered while copying {file.path}: {exc}")

        for file in files_uploaded:
            self.state.files[file.path] = file
        for file in files_deleted:
            del self.state.files[file.path]

        self.state.store_state()



