import os
import logging
from dbfsps.syncer.state import State
from dbfsps.syncer.file import File, sort_list_of_files, calculate_file_hash
from dbfsps.sdk.dbfs import Dbfs
from dbfsps.cli.utils import create_requirements_file


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

        self.logger.info(f"Creating plan for package {self.state.packagepath}")
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
        self._add_requirements_file()

    def _add_requirements_file(self):
        req_rel_path = get_requirements_relative_path(self.state.package)
        req_abs_path = os.path.join(self.state.root, "requirements.txt")
        lock_abs_path = os.path.join(self.state.root, "poetry.lock")

        if not os.path.isfile(req_abs_path):
            create_requirements_file()

        requirements_hash = calculate_file_hash(lock_abs_path)
        file_req = File(
            req_rel_path, self.state.package, self.state.root,
            hashstr=requirements_hash, relpath_remote="requirements.txt")
        self.local_files[file_req.path] = file_req

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
            if file_local != file_remote:
                self.logger.debug(f"Hash of {path} differs")
                if "requirements.txt" in path:
                    # In this case the lockfile has changes, so the requirements file needs to be regenerated
                    self.logger.info("Lockfile has changed, re-creating requirements.txt")
                    create_requirements_file()
                list_update.append(file_local)
        self.files_updated = sort_list_of_files(list_update)
        self.files_new = sort_list_of_files([self.local_files[k] for k in list_new])
        self.files_deleted = sort_list_of_files([self.state.files[k] for k in list_delete])

    def print_plan(self):
        """Prints the plan to standard output"""
        n_upd = len(self.files_updated)
        n_new = len(self.files_new)
        n_del = len(self.files_deleted)
        summary = f"{n_del} files will be deleted; {n_new} files will be added; {n_upd} files will be updated."
        header, footer = self._format_header_footer(summary)
        print(header)
        for file in self.files_updated:
            print(f"File {file.path} will be updated")
        for file in self.files_new:
            print(f"File {file.path} will be added")
        for file in self.files_deleted:
            print(f"File {file.path} will be removed")
        print(summary)
        print(f"Remote path is {self.remote_path}")
        print(footer)

    def _format_header_footer(self, summary):
        title = f"Plan for syncing {self.state.package}"
        space_to_fill = int(len(summary) - 2 - len(title))
        if space_to_fill % 2 != 0:
            space_to_fill += 1
        n_header_signs = int(space_to_fill/2)
        header_sep = "=" * n_header_signs
        header = f"{header_sep} {title} {header_sep}"
        footer = "=" * len(header)
        return header, footer

    def apply_plan(self, dbfs: Dbfs):
        """Executes the delete/add/update operations from the plan and updates the statefile

        :param dbfs:
            An instance of the dbfs client to connect to Databricks
        """
        files_to_upload = self.files_updated+self.files_new
        files_uploaded = []
        files_deleted = []

        if files_to_upload or self.files_deleted:
            self.logger.info("Applying plan...")

        for file in files_to_upload:
            dbfs_path = os.path.join(self.remote_path, file.path_remote)
            self.logger.info(f"Copying {file.path_abs} to {dbfs_path}")
            try:
                dbfs.cp(file.path_abs, dbfs_path, overwrite=True)
                files_uploaded.append(file)
            except Exception as exc:
                self.logger.error(f"Exception encountered while copying {file.path}: {exc}")
        for file in self.files_deleted:
            dbfs_path = os.path.join(self.remote_path, file.path_remote)
            self.logger.info(f"Removing {dbfs_path}")
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


def get_requirements_relative_path(rel_package_path: str) -> str:
    """The requirements file should be in the root of the repo.
    This calculates the relative path to the file from the package dir"""
    levels = len(rel_package_path.split(os.sep))
    prefix = os.sep.join([".." for _ in range(levels)])
    rel_path = os.path.join(prefix, "requirements.txt")
    return rel_path
