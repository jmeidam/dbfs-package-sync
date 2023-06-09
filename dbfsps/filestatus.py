import os
import logging
from typing import Tuple, Callable
from pandas import read_csv, DataFrame, to_datetime, concat
from datetime import datetime
from dbfsps.cli.utils import create_requirements_file


class Status:
    def __init__(self, package_name: str, root_dir: str, status_file_name: str = ".dbfsps_file_status"):
        self.logger = logging.getLogger(__name__)
        self._skip_dirs = ["__pycache__"]
        self.package_name = package_name
        self.root_dir = root_dir
        self.package_path = os.path.join(self.root_dir, self.package_name)
        self.status_filepath = os.path.join(root_dir, status_file_name)
        self._status_df = self._load_status_data()
        self._mod_files = []
        self._deleted_files = []

    def get_modified_files(self) -> list:
        return self._mod_files

    def _load_status_data(self) -> DataFrame:
        """Tries to load status data from provided file path.
        Returns empty dataframe with "filepath" and "mod_datetime" column if file is not found
        """
        logger = logging.getLogger(__name__)
        logger.debug(f'Loading status from "{self.status_filepath}"')
        try:
            df_filetimes = read_csv(self.status_filepath)
        except FileNotFoundError:
            logger.debug(f'File "{self.status_filepath}" not found, creating clean status table.')
            df_filetimes = DataFrame({"filepath": [], "mod_datetime": []})
        return df_filetimes

    @staticmethod
    def get_current_timestamp(path: str) -> datetime:
        """Get last modified time in UTC"""
        mtime = os.path.getmtime(path)
        cur_datetime = to_datetime(mtime, unit="s")
        return cur_datetime

    def _read_datetime_from_data(self, filepath: str) -> datetime:
        filt = self._status_df["filepath"] == filepath
        return to_datetime(self._status_df.loc[filt, "mod_datetime"].values[0])

    def _write_datetime_to_date(self, filepath: str, cur_datetime: datetime):
        filt = self._status_df["filepath"] == filepath
        self._status_df.loc[filt, "mod_datetime"] = cur_datetime

    def get_last_synced_datetime(self) -> datetime:
        if "last_synced" in self._status_df["filepath"].to_list():
            filt = self._status_df["filepath"] == "last_synced"
            dt = to_datetime(self._status_df.loc[filt, "mod_datetime"].values[0])
        else:
            dt = datetime.min
        return dt

    def _update_existing_status(self, filepath: str, cur_datetime: datetime, side_effect: Callable = None):
        # This method updates df_status and modified_files in place.
        # Get the file's mod_datetime from the last status
        logger = logging.getLogger(__name__)
        prev_datetime = self._read_datetime_from_data(filepath)
        last_synced_time = self.get_last_synced_datetime()
        logger.debug(f"Current datetime:  {cur_datetime}")
        logger.debug(f"Previous datetime: {prev_datetime}")
        # If the lockfile has been modified, update mod_datetime to
        # its current value and add it to the list of modified files
        if cur_datetime > prev_datetime and cur_datetime > last_synced_time and os.path.isfile(filepath):
            if side_effect is not None:
                side_effect()
            self._write_datetime_to_date(filepath, cur_datetime)
            if filepath != "last_synced":
                self._mod_files.append(filepath)

    def _add_record_to_status(self, cur_datetime: datetime, filepath: str, side_effect: Callable = None):
        self.logger.debug(f"Adding new file {filepath}")
        # Add new record
        df_new_record = DataFrame({"filepath": [filepath], "mod_datetime": [cur_datetime]})
        self._status_df = concat([self._status_df, df_new_record], ignore_index=True)
        if filepath != "last_synced":
            self._mod_files.append(filepath)
        if side_effect is not None:
            side_effect()

    def update_last_synced(self, last_synced_datetime: datetime):
        records = self._status_df["filepath"].to_list()
        if "last_synced" not in records:
            self.logger.debug(f"Regestering last synced time {last_synced_datetime}")
            self._add_record_to_status(last_synced_datetime, "last_synced")
        else:
            self.logger.debug(f"Updating last synced time with {last_synced_datetime}")
            self._update_existing_status("last_synced", last_synced_datetime)



    def update(self):
        """Scans the status data and updates it with new timestamps.
        The results of the scan is an updated status table and a list of files that were modified since the last scan.
        """
        current_files = self.get_modified_files()

        # Handle requirements file (generated using poetry)
        # Script runs from root of repo, so we can use relative path to requirements and lock files
        self.logger.debug(f"Scanning poetry.lock")
        poetry_lock_path = os.path.join(self.root_dir, "poetry.lock")
        requirements_path = os.path.join(self.root_dir, "requirements.txt")
        cur_datetime = self.get_current_timestamp(poetry_lock_path)
        if requirements_path in current_files:
            self._update_existing_status(requirements_path, cur_datetime, side_effect=create_requirements_file)
        else:
            self._add_record_to_status(cur_datetime, requirements_path, side_effect=create_requirements_file)

        for root, dirs, files in os.walk(self.package_path):
            if os.path.basename(root) not in self._skip_dirs:
                for file in files:
                    path = os.path.join(root, file)
                    self.logger.debug(f"Scanning {path}")

                    # Get last modified time in UTC
                    cur_datetime = self.get_current_timestamp(path)
                    if path in current_files:
                        self._update_existing_status(path, cur_datetime)
                    else:
                        self._add_record_to_status(cur_datetime, path)

        self.logger.debug("Checking for missing files")
        for path in current_files:
            if not os.path.isfile(path):
                self.logger.debug(f"{path} was removed since last scan")
                self._status_df = self._status_df.loc[self._status_df["filepath"] != path, :]
                self._deleted_files.append(path)

        self.logger.debug(f'Writing updated status table to "{self.status_filepath}"')
        self._status_df.to_csv(self.status_filepath, index=False)


def load_status_data(statusfile: str) -> DataFrame:
    """Tries to load status data from provided file path.
    Returns empty dataframe with "filepath" and "mod_datetime" column is file is not found

    :param statusfile:
    :return:
    """
    logger = logging.getLogger(__name__)
    logger.debug(f'Loading status from "{statusfile}"')
    try:
        df_filetimes = read_csv(statusfile)
    except FileNotFoundError:
        logger.debug(f'File "{statusfile}" not found, creating clean status table.')
        df_filetimes = DataFrame({"filepath": [], "mod_datetime": []})
    return df_filetimes


def _update_requirements(df_status: DataFrame, cur_datetime: datetime, modifed_files: list):
    # This method updates df_status and modified_files in place.
    # Get the file's mod_datetime from the last status
    path = "requirements.txt"
    logger = logging.getLogger(__name__)
    prev_datetime = to_datetime(df_status.loc[df_status.filepath == path, "mod_datetime"].values[0])
    logger.debug(f"Current datetime:  {cur_datetime}")
    logger.debug(f"Previous datetime: {prev_datetime}")
    # If the lockfile has been modified, update mod_datetime to
    # its current value and add it to the list of modified files
    if cur_datetime > prev_datetime and os.path.isfile(path):
        logger.debug(f"Project requirements were modified since last scan")
        create_requirements_file()
        df_status.loc[df_status.filepath == path, "mod_datetime"] = cur_datetime
        modifed_files.append(path)


def _create_requirements(df_status: DataFrame, cur_datetime: datetime, modified_files: list) -> DataFrame:
    path = "requirements.txt"
    create_requirements_file()
    logger = logging.getLogger(__name__)
    logger.debug(f"Adding new file {path}")
    # Add new record
    df_new_record = DataFrame({"filepath": [path], "mod_datetime": [cur_datetime]})
    df_status = concat([df_status, df_new_record], ignore_index=True)
    modified_files.append(path)
    return df_status


def _update_existing_status(df_status: DataFrame, cur_datetime: datetime, path: str, modifed_files: list):
    # This method updates df_status and modified_files in place.
    # Get the file's mod_datetime from the last status
    logger = logging.getLogger(__name__)
    prev_datetime = to_datetime(df_status.loc[df_status.filepath == path, "mod_datetime"].values[0])
    logger.debug(f"Current datetime:  {cur_datetime}")
    logger.debug(f"Previous datetime: {prev_datetime}")
    # If the file has been modified, update mod_datetime to its current value and add it to the list of modified files
    if cur_datetime > prev_datetime and os.path.isfile(path):
        logger.debug(f"{path} was modified since last scan")
        df_status.loc[df_status.filepath == path, "mod_datetime"] = cur_datetime
        modifed_files.append(path)


def _add_record_to_status(df_status: DataFrame, cur_datetime: datetime, path: str, modified_files: list) -> DataFrame:
    logger = logging.getLogger(__name__)
    logger.debug(f"Adding new file {path}")
    # Add new record
    df_new_record = DataFrame({"filepath": [path], "mod_datetime": [cur_datetime]})
    df_status = concat([df_status, df_new_record], ignore_index=True)
    modified_files.append(path)
    return df_status


def update_status_data(
    df_status: DataFrame, package_location: str, skip_dirs: list = None
) -> Tuple[DataFrame, list, list]:
    """Scans the status data and updates it with new timestamps.
    The results of the scan is an updated status table and a list of files that were modified since the last scan.

    :param df_status:
        File status dataframe with columns "filepath" and "mod_datetime"
    :param package_location:
        Location of package folder to scan
    :param skip_dirs:
        List of directories to skip. Default is __pycache__
    :return:
    """
    logger = logging.getLogger(__name__)
    if not skip_dirs:
        skip_dirs = ["__pycache__"]
    else:
        skip_dirs += ["__pycache__"]
    modified_files = []
    current_files = df_status["filepath"].to_list()

    # Handle requirements file (generated using poetry)
    # Script runs from root of repo, so we can use relative path to requirements and lock files
    logger.debug(f"Scanning poetry.lock")
    mtime = os.path.getmtime("./poetry.lock")
    cur_datetime = to_datetime(mtime, unit="s")
    if "requirements.txt" in current_files:
        _update_requirements(df_status, cur_datetime, modified_files)
    else:
        df_status = _create_requirements(df_status, cur_datetime, modified_files)

    for root, dirs, files in os.walk(package_location):
        if os.path.basename(root) not in skip_dirs:
            for file in files:
                path = os.path.join(root, file)
                logger.debug(f"Scanning {path}")

                # Get last modified time in UTC
                mtime = os.path.getmtime(path)
                cur_datetime = to_datetime(mtime, unit="s")
                if path in current_files:
                    _update_existing_status(df_status, cur_datetime, path, modified_files)
                else:
                    df_status = _add_record_to_status(df_status, cur_datetime, path, modified_files)

    deleted_files = []
    logger.debug("Checking for missing files")
    for path in df_status["filepath"].to_list():
        if not os.path.isfile(path):
            logger.debug(f"{path} was removed since last scan")
            df_status = df_status.loc[df_status.filepath != path, :]
            deleted_files.append(path)

    return df_status, modified_files, deleted_files
