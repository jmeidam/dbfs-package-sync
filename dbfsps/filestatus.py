import os
import logging
from typing import Tuple
from pandas import read_csv, DataFrame, to_datetime, concat
from datetime import datetime
from dbfsps.cli.utils import create_requirements_file


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
        df_filetimes = DataFrame({'filepath': [], 'mod_datetime': []})
    return df_filetimes


def _update_requirements(df_status: DataFrame, cur_datetime: datetime, modifed_files: list):
    # This method updates df_status and modified_files in place.
    # Get the file's mod_datetime from the last status
    path = 'requirements.txt'
    logger = logging.getLogger(__name__)
    prev_datetime = to_datetime(df_status.loc[df_status.filepath == path, 'mod_datetime'].values[0])
    logger.debug(f'Current datetime:  {cur_datetime}')
    logger.debug(f'Previous datetime: {prev_datetime}')
    # If the lockfile has been modified, update mod_datetime to
    # its current value and add it to the list of modified files
    if cur_datetime > prev_datetime and os.path.isfile(path):
        logger.debug(f'Project requirements were modified since last scan')
        create_requirements_file()
        df_status.loc[df_status.filepath == path, 'mod_datetime'] = cur_datetime
        modifed_files.append(path)


def _create_requirements(df_status: DataFrame, cur_datetime: datetime, modified_files: list) -> DataFrame:
    path = 'requirements.txt'
    create_requirements_file()
    logger = logging.getLogger(__name__)
    logger.debug(f'Adding new file {path}')
    # Add new record
    df_new_record = DataFrame({'filepath': [path], 'mod_datetime': [cur_datetime]})
    df_status = concat([df_status, df_new_record], ignore_index=True)
    modified_files.append(path)
    return df_status


def _update_existing_status(df_status: DataFrame, cur_datetime: datetime, path: str, modifed_files: list):
    # This method updates df_status and modified_files in place.
    # Get the file's mod_datetime from the last status
    logger = logging.getLogger(__name__)
    prev_datetime = to_datetime(df_status.loc[df_status.filepath == path, 'mod_datetime'].values[0])
    logger.debug(f'Current datetime:  {cur_datetime}')
    logger.debug(f'Previous datetime: {prev_datetime}')
    # If the file has been modified, update mod_datetime to its current value and add it to the list of modified files
    if cur_datetime > prev_datetime and os.path.isfile(path):
        logger.debug(f'{path} was modified since last scan')
        df_status.loc[df_status.filepath == path, 'mod_datetime'] = cur_datetime
        modifed_files.append(path)


def _add_record_to_status(df_status: DataFrame, cur_datetime: datetime, path: str, modified_files: list) -> DataFrame:
    logger = logging.getLogger(__name__)
    logger.debug(f'Adding new file {path}')
    # Add new record
    df_new_record = DataFrame({'filepath': [path], 'mod_datetime': [cur_datetime]})
    df_status = concat([df_status, df_new_record], ignore_index=True)
    modified_files.append(path)
    return df_status


def update_status_data(
        df_status: DataFrame, package_location: str, skip_dirs: list = None) -> Tuple[DataFrame, list, list]:
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
        skip_dirs = ['__pycache__']
    else:
        skip_dirs += ['__pycache__']
    modified_files = []
    current_files = df_status["filepath"].to_list()

    # Handle requirements file (generated using poetry)
    # Script runs from root of repo, so we can use relative path to requirements and lock files
    logger.debug(f'Scanning poetry.lock')
    mtime = os.path.getmtime('./poetry.lock')
    cur_datetime = to_datetime(mtime, unit='s')
    if "requirements.txt" in current_files:
        _update_requirements(df_status, cur_datetime, modified_files)
    else:
        df_status = _create_requirements(df_status, cur_datetime, modified_files)

    for root, dirs, files in os.walk(package_location):
        if os.path.basename(root) not in skip_dirs:
            for file in files:
                path = os.path.join(root, file)
                logger.debug(f'Scanning {path}')

                # Get last modified time in UTC
                mtime = os.path.getmtime(path)
                cur_datetime = to_datetime(mtime, unit='s')
                if path in current_files:
                    _update_existing_status(df_status, cur_datetime, path, modified_files)
                else:
                    df_status = _add_record_to_status(df_status, cur_datetime, path, modified_files)

    deleted_files = []
    logger.debug('Checking for missing files')
    for path in df_status["filepath"].to_list():
        if not os.path.isfile(path):
            logger.debug(f'{path} was removed since last scan')
            df_status = df_status.loc[df_status.filepath != path, :]
            deleted_files.append(path)

    return df_status, modified_files, deleted_files
