import os
import logging
import click
from dbfsps.cli.utils import CONTEXT_SETTINGS
from dbfsps.setupnotebook import SetupNotebook
from dbfsps.filestatus import load_status_data, update_status_data
from dbfsps.sdk.config import get_host_and_token
from dbfsps.sdk.dbfs import Dbfs


def verify_dbfs_path(dbfs_path: str) -> str:
    if not dbfs_path.startswith("dbfs:"):
        raise ValueError('remote path must start with "dbfs:"')
    if "\\" in dbfs_path:
        raise ValueError('remote path must be unix format, so only forward slashes "/"')
    return dbfs_path.rstrip("/")


def get_remote_path(remote_path: str, package_name: str) -> str:
    """Gets the remote path. Will try to fetch it in the following order:
        1. The remote_path argument
        2. PACKAGE_REMOTE_DIR environment variable
        3. Default path "dbfs:/FileStore/packages/"

    :param remote_path:
        DFBS path. Must be prefixed with "dbfs:"
    :param package_name:
        Appended to remote_path if remote_path is not None
    :return:
    """
    logger = logging.getLogger(__name__)
    if not remote_path:
        try:
            remote_path = os.environ["PACKAGE_REMOTE_DIR"]
            logger.debug(f'Using remote_path from environment variable: "{remote_path}"')
        except KeyError:
            remote_path = f"dbfs:/FileStore/packages/{package_name}"
            logger.debug(f'Using default remote path: "{remote_path}"')
    else:
        remote_path = remote_path
        logger.debug(f'Using remote path specified in argument: "{remote_path}"')

    remote_path = verify_dbfs_path(remote_path)

    return remote_path


def upload_file(dbfs: Dbfs, source: str, destination: str):
    dbfs.cp(source, destination, overwrite=True)


def remove_file(dbfs: Dbfs, dbfs_path: str):
    dbfs.rm(dbfs_path)


@click.command(context_settings=CONTEXT_SETTINGS)
@click.argument("package_name")
@click.option("--profile", "-p", default=None, help="Databricks CLI profile to use to make the connection.")
@click.option(
    "--package-location",
    "-l",
    default=None,
    help="Location of the package to be uploaded. Will be ./<package_name> by default",
)
@click.option(
    "--status-file",
    "-s",
    default=".dbfsps_file_status",
    help="File that keeps track of when package files were last modified",
)
@click.option(
    "--remote-path",
    "-r",
    default=None,
    help="Remote path to store package and requirements. "
    "If not provided, will first check PACKAGE_REMOTE_DIR variable, "
    "then use dbfs:/FileStore/packages/<package_name>",
)
@click.option(
    "--delete-status-file", "-x", is_flag=True, default=False, help="Delete status file if exists to start over"
)
@click.option(
    "--dry-run",
    "-d",
    is_flag=True,
    default=False,
    help="Do not upload anything, only print what would have been uploaded",
)
def databricks_sync_api(
    package_name: str,
    package_location: str,
    status_file: str,
    remote_path: str,
    delete_status_file: bool,
    dry_run: bool,
    profile: str,
):
    """
    Synchronize remote package with local changes
    """
    logger = logging.getLogger("dbfsps")
    logger.setLevel(logging.INFO)

    if not os.path.isfile("pyproject.toml"):
        raise RuntimeError("Must be run from source root directory (where pyproject.toml is located)")

    if delete_status_file:
        try:
            os.remove(status_file)
        except FileNotFoundError:
            pass

    if not profile:
        raise ValueError("Must specify a databricks-cli profile to use")

    package_name = package_name.replace("-", "_").lower()

    remote_path = get_remote_path(remote_path, package_name)

    if not package_location:
        package_location = package_name

    nb_path = f"init_{package_name}.py"
    nb = SetupNotebook(remote_path.replace("dbfs:", "/dbfs"), nb_path)
    if not os.path.isfile(nb.notebook_path):
        nb.generate_notebook_file()

    df_status = load_status_data(status_file)
    df_status, files_to_upload, files_to_remove = update_status_data(df_status, package_location)

    # Save updated status data
    logging.debug(f'Writing updated status table to "{status_file}"')
    df_status.to_csv(status_file, index=False)

    logger.info(f"The following files will be uploaded: {files_to_upload}")
    logger.info(f"The following files will be removed (remotely): {files_to_remove}")
    logger.info(f"Using remote root path {remote_path}")

    if len(files_to_upload) == 0 and len(files_to_remove) == 0:
        logger.info("Nothing to do")
        return

    if not dry_run:
        host, token = get_host_and_token(profile=profile)
        dbfs = Dbfs(host, token)
        for file_to_upload in files_to_upload:
            remote_path_full = os.path.join(remote_path, file_to_upload)
            upload_file(dbfs, file_to_upload, remote_path_full)
        for file_to_remove in files_to_remove:
            remote_path_full = os.path.join(remote_path, file_to_remove)
            remove_file(dbfs, remote_path_full)
    else:
        for file_to_upload in files_to_upload:
            remote_path_full = os.path.join(remote_path, file_to_upload)
            print(f"Will upload {file_to_upload} to {remote_path_full}")
        for file_to_remove in files_to_remove:
            remote_path_full = os.path.join(remote_path, file_to_remove)
            print(f"Will remove remote file {remote_path_full}")
