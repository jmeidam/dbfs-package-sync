import os
import logging
import click
from dbfsps.cli.utils import CONTEXT_SETTINGS, get_remote_path
from dbfsps.setupnotebook import SetupNotebook
from dbfsps.syncer.state import State
from dbfsps.syncer.plan import Plan
from dbfsps.sdk.config import get_host_and_token
from dbfsps.sdk.dbfs import Dbfs


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
@click.option(
    "--root-path",
    "-b",
    default=os.path.abspath(os.curdir),
    help="Absolute path to the root dir of the repository where you can find pyproject.toml",
)
def databricks_sync_api(
    package_name: str,
    package_location: str,
    status_file: str,
    remote_path: str,
    delete_status_file: bool,
    dry_run: bool,
    profile: str,
    root_path: str
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

    st = State(root_path, package_location, statefilename=status_file)
    plan = Plan(st, remote_path=remote_path)
    plan.print_plan()

    if not dry_run:
        host, token = get_host_and_token(profile=profile)
        dbfs = Dbfs(host, token)
        plan.apply_plan(dbfs)
