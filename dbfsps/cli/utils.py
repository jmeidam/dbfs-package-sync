import logging
import os
import click
from dbfsps import __version__
import subprocess


CONTEXT_SETTINGS = {"help_option_names": ["-h", "--help"]}


def process_cmd_command(command: str):
    logger = logging.getLogger(__name__)
    logger.debug(f'Running command: "{command}"')
    try:
        # do something with output
        subprocess.check_call(command.split())
    except subprocess.CalledProcessError:
        # There was an error - command exited with non-zero code
        logger.error(f'command "{command}" failed')
        raise


# Stolen from databricks-cli
def print_version_callback(ctx, param, value):  # NOQA
    if not value or ctx.resilient_parsing:
        return
    click.echo("Version {}".format(__version__))
    ctx.exit()


def create_requirements_file():
    """Uses the shell and poetry to generate a requirements.txt"""
    logger = logging.getLogger(__name__)
    logger.info("(re-)Creating requirements.txt")
    process_cmd_command("poetry export -f requirements.txt --output requirements.txt")


def verify_dbfs_path(dbfs_path: str) -> str:
    """Verify that the dbfs path has the proper format"""
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
