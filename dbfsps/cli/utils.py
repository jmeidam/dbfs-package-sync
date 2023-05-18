import logging
import click
from dbfsps import __version__
import subprocess


CONTEXT_SETTINGS = {'help_option_names': ['-h', '--help']}


def process_cmd_command(command: str):
    logging.debug(f'Running command: "{command}"')
    try:
        # do something with output
        subprocess.check_call(command.split())
    except subprocess.CalledProcessError:
        # There was an error - command exited with non-zero code
        logging.error(f'command "{command}" failed')
        raise

# Stolen from databricks-cli
def print_version_callback(ctx, param, value):  # NOQA
    if not value or ctx.resilient_parsing:
        return
    click.echo('Version {}'.format(__version__))
    ctx.exit()


def create_requirements_file():
    logger = logging.getLogger(__name__)
    logger.info("(re-)Creating requirements.txt")
    process_cmd_command("poetry export -f requirements.txt --output requirements.txt")
