import os
import netrc
import configparser
from pathlib import Path
from typing import Tuple

from dbfsps.sdk.errors import DatabricksConfigError


def _get_auth_from_cfg(dbcfg_path: str, profile: str) -> dict:
    config = configparser.ConfigParser()
    config.read(dbcfg_path)
    if not profile:
        raise DatabricksConfigError("From cfg: Must provide a profile")

    if profile not in config.sections():
        raise DatabricksConfigError(f'From cfg: The profile named "{profile}" does not exist in {dbcfg_path}')

    try:
        host = config.get(profile, "host")
    except configparser.NoOptionError:
        raise DatabricksConfigError(f"From cfg: No host is specified for profile {profile}")
    try:
        token = config.get(profile, "token")
    except configparser.NoOptionError:
        raise DatabricksConfigError(f"From cfg: No token is specified for profile {profile}")

    return {"host": host, "token": token}


def _get_auth_from_netrc(netrc_path: str, host: str) -> dict:
    config_obj = netrc.netrc(netrc_path)
    try:
        host_details = config_obj.hosts[host.replace("https://", "")]
    except KeyError:
        raise DatabricksConfigError(f"From netrc: Cannot find host {host}")

    if host_details[0] != "token":
        raise DatabricksConfigError(f'From netrc: Unsupported login parameter "{host_details[0]}". Use "token"')

    return {"host": host, "token": host_details[2]}


def _get_auth_from_env(host: str = None) -> dict:
    if not host:
        try:
            host = os.environ["DATABRICKS_HOST"]
        except KeyError:
            raise DatabricksConfigError("From env: DATABRICKS_HOST not found")
    try:
        token = os.environ["DATABRICKS_TOKEN"]
    except KeyError:
        raise DatabricksConfigError("From env: DATABRICKS_TOKEN not found")

    return {"host": host, "token": token}


def get_host_and_token(
    profile: str = None, host: str = None, dbcfg_path: str = None, netrc_path: str = None
) -> Tuple[str, str]:
    """Gets the host and token from one of three sources:

        - .databrickscfg file in the home directory or provided dbcfg_path (profile parameter required)
        - .netrc file in the home directory or provided netrc_path (host parameter required)
        - environment variables DATABRICKS_TOKEN and DATABRICKS_HOST (host can be manually provided)

    :param profile:
        Databricks configuration profile name
    :param host:
        Databricks host.
        Examples: https://adb-1234567891234567.8.azuredatabricks.net or adb-1234567891234567.8.azuredatabricks.net
    :param dbcfg_path:
        Databricks config file path (default ~/.databrickscfg)
    :param netrc_path:
        netrc file path (default ~/.netrc)
    """
    errors = []
    auth_info = None
    if not dbcfg_path:
        dbcfg_path = str(Path.home() / ".databrickscfg")
    if not netrc_path:
        netrc_path = str(Path.home() / ".netrc")

    # First try to read from databrickscfg
    try:
        auth_info = _get_auth_from_cfg(dbcfg_path, profile)
    except DatabricksConfigError as exc:
        errors.append(exc)

    # If databrickscfg failed, try netrc file
    if not auth_info:
        try:
            auth_info = _get_auth_from_netrc(netrc_path, host)
        except DatabricksConfigError as exc:
            errors.append(exc)

    # If both databrickscfg and netrc file failed, try environment variables
    if not auth_info:
        try:
            auth_info = _get_auth_from_env(host=host)
        except DatabricksConfigError as exc:
            errors.append(exc)

    # If all attempts failed, raise exception and list reasons for failure
    if not auth_info:
        fail_reasons = "\n".join([str(e) for e in errors])
        raise DatabricksConfigError(
            f"Unable to get both host and token from supported sources. Reasons:\n{fail_reasons}"
        )

    return auth_info["host"], auth_info["token"]
