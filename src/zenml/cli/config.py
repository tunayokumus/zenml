#  Copyright (c) ZenML GmbH 2021. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""CLI for manipulating ZenML local and global config file."""
from email.policy import default
from operator import xor
from typing import TYPE_CHECKING, Optional

import click

from zenml.cli import utils as cli_utils
from zenml.cli.cli import cli
from zenml.config.global_config import ConfigProfile, GlobalConfig
from zenml.console import console
from zenml.enums import LoggingLevels, StoreType
from zenml.repository import Repository
from zenml.utils.analytics_utils import AnalyticsEvent, track_event

if TYPE_CHECKING:
    pass


# Analytics
@cli.group()
def analytics() -> None:
    """Analytics for opt-in and opt-out"""


@analytics.command("get")
def is_analytics_opted_in() -> None:
    """Check whether user is opt-in or opt-out of analytics."""
    gc = GlobalConfig()
    cli_utils.declare(f"Analytics opt-in: {gc.analytics_opt_in}")


@analytics.command("opt-in", context_settings=dict(ignore_unknown_options=True))
def opt_in() -> None:
    """Opt-in to analytics"""
    gc = GlobalConfig()
    gc.analytics_opt_in = True
    cli_utils.declare("Opted in to analytics.")
    track_event(AnalyticsEvent.OPT_IN_ANALYTICS)


@analytics.command(
    "opt-out", context_settings=dict(ignore_unknown_options=True)
)
def opt_out() -> None:
    """Opt-out to analytics"""
    gc = GlobalConfig()
    gc.analytics_opt_in = False
    cli_utils.declare("Opted out of analytics.")
    track_event(AnalyticsEvent.OPT_OUT_ANALYTICS)


# Logging
@cli.group()
def logging() -> None:
    """Configuration of logging for ZenML pipelines."""


# Setting logging
@logging.command("set-verbosity")
@click.argument(
    "verbosity",
    type=click.Choice(
        list(map(lambda x: x.name, LoggingLevels)), case_sensitive=False
    ),
)
def set_logging_verbosity(verbosity: str) -> None:
    """Set logging level"""
    # TODO [ENG-150]: Implement this.
    verbosity = verbosity.upper()
    if verbosity not in LoggingLevels.__members__:
        raise KeyError(
            f"Verbosity must be one of {list(LoggingLevels.__members__.keys())}"
        )
    cli_utils.declare(f"Set verbosity to: {verbosity}")


# Profiles
@cli.group()
def profile() -> None:
    """Configuration of ZenML profiles."""


@profile.command("create")
@click.argument(
    "name",
    type=str,
    required=True,
)
@click.option(
    "--url",
    "-u",
    "url",
    help="The service URL to use for the profile.",
    required=False,
    type=str,
)
@click.option(
    "--store-type",
    "-t",
    "store_type",
    help="The store type to use for the profile.",
    required=False,
    type=click.Choice(list(StoreType)),
    default=StoreType.LOCAL,
)
def create_profile_command(
    name: str, url: Optional[str], store_type: Optional[StoreType]
) -> None:
    """Creates a new configuration profile."""

    cfg = GlobalConfig()
    if cfg.get_profile(name):
        cli_utils.error(f"Profile {name} already exists.")
        return
    cfg.add_or_update_profile(
        ConfigProfile(name=name, service_url=url, store_type=store_type)
    )
    cli_utils.declare(f"Profile `{name}` successfully created.")


@profile.command("list")
def list_profiles_command() -> None:
    """List configuration profiles."""
    profiles = GlobalConfig().profiles

    if len(profiles) == 0:
        cli_utils.warning("No profiles configured!")
        return

    active_profile_name = GlobalConfig().active_profile_name

    profile_dicts = []
    for profile_name, profile in profiles.items():
        is_active = profile_name == active_profile_name
        profile_config = {
            "ACTIVE": ":point_right:" if is_active else "",
            "PROFILE NAME": profile_name,
            "STORE TYPE": profile.store_type.value,
            "URL": profile.service_url,
        }
        profile_dicts.append(profile_config)

    cli_utils.print_table(profile_dicts)


@profile.command(
    "describe",
    help="Show details about the active profile.",
)
@click.argument(
    "name",
    type=click.STRING,
    required=False,
)
def describe_profile(name: Optional[str]) -> None:
    """Show details about the active profile."""
    cfg = GlobalConfig()
    name = name or cfg.active_profile_name
    if len(cfg.profiles) == 0:
        cli_utils.warning("No profiles registered!")
        return

    profile = cfg.get_profile(name)
    if not profile:
        cli_utils.error(f"Profile `{name}` does not exist.")
        return

    cli_utils.print_profile(
        profile,
        active=name == cfg.active_profile_name,
        name=name,
    )


@profile.command("delete")
@click.argument("name", type=str)
def delete_profile(name: str) -> None:
    """Delete a profile."""
    if not GlobalConfig().get_profile(name):
        cli_utils.error(f"Profile {name} already exists.")
        return

    with console.status(f"Deleting profile `{name}`...\n"):
        GlobalConfig().delete_profile(name)
        cli_utils.declare(f"Deleted profile {name}.")


@profile.command("set")
@click.argument("name", type=str)
def set_active_profile(name: str) -> None:
    """Set a profile as active."""
    cfg = GlobalConfig()
    current_profile_name = cfg.active_profile_name
    if current_profile_name == name:
        cli_utils.declare(f"Profile `{name}` is already active.")
        return
    with console.status(f"Setting the active profile to `{name}`..."):
        cfg.activate_profile(name)
        try:
            # attempt loading the repository to make sure that the profile
            # configuration is valid
            Repository()
        except Exception as e:
            cli_utils.error(
                f"Error activating profile: {e}. "
                f"Keeping current profile: {current_profile_name}."
            )
            cfg.activate_profile(current_profile_name)
            raise
        cli_utils.declare(f"Active profile: {name}")


@profile.command("get")
def get_active_profile() -> None:
    """Get the active profile."""
    with console.status("Getting the active profile..."):
        cli_utils.declare(
            f"Active profile is: {GlobalConfig().active_profile_name}"
        )
