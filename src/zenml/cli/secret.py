#  Copyright (c) ZenML GmbH 2020. All Rights Reserved.
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

import click

from zenml.cli.cli import cli
from zenml.cli.utils import confirmation
from zenml.console import console
from zenml.repository import Repository
from zenml.stack.stack_component import StackComponent


# Secrets
@cli.group()
def secret() -> None:
    """Secrets for storing key-value pairs for use in authentication."""


@secret.command("create")
@click.argument("name", type=str)
@click.option(
    "--secret",
    "-s",
    "secret_value",
    help="The secret to create.",
    required=True,
    type=str,
)
def create_secret(name: str, secret_value: str) -> None:
    """Create a secret."""
    with console.status(f"Creating secret `{name}`..."):
        active_stack = Repository().active_stack
        secrets_manager = active_stack.components.get(
            StackComponent.SECRETS_MANAGER
        )
        secrets_manager.create_secret(name, secret_value)
        console.print(f"Secret `{name.upper()}` created.")


@secret.command("get")
@click.argument("name", type=str)
def get_secret(name: str) -> None:
    """Get a secret, given its name."""
    with console.status(f"Getting secret `{name}`..."):
        active_stack = Repository().active_stack
        secrets_manager = active_stack.components.get(
            StackComponent.SECRETS_MANAGER
        )
        secret_value = secrets_manager.get_secret_by_key(name)
        console.print(f"Secret for `{name.upper()}` is `{secret_value}`.")


@secret.command("delete")
@click.argument("name", type=str)
def delete_secret(name: str) -> None:
    """Delete a secret, given its name."""
    confirmation_response = confirmation(
        f"This will delete the secret associated with `{name}`. "
        "Are you sure you want to proceed?"
    )
    if not confirmation_response:
        console.print("Aborting secret deletion...")
    else:
        with console.status(f"Deleting secret `{name}`..."):
            active_stack = Repository().active_stack
            secrets_manager = active_stack.components.get(
                StackComponent.SECRETS_MANAGER
            )
            secrets_manager.delete_secret_by_key(name)
            console.print(f"Deleted secret for `{name.upper()}`.")


@secret.command("update")
@click.argument("name", type=str)
@click.option(
    "--secret",
    "-s",
    "secret_value",
    help="The new secret value to update for a specific name.",
    required=True,
    type=str,
)
def update_secret(name: str, secret_value: str) -> None:
    """Update a secret."""
    with console.status(f"Updating secret `{name}`..."):
        active_stack = Repository().active_stack
        secrets_manager = active_stack.components.get(
            StackComponent.SECRETS_MANAGER
        )
        secrets_manager.update_secret_by_key(name, secret_value)
        console.print(f"Secret `{name.upper()}` updated.")
