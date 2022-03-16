#  Copyright (c) ZenML GmbH 2022. All Rights Reserved.
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
import base64
import json
import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from zenml.enums import StackComponentType
from zenml.exceptions import StackComponentExistsError
from zenml.io import fileio
from zenml.io.utils import (
    read_file_contents_as_string,
    write_file_contents_as_string,
)
from zenml.logger import get_logger
from zenml.stack_stores import BaseStackStore
from zenml.stack_stores.models import (
    StackComponentConfiguration,
    StackConfiguration,
    StackStoreModel,
)
from zenml.utils import yaml_utils

logger = get_logger(__name__)

REPOSITORY_DIRECTORY_NAME = ".repo"


class LocalStackStore(BaseStackStore):
    def __init__(
        self,
        url: str,
        stack_data: Optional[StackStoreModel] = None,
    ) -> None:
        """Initializes a local stack store instance.

        Args:
            url: URL of local directory of the repository to use for
                stack storage.
            stack_data: optional stack data store object to pre-populate the
                stack store with.
        """
        self._url = url
        self._root = self.get_path_from_url(url)

        if stack_data is not None:
            self.__store = stack_data
        elif fileio.file_exists(self._store_path()):
            config_dict = yaml_utils.read_yaml(self._store_path())
            self.__store = StackStoreModel.parse_obj(config_dict)
        else:
            self.__store = StackStoreModel.empty_store()

    @staticmethod
    def get_path_from_url(url: str) -> Optional[Path]:
        """Get the path from a URL.

        Args:
            url: The URL to get the path from.

        Returns:
            The path from the URL.
        """
        if not LocalStackStore.is_valid_url(url):
            raise ValueError(f"Invalid URL for local store: {url}")
        url = url.replace("file://", "")
        return Path(url)

    # Public interface implementations:

    @property
    def url(self) -> str:
        """URL of the repository."""
        return self._url

    @staticmethod
    def get_local_url(path: str) -> str:
        """Get a local URL for a given local path."""
        return f"file://{path}"

    @staticmethod
    def is_valid_url(url: str) -> bool:
        """Check if the given url is a valid local path."""
        url = url.replace("file://", "")
        path = Path(url)
        return path.exists() and path.is_dir()

    @property
    def active_stack_name(self) -> str:
        """The name of the active stack for this repository.

        Raises:
            RuntimeError: If no active stack name is configured.
        """
        if not self.__store.active_stack_name:
            raise RuntimeError(
                "No active stack name configured. Run "
                "`zenml stack set STACK_NAME` to update the active stack."
            )
        return self.__store.active_stack_name

    def activate_stack(self, name: str) -> None:
        """Activates the stack for the given name.

        Args:
            name: Name of the stack to activate.

        Raises:
            KeyError: If no stack exists for the given name.
        """
        if name not in self.__store.stacks:
            raise KeyError(f"Unable to find stack for name '{name}'.")

        self.__store.active_stack_name = name
        self._write_store()

    def get_stack_configuration(self, name: str) -> StackConfiguration:
        """Fetches a stack.

        Args:
            name: The name of the stack to fetch.

        Raises:
            KeyError: If no stack exists for the given name.
        """
        logger.debug("Fetching stack with name '%s'.", name)
        if name not in self.__store.stacks:
            raise KeyError(
                f"Unable to find stack with name '{name}'. Available names: "
                f"{set(self.__store.stacks)}."
            )

        return self.__store.stacks[name]

    @property
    def stack_configurations(self) -> Dict[str, StackConfiguration]:
        """Configuration for all stacks registered in this repository."""
        return self.__store.stacks.copy()

    def register_stack_component(
        self,
        component: StackComponentConfiguration,
    ) -> None:
        """Register a stack component.

        Args:
            component: The component to register.

        Raises:
            StackComponentExistsError: If a stack component with the same type
                and name already exists.
        """
        components = self.__store.stack_components[component.type]
        if component.name in components:
            raise StackComponentExistsError(
                f"Unable to register stack component (type: {component.type}) "
                f"with name '{component.name}': Found existing stack component "
                f"with this name."
            )

        # write the component configuration file
        component_config_path = self._get_stack_component_config_path(
            component_type=component.type, name=component.name
        )
        fileio.create_dir_recursive_if_not_exists(
            os.path.dirname(component_config_path)
        )
        write_file_contents_as_string(
            component_config_path,
            json.loads(base64.b64decode(component.config).decode()),
        )

        # add the component to the repository configuration and write it to disk
        components[component.name] = component.flavor
        self._write_store()
        logger.info(
            "Registered stack component with name '%s'.", component.name
        )

    # Private interface implementations:

    def _create_stack(
        self, name: str, stack_configuration: StackConfiguration
    ) -> None:
        """Add a stack to storage.

        Args:
            name: The name to save the stack as.
            stack_configuration: StackConfiguration to persist.
        """
        self.__store.stacks[name] = stack_configuration
        self._write_store()
        logger.info("Registered stack with name '%s'.", name)

    def _delete_stack(self, name: str) -> None:
        """Delete a stack from storage.

        Args:
            name: The name of the stack to be deleted.
        """
        try:
            del self.__store.stacks[name]
            self._write_store()
            logger.info("Deregistered stack with name '%s'.", name)
        except KeyError:
            logger.warning(
                "Unable to deregister stack with name '%s': No stack exists "
                "with this name.",
                name,
            )

    def _get_component_flavor_and_config(
        self, component_type: StackComponentType, name: str
    ) -> Tuple[str, bytes]:
        """Fetch the flavor and configuration for a stack component.

        Args:
            component_type: The type of the component to fetch.
            name: The name of the component to fetch.

        Raises:
            KeyError: If no stack component exists for the given type and name.
        """
        components: Dict[str, str] = self.__store.stack_components[
            component_type
        ]
        if name not in components:
            raise KeyError(
                f"Unable to find stack component (type: {component_type}) "
                f"with name '{name}'. Available names: {set(components)}."
            )

        component_config_path = self._get_stack_component_config_path(
            component_type=component_type, name=name
        )
        flavor = components[name]
        config = base64.b64encode(
            read_file_contents_as_string(component_config_path).encode()
        )
        return flavor, config

    def _get_stack_component_names(
        self, component_type: StackComponentType
    ) -> List[str]:
        """Get names of all registered stack components of a given type."""
        return list(self.__store.stack_components[component_type])

    def _delete_stack_component(
        self, component_type: StackComponentType, name: str
    ) -> None:
        """Remove a StackComponent from storage."""
        components = self.__store.stack_components[component_type]
        try:
            del components[name]
            self._write_store()
            logger.info(
                "Deregistered stack component (type: %s) with name '%s'.",
                component_type.value,
                name,
            )
        except KeyError:
            logger.warning(
                "Unable to deregister stack component (type: %s) with name "
                "'%s': No stack component exists with this name.",
                component_type.value,
                name,
            )
        component_config_path = self._get_stack_component_config_path(
            component_type=component_type, name=name
        )

        if fileio.file_exists(component_config_path):
            fileio.remove(component_config_path)

    # Implementation-specific internal methods:

    def _get_stack_component_config_path(
        self, component_type: StackComponentType, name: str
    ) -> str:
        """Path to the configuration file of a stack component."""
        path = self._root / component_type.plural / f"{name}.yaml"
        return str(path)

    def _store_path(self) -> str:
        """Path to the repository configuration file."""
        return str(self._root / "config.yaml")

    def _write_store(self) -> None:
        """Writes the repository configuration file."""
        config_dict = json.loads(self.__store.json())
        yaml_utils.write_yaml(self._store_path(), config_dict)
