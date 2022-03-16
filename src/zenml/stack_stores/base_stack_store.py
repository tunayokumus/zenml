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
from abc import ABC, abstractmethod
from typing import Dict, List, Tuple

import yaml

from zenml.enums import StackComponentType
from zenml.exceptions import StackComponentExistsError, StackExistsError
from zenml.logger import get_logger
from zenml.stack_stores.models import (
    StackComponentConfiguration,
    StackConfiguration,
    StackWrapper,
)

logger = get_logger(__name__)


class BaseStackStore(ABC):
    """Base class for accessing data in ZenML Repository and new Service."""

    # Public Interface:

    @property
    @abstractmethod
    def url(self) -> str:
        """Get the repository URL."""

    @staticmethod
    @abstractmethod
    def get_local_url(path: str) -> str:
        """Get a local URL for a given local path."""

    @staticmethod
    @abstractmethod
    def is_valid_url(url: str) -> bool:
        """Check if the given url is valid."""

    @property
    @abstractmethod
    def active_stack_name(self) -> str:
        """The name of the active stack for this repository."""

    @abstractmethod
    def activate_stack(self, name: str) -> None:
        """Activates the stack for the given name."""

    @abstractmethod
    def get_stack_configuration(self, name: str) -> StackConfiguration:
        """Fetches a stack configuration."""

    @property
    @abstractmethod
    def stack_configurations(self) -> Dict[str, StackConfiguration]:
        """Configuration for all stacks registered in this repository."""

    @abstractmethod
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

    # Private interface (must be implemented, not to be called by user):

    @abstractmethod
    def _create_stack(
        self, name: str, stack_configuration: StackConfiguration
    ) -> None:
        """Add a stack to storage.

        Args:
            name: The name to save the stack as.
            stack_configuration: StackConfiguration to persist.
        """

    @abstractmethod
    def _delete_stack(self, name: str) -> None:
        """Delete a stack from storage.

        Args:
            name: The name of the stack to be deleted.
        """

    @abstractmethod
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

    @abstractmethod
    def _get_stack_component_names(
        self, component_type: StackComponentType
    ) -> List[str]:
        """Get names of all registered stack components of a given type."""

    @abstractmethod
    def _delete_stack_component(
        self, component_type: StackComponentType, name: str
    ) -> None:
        """Remove a StackComponent from storage."""

    # Common code (user facing):

    @property
    def stacks(self) -> List[StackWrapper]:
        """All stacks registered in this repository."""
        return [
            self._stack_from_configuration(name, conf)
            for name, conf in self.stack_configurations.items()
        ]

    def get_stack(self, name: str) -> StackWrapper:
        """Fetches a stack."""
        return self._stack_from_configuration(
            name, self.get_stack_configuration(name)
        )

    def register_stack(self, stack: StackWrapper) -> Dict[str, str]:
        """Registers a stack and its components.

        If any of the stacks' components aren't registered in the repository
        yet, this method will try to register them as well.

        Args:
            stack: The stack to register.

        Raises:
            StackExistsError: If a stack with the same name already exists.
            StackComponentExistsError: If a component of the stack wasn't
                registered and a different component with the same name
                already exists.
        """
        try:
            self.get_stack(stack.name)
        except KeyError:
            pass
        else:
            raise StackExistsError(
                f"Unable to register stack with name '{stack.name}': Found "
                f"existing stack with this name."
            )

        def __check_component(
            component: StackComponentConfiguration,
        ) -> Tuple[StackComponentType, str]:
            try:
                existing_component = self.get_stack_component(
                    component_type=component.type, name=component.name
                )
                if existing_component.uuid != component.uuid:
                    raise StackComponentExistsError(
                        f"Unable to register one of the stacks components: "
                        f"A component of type '{component.type}' and name "
                        f"'{component.name}' already exists."
                    )
            except KeyError:
                self.register_stack_component(component)
            return component.type, component.name

        components = {
            t.value: n for t, n in map(__check_component, stack.components)
        }
        metadata = {c.type.value: c.flavor for c in stack.components}
        stack_configuration = StackConfiguration(**components)
        self._create_stack(stack.name, stack_configuration)
        return metadata

    def deregister_stack(self, name: str) -> None:
        """Deregisters a stack.

        Args:
            name: The name of the stack to deregister.

        Raises:
            ValueError: If the stack is the currently active stack for this
                repository.
        """
        if name == self.active_stack_name:
            raise ValueError(f"Unable to deregister active stack '{name}'.")
        self._delete_stack(name)

    def get_stack_component(
        self, component_type: StackComponentType, name: str
    ) -> StackComponentConfiguration:
        """Fetches a registered stack component.
        Raises:
            KeyError: If no component with the requested type and name exists.
        """
        flavor, config = self._get_component_flavor_and_config(
            component_type, name=name
        )
        uuid = yaml.safe_load(base64.b64decode(config).decode())["uuid"]
        return StackComponentConfiguration(
            type=component_type,
            flavor=flavor,
            name=name,
            uuid=uuid,
            config=config,
        )

    def get_stack_components(
        self, component_type: StackComponentType
    ) -> List[StackComponentConfiguration]:
        """Fetches all registered stack components of the given type."""
        return [
            self.get_stack_component(component_type=component_type, name=name)
            for name in self._get_stack_component_names(component_type)
        ]

    def deregister_stack_component(
        self, component_type: StackComponentType, name: str
    ) -> None:
        """Deregisters a stack component.

        Args:
            component_type: The type of the component to deregister.
            name: The name of the component to deregister.
        """
        for stack_name, stack_config in self.stack_configurations.items():
            if stack_config.contains_component(
                component_type=component_type, name=name
            ):
                raise ValueError(
                    f"Unable to deregister stack component (type: "
                    f"{component_type}, name: {name}) that is part of a "
                    f"registered stack (stack name: '{stack_name}')."
                )
        self._delete_stack_component(component_type, name=name)

    # Common code (internal implementations, private):

    def _stack_from_configuration(
        self, name: str, stack_configuration: StackConfiguration
    ) -> StackWrapper:
        """Build a StackWrapper from stored configurations"""
        stack_components = [
            self.get_stack_component(
                component_type=StackComponentType(component_type_name),
                name=component_name,
            )
            for component_type_name, component_name in stack_configuration.dict().items()
            if component_name
        ]
        return StackWrapper(name=name, components=stack_components)
