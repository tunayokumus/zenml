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
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Type, cast

import yaml

from zenml.config.global_config import GlobalConfig, ConfigProfile
from zenml.constants import (
    ENV_ZENML_REPOSITORY_PATH,
    LOCAL_LEGACY_CONFIG_DIRECTORY_NAME,
)
from zenml.enums import StackComponentFlavor, StackComponentType, StoreType
from zenml.environment import Environment
from zenml.exceptions import ForbiddenRepositoryAccessError
from zenml.io import fileio
from zenml.logger import get_logger
from zenml.post_execution import PipelineView
from zenml.stack import Stack, StackComponent
from zenml.stack_stores import BaseStackStore, LocalStackStore, SqlStackStore
from zenml.stack_stores.models import (
    StackComponentConfiguration,
    StackConfiguration,
    StackWrapper,
)
from zenml.utils.analytics_utils import AnalyticsEvent, track, track_event

logger = get_logger(__name__)


class RepositoryMetaClass(type):
    """Repository singleton metaclass.

    This metaclass is used to enforce a singleton instance of the Repository
    class with the following additional properties:

    * the singleton Repository instance is created on first access to reflect
    the currently active global configuration profile.
    * the global Repository is initialized automatically on import with the
    default configuration (local stack) if no stack is configured yet.
    * the Repository mustn't be accessed from within pipeline steps

    """

    def __init__(cls, *args: Any, **kwargs: Any) -> None:
        """Initialize the Repository class."""
        super().__init__(*args, **kwargs)
        cls.__global_repository: Optional["Repository"] = None

    def __call__(cls, *args: Any, **kwargs: Any) -> "Repository":
        """Create or return the global Repository instance.

        Raises:
            ForbiddenRepositoryAccessError: If trying to create a `Repository`
                instance while a ZenML step is being executed.
        """
        if Environment().step_is_running:
            raise ForbiddenRepositoryAccessError(
                "Unable to access repository during step execution. If you "
                "require access to the artifact or metadata store, please use "
                "a `StepContext` inside your step instead.",
                url="https://docs.zenml.io/features/step-fixtures#using-the-stepcontext",
            )

        if not cls.__global_repository:
            cls.__global_repository = cast(
                "Repository", super().__call__(*args, **kwargs)
            )
            # Initialize the global repository with the default stack
            # configuration if no stack has been configured yet
            if cls.__global_repository.is_empty:
                cls.__global_repository._initialize()

        return cls.__global_repository


class Repository(metaclass=RepositoryMetaClass):
    """ZenML repository class.

    The ZenML repository manages configuration options for ZenML stacks as well
    as their components.
    """

    def __init__(self):
        """Initializes the global repository instance."""

        legacy_location = Repository._find_legacy_repository()
        if legacy_location:
            logger.warning(
                "A ZenML repository folder was found at %s. \n"
                "Support for legacy .zen folders has been deprecated and will "
                "be removed in a future ZenML release. If you still need to "
                "use the stacks configured in this repository, please add them "
                "manually to the global ZenML repository, then delete the .zen "
                "folder.",
                legacy_location,
            )

        self.stack_store: BaseStackStore = self.create_store(
            GlobalConfig().active_profile
        )

    @staticmethod
    def get_store_class(type: StoreType) -> Type[BaseStackStore]:
        """Returns the class of the given store type."""
        return {
            StoreType.LOCAL: LocalStackStore,
            StoreType.SQL: SqlStackStore,
        }[type]

    @staticmethod
    def create_store(profile: ConfigProfile) -> BaseStackStore:
        """Create the repository persistance back-end store from a configuration
        profile.

        If the configuration profile doesn't specify all necessary configuration
        options (e.g. the type or URL), a default configuration will be used.

        Args:
            profile: The configuration profile to use for persisting the
                repository information.

        Returns:
            The initialized repository store.
        """
        store_class = Repository.get_store_class(profile.store_type)

        if not profile.service_url:
            profile.service_url = store_class.get_local_url(profile.config_path)

        if store_class.is_valid_url(profile.service_url):
            return store_class(profile.service_url)

        raise ValueError(
            f"Invalid URL for store type `{profile.store_type.value}`: "
            f"{profile.service_url}"
        )

    @track(event=AnalyticsEvent.INITIALIZE_REPO)
    def _initialize(self) -> None:
        """Initializes the global ZenML repository.

        The newly created repository will contain a single stack with a local
        orchestrator, a local artifact store and a local SQLite metadata store.
        """
        logger.debug("Initializing repository...")

        # register and activate a local stack
        stack = Stack.default_local_stack()
        self.register_stack(stack)
        self.activate_stack(stack.name)

    @property
    def is_empty(self) -> bool:
        """Check if the repository is empty."""
        return not self.stacks

    @property
    def version(self) -> str:
        """The version of the repository."""
        return self.__config.version

    @property
    def stacks(self) -> List[Stack]:
        """All stacks registered in this repository."""
        return [self._stack_from_wrapper(s) for s in self.stack_store.stacks]

    @property
    def stack_configurations(self) -> Dict[str, StackConfiguration]:
        """Configuration objects for all stacks registered in this repository.

        This property is intended as a quick way to get information about the
        components of the registered stacks without loading all installed
        integrations. The contained stack configurations might be invalid if
        they were modified by hand, to ensure you get valid stacks use
        `repo.stacks()` instead.

        Modifying the contents of the returned dictionary does not actually
        register/deregister stacks, use `repo.register_stack(...)` or
        `repo.deregister_stack(...)` instead.
        """
        return self.stack_store.stack_configurations

    @property
    def active_stack(self) -> Stack:
        """The active stack for this repository.

        Raises:
            RuntimeError: If no active stack name is configured.
            KeyError: If no stack was found for the configured name or one
                of the stack components is not registered.
        """
        return self.get_stack(name=self.active_stack_name)

    @property
    def active_stack_name(self) -> str:
        """The name of the active stack for this repository.

        Raises:
            RuntimeError: If no active stack name is configured.
        """
        return self.stack_store.active_stack_name

    @track(event=AnalyticsEvent.SET_STACK)
    def activate_stack(self, name: str) -> None:
        """Activates the stack for the given name.

        Args:
            name: Name of the stack to activate.

        Raises:
            KeyError: If no stack exists for the given name.
        """
        self.stack_store.activate_stack(name)

    def get_stack(self, name: str) -> Stack:
        """Fetches a stack.

        Args:
            name: The name of the stack to fetch.

        Raises:
            KeyError: If no stack exists for the given name or one of the
                stacks components is not registered.
        """
        return self._stack_from_wrapper(self.stack_store.get_stack(name))

    def register_stack(self, stack: Stack) -> None:
        """Registers a stack and it's components.

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
        metadata = self.stack_store.register_stack(
            StackWrapper.from_stack(stack)
        )
        track_event(AnalyticsEvent.REGISTERED_STACK, metadata=metadata)

    def deregister_stack(self, name: str) -> None:
        """Deregisters a stack.

        Args:
            name: The name of the stack to deregister.

        Raises:
            ValueError: If the stack is the currently active stack for this
                repository.
        """
        self.stack_store.deregister_stack(name)

    def get_stack_components(
        self, component_type: StackComponentType
    ) -> List[StackComponent]:
        """Fetches all registered stack components of the given type."""
        return [
            self._component_from_configuration(c)
            for c in self.stack_store.get_stack_components(component_type)
        ]

    def get_stack_component(
        self, component_type: StackComponentType, name: str
    ) -> StackComponent:
        """Fetches a registered stack component.

        Args:
            component_type: The type of the component to fetch.
            name: The name of the component to fetch.

        Raises:
            KeyError: If no stack component exists for the given type and name.
        """
        logger.debug(
            "Fetching stack component of type '%s' with name '%s'.",
            component_type.value,
            name,
        )
        return self._component_from_configuration(
            self.stack_store.get_stack_component(component_type, name=name)
        )

    def register_stack_component(
        self,
        component: StackComponent,
    ) -> None:
        """Registers a stack component.

        Args:
            component: The component to register.

        Raises:
            StackComponentExistsError: If a stack component with the same type
                and name already exists.
        """
        self.stack_store.register_stack_component(
            StackComponentConfiguration.from_component(component)
        )
        analytics_metadata = {
            "type": component.type.value,
            "flavor": component.flavor.value,
        }
        track_event(
            AnalyticsEvent.REGISTERED_STACK_COMPONENT,
            metadata=analytics_metadata,
        )

    def deregister_stack_component(
        self, component_type: StackComponentType, name: str
    ) -> None:
        """Deregisters a stack component.

        Args:
            component_type: The type of the component to deregister.
            name: The name of the component to deregister.
        """
        self.stack_store.deregister_stack_component(component_type, name=name)

    @track(event=AnalyticsEvent.GET_PIPELINES)
    def get_pipelines(
        self, stack_name: Optional[str] = None
    ) -> List[PipelineView]:
        """Fetches post-execution pipeline views.

        Args:
            stack_name: If specified, pipelines in the metadata store of the
                given stack are returned. Otherwise pipelines in the metadata
                store of the currently active stack are returned.

        Returns:
            A list of post-execution pipeline views.

        Raises:
            KeyError: If no stack with the given name exists.
        """
        stack_name = stack_name or self.active_stack_name
        metadata_store = self.get_stack(stack_name).metadata_store
        return metadata_store.get_pipelines()

    @track(event=AnalyticsEvent.GET_PIPELINE)
    def get_pipeline(
        self, pipeline_name: str, stack_name: Optional[str] = None
    ) -> Optional[PipelineView]:
        """Fetches a post-execution pipeline view.

        Args:
            pipeline_name: Name of the pipeline.
            stack_name: If specified, pipelines in the metadata store of the
                given stack are returned. Otherwise pipelines in the metadata
                store of the currently active stack are returned.

        Returns:
            A post-execution pipeline view for the given name or `None` if
            it doesn't exist.

        Raises:
            KeyError: If no stack with the given name exists.
        """
        stack_name = stack_name or self.active_stack_name
        metadata_store = self.get_stack(stack_name).metadata_store
        return metadata_store.get_pipeline(pipeline_name)

    @staticmethod
    def _is_legacy_repository_directory(path: Path) -> bool:
        """Checks whether a legacy ZenML repository exists at the given path."""
        config_dir = path / LOCAL_LEGACY_CONFIG_DIRECTORY_NAME
        return fileio.is_dir(str(config_dir))

    @staticmethod
    def _find_legacy_repository() -> Optional[Path]:
        """Search for a legacy ZenML repository directory.

        Tthis function tries to find the repository using the
        environment variable `ZENML_REPOSITORY_PATH` (if set) and
        recursively searching in the parent directories of the current
        working directory.

        Returns:
            Absolute path to a legacy ZenML repository directory or None if no
            legacy repository was found.
        """
        # try to get path from the environment variable
        env_var_path = os.getenv(ENV_ZENML_REPOSITORY_PATH)
        if env_var_path:
            path = Path(env_var_path)
            # explicit path via environment variable, don't search
            # parent directories
            search_parent_directories = False
        else:
            # try to find the repo in the parent directories of the current
            # working directory
            path = Path.cwd()
            search_parent_directories = True

        def _find_repo_helper(path_: Path) -> Optional[Path]:
            """Helper function to recursively search parent directories for a
            ZenML repository."""
            if Repository._is_legacy_repository_directory(path_):
                return path_

            if not search_parent_directories or fileio.is_root(str(path_)):
                return None

            return _find_repo_helper(path_.parent)

        legacy_repo_path = _find_repo_helper(path)

        if legacy_repo_path:
            return legacy_repo_path.resolve()

        return None

    def _component_from_configuration(
        self, conf: StackComponentConfiguration
    ) -> StackComponent:
        """Instantiate a StackComponent from the Configuration."""
        from zenml.stack.stack_component_class_registry import (
            StackComponentClassRegistry,
        )

        flavor = StackComponentFlavor.for_type(conf.type)(conf.flavor)
        component_class = StackComponentClassRegistry.get_class(
            component_type=conf.type, component_flavor=flavor
        )
        component_config = yaml.safe_load(
            base64.b64decode(conf.config).decode()
        )
        return component_class.parse_obj(component_config)

    def _stack_from_wrapper(self, wrapper: StackWrapper) -> Stack:
        """Instantiate a Stack from the serializable Wrapper."""
        stack_components = {}
        for component_config in wrapper.components:
            component_type = component_config.type
            component = self._component_from_configuration(component_config)
            stack_components[component_type] = component

        return Stack.from_components(
            name=wrapper.name, components=stack_components
        )
