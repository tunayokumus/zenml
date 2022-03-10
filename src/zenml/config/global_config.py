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
import json
import os
import shutil
import uuid
from typing import Any, ClassVar, Dict, Optional, cast

from pydantic import BaseModel, Field, ValidationError, validator
from pydantic.main import ModelMetaclass
from semver import VersionInfo  # type: ignore [import]

from zenml import __version__
from zenml.enums import StoreType
from zenml.io import fileio
from zenml.io.utils import get_global_config_directory
from zenml.logger import get_logger
from zenml.utils import yaml_utils

logger = get_logger(__name__)


LEGACY_CONFIG_FILE_NAME = ".zenglobal.json"
DEFAULT_PROFILE_NAME = "default"
CONFIG_ENV_VAR_PREFIX = "ZENML_"


class GlobalConfigMetaClass(ModelMetaclass):
    """Global configuration metaclass.

    This metaclass is used to enforce a singleton instance of the GlobalConfig
    class with the following additional properties:

    * the GlobalConfig is initialized automatically on import with the
    default configuration, if no config file exists yet.
    * an empty default profile is added to the global config on initialization
    if no other profiles are configured yet.
    * the GlobalConfig undergoes a schema migration if the version of the
    config file is older than the current version of the ZenML package.
    """

    def __init__(cls, *args: Any, **kwargs: Any) -> None:
        """Initialize a singleton class."""
        super().__init__(*args, **kwargs)
        cls.__global_config: Optional["GlobalConfig"] = None

    def __call__(cls, *args: Any, **kwargs: Any) -> "GlobalConfig":
        """Create or return the default global config instance.

        If the GlobalConfig constructor is called with a custom config file
        path, a new GlobalConfig instance is created regardless of whether
        the global config instance already exists or not.
        """
        if args or kwargs:
            return cast("GlobalConfig", super().__call__(*args, **kwargs))

        if not cls.__global_config:
            cls.__global_config = cast(
                "GlobalConfig", super().__call__(*args, **kwargs)
            )
            cls.__global_config._migrate_config()
            cls.__global_config._add_and_activate_default_profile()

        return cls.__global_config


class ConfigProfile(BaseModel):
    """Stores configuration profile options.

    Attributes:
        name: Name of the profile.
        service_url: URL pointing to the ZenML service backend
        _config: global configuration to which this profile belongs.
    """

    name: str
    service_url: Optional[str] = None
    store_type: Optional[StoreType] = StoreType.LOCAL
    _config: "GlobalConfig"

    def __init__(
        self, config: Optional["GlobalConfig"] = None, **kwargs
    ) -> None:
        """Initializes a GlobalConfig object using values from the config file.

        If the config file doesn't exist yet, we try to read values from the
        legacy (ZenML version < 0.6) config file.

        Args:
            config: global configuration to which this profile belongs. When not
                specified, the default global configuration path is used.
            **kwargs: additional keyword arguments are passed to the
                BaseModel constructor.
        """
        self._config = config
        super().__init__(**kwargs)

    @property
    def config_path(self) -> str:
        """Directory where the profile configuration is stored."""
        return os.path.join(
            self.global_config.config_path, "profiles", self.name
        )

    def initialize(self) -> None:
        """Initialize the profile directory."""
        from zenml.repository import Repository

        if not self.service_url:
            fileio.create_dir_recursive_if_not_exists(self.config_path)
        Repository.create_store(self)

    def cleanup(self) -> None:
        """Cleanup the profile directory."""
        if fileio.is_dir(self.config_path):
            fileio.rm_dir(self.config_path)

    @property
    def global_config(self) -> "GlobalConfig":
        """Return the global configuration to which this profile belongs."""
        return self._config or GlobalConfig()

    class Config:
        """Pydantic configuration class."""

        # Validate attributes when assigning them. We need to set this in order
        # to have a mix of mutable and immutable attributes
        validate_assignment = True
        # Ignore extra attributes from configs of previous ZenML versions
        extra = "ignore"
        # all attributes with leading underscore are private and therefore
        # are mutable and not included in serialization
        underscore_attrs_are_private = True


class GlobalConfig(BaseModel, metaclass=GlobalConfigMetaClass):
    """Stores global configuration options.

    Configuration options are read from a config file, but can be overwritten
    by environment variables. See `GlobalConfig.__getattribute__` for more
    details.

    Attributes:
        user_id: Unique user id.
        analytics_opt_in: If a user agreed to sending analytics or not.
        version: Version of ZenML that was last used to create or update the
            global config.
        active_profile_name: The name of the active configuration profile.
        profiles: Map of configuration profiles, indexed by name.
        _config_path: Directory where the global config file is stored.
    """

    user_id: uuid.UUID = Field(default_factory=uuid.uuid4, allow_mutation=False)
    analytics_opt_in: bool = True
    version: Optional[str]
    active_profile_name: Optional[str]
    profiles: Dict[str, ConfigProfile] = Field(default_factory=dict)
    _config_path: str

    def __init__(self, config_path: Optional[str] = None) -> None:
        """Initializes a GlobalConfig object using values from the config file.

        If the config file doesn't exist yet, we try to read values from the
        legacy (ZenML version < 0.6) config file.

        Args:
            config_path: custom config file path. When not specified, the
                default global configuration path is used and the global
                configuration singleton instance is returned.
        """
        self._config_path = config_path or self.default_config_directory()
        config_values = self._read_config()
        super().__init__(**config_values)

        if not fileio.file_exists(self._config_file(config_path)):
            # if the config file hasn't been written to disk, write it now to
            # make sure to persist the unique user id
            self._write_config()

    @validator("version")
    def _validate_version(cls, v: Optional[str]) -> Optional[str]:
        """Validate the version attribute."""
        if v is None:
            return v

        VersionInfo.parse(v)
        return v

    def __setattr__(self, key: str, value: Any) -> None:
        """Sets an attribute on the config and persists the new value in the
        global configuration."""
        super().__setattr__(key, value)
        if key.startswith("_"):
            return
        self._write_config()

    def __getattribute__(self, key: str) -> Any:
        """Gets an attribute value for a specific key.

        If a value for this attribute was specified using an environment
        variable called `$(CONFIG_ENV_VAR_PREFIX)$(ATTRIBUTE_NAME)` and its
        value can be parsed to the attribute type, the value from this
        environment variable is returned instead.
        """
        value = super().__getattribute__(key)
        if key.startswith("_"):
            return value

        environment_variable_name = f"{CONFIG_ENV_VAR_PREFIX}{key.upper()}"
        try:
            environment_variable_value = os.environ[environment_variable_name]
            # set the environment variable value to leverage pydantics type
            # conversion and validation
            super().__setattr__(key, environment_variable_value)
            return_value = super().__getattribute__(key)
            # set back the old value as we don't want to permanently store
            # the environment variable value here
            super().__setattr__(key, value)
            return return_value
        except (ValidationError, KeyError, TypeError):
            return value

    def _migrate_config(self) -> None:
        """Migrates the global config to the latest version."""

        curr_version = VersionInfo.parse(__version__)
        if self.version is None:
            logger.info(
                "Initializing the ZenML global configuration version to %s",
                curr_version,
            )
        else:
            config_version = VersionInfo.parse(self.version)
            if self.version > curr_version:
                raise RuntimeError(
                    "The ZenML global configuration version (%s) is higher "
                    "than the version of ZenML currently being used (%s). "
                    "Please update ZenML to at least match the global "
                    "configuration version to avoid loss of information.",
                    config_version,
                    curr_version,
                )
            if config_version == curr_version:
                return

            logger.info(
                "Migrating the ZenML global configuration from version %s "
                "to version %s...",
                config_version,
                curr_version,
            )

        # this will also trigger rewriting the config file to disk
        # to ensure the schema migration results are persisted
        self.version = __version__

    def _read_config(self) -> Dict[str, Any]:
        """Reads configuration options from disk.

        If the config file doesn't exist yet, this method falls back to reading
        options from a legacy config file or returns an empty dictionary.
        """
        legacy_config_file = os.path.join(
            self._config_path, LEGACY_CONFIG_FILE_NAME
        )

        config_values = {}
        if fileio.file_exists(self._config_file()):
            config_values = cast(
                Dict[str, Any],
                yaml_utils.read_yaml(self._config_file()),
            )
        elif fileio.file_exists(legacy_config_file):
            config_values = cast(
                Dict[str, Any], yaml_utils.read_json(legacy_config_file)
            )

        return config_values

    def _write_config(self, config_path: Optional[str] = None) -> None:
        """Writes the global configuration options to disk.

        Args:
            config_path: custom config file path. When not specified, the default
                global configuration path is used.
        """
        config_file = self._config_file(config_path)
        yaml_dict = json.loads(self.json())
        logger.debug(f"Writing config to {config_file}")

        if not fileio.file_exists(config_file):
            fileio.create_dir_recursive_if_not_exists(
                config_path or self._config_path
            )

        yaml_utils.write_yaml(config_file, yaml_dict)

    @staticmethod
    def default_config_directory() -> str:
        """Path to the default global configuration directory."""
        return get_global_config_directory()

    def _config_file(self, config_path: Optional[str] = None) -> str:
        """Path to the file where global configuration options are stored.

        Args:
            config_path: custom config file path. When not specified, the default
                global configuration path is used.
        """
        return os.path.join(config_path or self._config_path, "config.yaml")

    def copy_config_with_active_profile(
        self, config_path: str, load_config_path: Optional[str] = None
    ) -> "GlobalConfig":
        """Create a copy of the global config and the active profile using
        a different config path.

        Args:
            config_path: path where the global config copy should be saved
            load_config_path: path that will be used to load the global config
                copy. This can be set to a value different than `config_path`
                if the global config copy will be loaded from a different
                path, e.g. when the global config copy is copied to a
                container image. This will be reflected in the paths and URLs
                encoded in the copied profile.
        """
        self._write_config(config_path)

        config_copy = GlobalConfig(config_path=config_path)
        config_copy.profiles = {}
        config_copy.add_or_update_profile(self.active_profile)
        config_copy.activate_profile(self.active_profile_name)
        shutil.copytree(
            self.active_profile.config_path,
            config_copy.active_profile.config_path,
            dirs_exist_ok=True,
        )
        config_copy.active_profile.service_url = (
            self.active_profile.service_url.replace(
                self.config_path,
                load_config_path or config_copy.config_path,
            )
        )
        config_copy._write_config()
        return config_copy

    @property
    def config_path(self) -> str:
        """Directory where the global configuration file is located."""
        return self._config_path

    def add_or_update_profile(self, profile: ConfigProfile) -> None:
        """Adds or updates a profile in the global configuration.

        Args:
            profile: profile configuration
        """
        profile = profile.copy()
        profile._config = self
        if profile.name not in self.profiles:
            profile.initialize()
        self.profiles[profile.name] = profile
        self._write_config()

    def get_profile(self, profile_name: str) -> Optional[ConfigProfile]:
        """Get a global configuration profile.

        Args:
            profile_name: name of the profile to get

        Returns:
            The profile configuration or None if the profile doesn't exist
        """
        return self.profiles.get(profile_name)

    def activate_profile(self, profile_name: str) -> None:
        """Set a profile as the active.

        Args:
            profile_name: name of the profile to add
        """
        if profile_name not in self.profiles:
            raise KeyError(f"Profile '{profile_name}' not found.")
        self.active_profile_name = profile_name

    def _get_default_service_url(self) -> str:
        """Get the default service backend URL.

        Returns:
            The URL of the default service backend
        """
        from zenml.stack_stores import SqlStackStore

        return SqlStackStore.get_local_url(self.config_path)

    def _add_and_activate_default_profile(self) -> ConfigProfile:
        """Creates and activates the default configuration profile if no
        profiles are configured."""

        if self.profiles:
            return

        default_profile = ConfigProfile(
            name=DEFAULT_PROFILE_NAME,
        )
        self.add_or_update_profile(default_profile)
        self.activate_profile(DEFAULT_PROFILE_NAME)
        default_profile.initialize()
        return default_profile

    @property
    def active_profile(self) -> ConfigProfile:
        """Return the active profile. If no profile is created yet, create and
        return the default profile.

        Returns:
            The active profile configuration.
        """
        return self.profiles[self.active_profile_name]

    def delete_profile(self, profile_name: str) -> None:
        """Deletes a profile from the global configuration.

        If the profile is the active profile, the default profile is activated.
        The default profile cannot be removed.

        Args:
            profile_name: name of the profile to delete
        """
        if profile_name not in self.profiles:
            raise ValueError(f"Profile '{profile_name}' not found.")
        profile = self.profiles[profile_name]
        del self.profiles[profile_name]
        profile.cleanup()

        self._write_config()

    class Config:
        """Pydantic configuration class."""

        # Validate attributes when assigning them. We need to set this in order
        # to have a mix of mutable and immutable attributes
        validate_assignment = True
        # Ignore extra attributes from configs of previous ZenML versions
        extra = "ignore"
        # all attributes with leading underscore are private and therefore
        # are mutable and not included in serialization
        underscore_attrs_are_private = True
