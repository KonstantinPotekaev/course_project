import json
import os
from abc import ABC
from enum import Enum
from pathlib import Path
from typing import Optional, TypeVar, Generic

from pydantic import BaseModel, Field


class ConfigSource(str, Enum):
    ARGS = "args"
    CONFIG_FILE = "config"
    DEFAULT = "default"


T = TypeVar("T")


class ConfigParameter(BaseModel, Generic[T]):
    value: Optional[T]
    source: Optional[ConfigSource]
    required: bool = False

    class Config:
        arbitrary_types_allowed = True


class BaseConfigurationParams(str, Enum):
    pass


class BaseParameterManager(BaseModel, ABC):
    class Config:
        arbitrary_types_allowed = True

    @classmethod
    def from_args(cls, args, check_requirements: bool = False) -> "BaseParameterManager":
        params = {}
        for field_name, field_info in cls.__fields__.items():
            value = getattr(args, field_name, None)
            if not value:
                continue
            try:
                field_type = field_info.outer_type_.__args__[0]
                params[field_name] = ConfigParameter(value=field_type(value), source=ConfigSource.ARGS)
            except (ValueError, TypeError) as e:
                raise ValueError(f"Failed to convert argument '{field_name}' to expected type: {e}")

        instance = cls(**params)
        if check_requirements:
            instance.check_required_fields()
        return instance

    @classmethod
    def from_json(cls, file_path: Path, check_requirements: bool = False) -> "BaseParameterManager":
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Configuration file {file_path} does not exist.")

        try:
            with open(file_path, "r") as file:
                data = json.load(file)
        except json.JSONDecodeError as e:
            raise ValueError(f"Error parsing JSON file '{file_path}': {e}")

        params = {}
        for key, value in data.items():
            if not key in cls.__fields__:
                continue
            try:
                field_info = cls.__fields__[key]
                field_type = field_info.outer_type_.__args__[0]
                params[key] = ConfigParameter(value=field_type(value), source=ConfigSource.CONFIG_FILE)
            except (ValueError, TypeError) as e:
                raise ValueError(f"Failed to convert argument '{value}' to expected type: {e}")

        instance = cls(**params)
        if check_requirements:
            instance.check_required_fields()
        return instance

    @classmethod
    def merge_configs(cls,
                      args_config: Optional["BaseParameterManager"] = None,
                      json_config: Optional["BaseParameterManager"] = None,
                      check_requirements: bool = False) -> "BaseParameterManager":
        params = {}

        for field_name, field_info in cls.__fields__.items():
            params[field_name] = cls._resolve_field_value(field_name,
                                                          field_info,
                                                          args_config,
                                                          json_config)

        instance = cls(**params)
        if check_requirements:
            instance.check_required_fields()
        return instance

    @classmethod
    def _resolve_field_value(cls,
                             field_name: str,
                             field_info, args_config: Optional["BaseParameterManager"],
                             json_config: Optional["BaseParameterManager"]):
        arg = getattr(args_config, field_name, None) if args_config else None

        if arg and arg.value and arg.source != ConfigSource.DEFAULT:
            return arg
        elif json_config:
            json_value = getattr(json_config, field_name, None)
            if json_value is not None:
                return json_value
        return field_info.default

    def update_parameters(self,
                          new_params: Optional["BaseParameterManager"],
                          update_args: bool = False,
                          update_defaults: bool = False,
                          check_requirements: bool = False):
        """
        Обновляет конфигурацию с новыми параметрами.

        :param new_params: ParameterManager, содержащий новые параметры конфигурации.
        :param update_args: Определяет, обновлять ли параметры, изначально переданные через аргументы командной строки.
        :param update_defaults: Определяет, обновлять ли параметры значениями по умолчанию.
        :param check_requirements: Определяет, проверять ли обязательные параметры.
        """
        for key, value in new_params:
            if key not in self.__fields__:
                continue

            current_param = getattr(self, key)
            if current_param.source == ConfigSource.ARGS and not update_args:
                continue
            if current_param.source == ConfigSource.DEFAULT and not update_defaults:
                continue

            self.__setattr__(key, value)

            if check_requirements:
                self.check_required_fields()

    def check_required_fields(self):
        """
        Проверяет, что все обязательные поля заданы.
        Выбрасывает ValueError, если какие-либо обязательные поля отсутствуют.
        """
        missing_fields = []
        for field_name, field_info in self.__fields__.items():
            field_value = getattr(self, field_name, None)
            if (
                    isinstance(field_value, ConfigParameter)
                    and field_value.required
                    and field_value.value is None
            ):
                missing_fields.append(field_name)

        if missing_fields:
            raise ValueError(f"The following required fields are missing or not set: {', '.join(missing_fields)}")

    def get_full_item(self, param_name: str):
        if param_name not in self.__fields__:
            raise ValueError(f"No parameter registered with name '{param_name}'")
        return getattr(self, param_name)

    def __getitem__(self, key):
        if key not in self.__fields__:
            raise KeyError(f"No parameter registered with name '{key}'")
        return getattr(self, key).value
