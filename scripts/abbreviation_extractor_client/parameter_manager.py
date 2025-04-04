from enum import Enum
from pathlib import Path
from typing import Optional

from pydantic import Field

from scripts.common.base_parameter_manager import BaseConfigurationParams, BaseParameterManager, ConfigParameter, \
    ConfigSource

class LanguageEnum(str, Enum):
    RUSSIAN = "ru"
    ENGLISH = "en"

class ConfigurationParams(BaseConfigurationParams):
    HOST = "host"
    ACCESS_KEY = "access_key"
    SECRET_KEY = "secret_key"
    IN_DIR = "in_dir"
    OUT_DIR = "out_dir"
    CONFIG = "config"
    THREADS = "threads"
    CHUNK_SIZE = "chunk_size"
    BUCKET = "bucket"
    API_TIMEOUT = "api_timeout"
    LANGUAGE = "language"

class ParameterManager(BaseParameterManager):
    host: Optional[ConfigParameter[str]] = Field(default=ConfigParameter(value=None, source=None, required=True))
    access_key: Optional[ConfigParameter[str]] = Field(default=ConfigParameter(value=None, source=None, required=True))
    secret_key: Optional[ConfigParameter[str]] = Field(default=ConfigParameter(value=None, source=None, required=True))
    in_dir: Optional[ConfigParameter[Path]] = Field(default=ConfigParameter(value=None, source=None, required=True))
    out_dir: Optional[ConfigParameter[Path]] = Field(default=ConfigParameter(value=None, source=None, required=True))
    config: Optional[ConfigParameter[Path]] = Field(default=ConfigParameter(value=None, source=None, required=False))
    threads: ConfigParameter[int] = Field(default=ConfigParameter(value=1, source=ConfigSource.DEFAULT))
    chunk_size: ConfigParameter[int] = Field(default=ConfigParameter(value=1024, source=ConfigSource.DEFAULT))
    bucket: ConfigParameter[str] = Field(default=ConfigParameter(value="abbreviation-test", source=ConfigSource.DEFAULT))
    api_timeout: ConfigParameter[int] = Field(default=ConfigParameter(value=3600, source=ConfigSource.DEFAULT))
    language: ConfigParameter[LanguageEnum] = Field(default=ConfigParameter(value=LanguageEnum.RUSSIAN, source=ConfigSource.DEFAULT))