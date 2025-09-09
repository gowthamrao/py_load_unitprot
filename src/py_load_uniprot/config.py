from pathlib import Path
from typing import Any, Literal, Optional, Type

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)


class DBSettings(BaseModel):
    host: str = "localhost"
    port: int = 5432
    user: str = "postgres"
    password: str = "password"
    dbname: str = "uniprot"

    @property
    def connection_string(self) -> str:
        return f"dbname='{self.dbname}' user='{self.user}' host='{self.host}' port='{self.port}' password='{self.password}'"


class URLSettings(BaseModel):
    uniprot_ftp_base_url: str = "https://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/complete/"
    release_notes_filename: str = "reldate.txt"
    checksums_filename: str = "MD5SUMS"


class YamlConfigSettingsSource(PydanticBaseSettingsSource):
    """
    A settings source that loads configuration from a YAML file.
    """

    def __init__(self, settings_cls: Type[BaseSettings]):
        super().__init__(settings_cls)
        self.config_file: Optional[Path] = getattr(
            self.settings_cls, "config_file", None
        )

    def get_field_value(
        self, field: Any, field_name: str
    ) -> tuple[Any, str] | tuple[None, None]:
        if self.config_file and self.config_file.is_file():
            with open(self.config_file, "r") as f:
                file_content = yaml.safe_load(f) or {}
                return file_content.get(field_name), field_name
        return None, None

    def __call__(self) -> dict[str, Any]:
        d: dict[str, Any] = {}
        if self.config_file and self.config_file.is_file():
            with open(self.config_file, "r") as f:
                d.update(yaml.safe_load(f) or {})
        return d


class Settings(BaseSettings):
    profile: Literal["full", "standard"] = "full"
    data_dir: Path = Path("data")
    db: DBSettings = Field(default_factory=DBSettings)
    urls: URLSettings = Field(default_factory=URLSettings)
    config_file: Optional[Path] = Field(default=None, exclude=True)

    model_config = SettingsConfigDict(
        env_prefix="PY_LOAD_UNIPROT_",
        env_nested_delimiter="__",
        env_file_encoding="utf-8",
    )

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: Type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        return (
            init_settings,
            env_settings,
            dotenv_settings,
            YamlConfigSettingsSource(settings_cls),
            file_secret_settings,
        )


_settings: Optional[Settings] = None


def initialize_settings(config_file: Optional[Path] = None) -> None:
    global _settings
    if _settings is None:
        if config_file and not config_file.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_file}")
        _settings = Settings(config_file=config_file)


def get_settings() -> Settings:
    if _settings is None:
        initialize_settings()
    if _settings is None:
        raise RuntimeError("Settings not initialized")
    return _settings
