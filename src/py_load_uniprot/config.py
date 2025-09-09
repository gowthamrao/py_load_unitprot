from pathlib import Path
from typing import Literal, Optional

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class DBSettings(BaseModel):
    """Database connection details."""

    host: str = "localhost"
    port: int = 5432
    user: str = "postgres"
    password: str = "password"
    dbname: str = "uniprot"

    @property
    def connection_string(self) -> str:
        """Constructs a libpq-compatible connection string."""
        return f"dbname='{self.dbname}' user='{self.user}' host='{self.host}' port='{self.port}' password='{self.password}'"


class URLSettings(BaseModel):
    """Configuration for UniProt source URLs."""

    uniprot_ftp_base_url: str = (
        "https://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/complete/"
    )
    release_notes_filename: str = "reldate.txt"
    checksums_filename: str = "MD5SUMS"


class Settings(BaseSettings):
    """Main application settings model."""

    profile: Literal["full", "standard"] = "full"
    data_dir: Path = Path("data")
    db: DBSettings = Field(default_factory=DBSettings)
    urls: URLSettings = Field(default_factory=URLSettings)

    model_config = SettingsConfigDict(
        env_prefix="PY_LOAD_UNIPROT_",
        env_nested_delimiter="__",
        env_file_encoding="utf-8",
    )


def load_settings(config_file: Optional[Path] = None) -> Settings:
    """
    Loads settings from a YAML file and/or environment variables.

    This factory function allows for explicit, programmatic configuration,
    making the package suitable for use as a library.

    Note on Precedence:
    Pydantic's `BaseSettings` gives precedence to values passed directly to its
    constructor over values from environment variables. Therefore, settings
    loaded from the YAML file will **override** environment variables.

    Args:
        config_file: Optional path to a YAML configuration file.

    Returns:
        A populated Settings object.

    Raises:
        FileNotFoundError: If the specified config_file does not exist.
    """
    init_kwargs = {}
    if config_file:
        if not config_file.is_file():
            raise FileNotFoundError(f"Configuration file not found: {config_file}")
        with open(config_file, "r") as f:
            init_kwargs = yaml.safe_load(f) or {}

    return Settings(**init_kwargs)
