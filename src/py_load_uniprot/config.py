"""
Configuration management for py-load-uniprot.

This module uses pydantic-settings to load configuration from a YAML file
and/or environment variables. It ensures that all necessary configuration
parameters are present and correctly typed.

The order of precedence for loading settings is:
1. Environment variables (highest priority)
2. YAML file
3. Default values defined in the models

Environment variables must be prefixed with 'PY_LOAD_UNIPROT_'.
Nested keys are separated by double underscores, e.g., PY_LOAD_UNIPROT_DB__HOST.
"""

from pathlib import Path
from typing import Any, Dict, Optional

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

# --- Pydantic Models ---

class DBSettings(BaseModel):
    """Database connection settings."""
    host: str = "localhost"
    port: int = 5432
    user: str = "postgres"
    password: str = "password"
    dbname: str = "uniprot"


class URLSettings(BaseModel):
    """UniProt source URL settings."""
    uniprot_ftp_base_url: str = "https://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/complete/"
    release_notes_filename: str = "reldate.txt"
    checksums_filename: str = "MD5SUMS"


def yaml_config_settings_source(settings: BaseSettings) -> Dict[str, Any]:
    """
    A custom settings source that loads configuration from a YAML file.
    The path to the YAML file is specified in the `config_file` attribute
    of the main Settings class.
    """
    config_file = getattr(settings.__class__.model_config, 'config_file', None)
    if config_file and Path(config_file).is_file():
        with open(config_file, 'r') as f:
            return yaml.safe_load(f)
    return {}


class Settings(BaseSettings):
    """
    Main configuration class for the application.
    """
    data_dir: Path = Field(default="data", description="Directory to store downloaded UniProt files.")
    db: DBSettings = Field(default_factory=DBSettings)
    urls: URLSettings = Field(default_factory=URLSettings)

    model_config = SettingsConfigDict(
        env_prefix='PY_LOAD_UNIPROT_',
        env_nested_delimiter='__',
        config_file=None
    )

    @classmethod
    def from_yaml(cls, path: Optional[Path]) -> "Settings":
        """
        Factory method to create a Settings instance from a specific YAML file.
        """
        if path:
            # Create a temporary config dict to pass the file path
            config = SettingsConfigDict(config_file=str(path))
            return cls(_settings_source=yaml_config_settings_source, **config)
        return cls()


# --- Singleton Pattern for Settings ---

_settings: Optional[Settings] = None


def initialize_settings(config_file: Optional[Path] = None):
    """
    Initializes the global settings object from a YAML file.
    This function should be called once at application startup.
    """
    global _settings
    if _settings is None:
        if config_file and not config_file.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_file}")
        _settings = Settings.from_yaml(config_file)
    else:
        # Optionally, you could log a warning that settings are already initialized.
        pass


def get_settings() -> Settings:
    """
    Retrieves the global settings object.

    Raises:
        RuntimeError: If settings have not been initialized.

    Returns:
        The global Settings instance.
    """
    if _settings is None:
        # Initialize with defaults if not explicitly initialized.
        # This is helpful for testing or simple script usage.
        initialize_settings()

    # The check below is now technically redundant due to the line above,
    # but it's good for type-hinting and ensuring correctness.
    if _settings is None:
         raise RuntimeError("Settings have not been initialized. Call initialize_settings() first.")

    return _settings
