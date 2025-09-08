import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path
from typing import Optional

# This will be the global settings object, initialized by the CLI.
# This approach prevents the settings from being loaded at import time,
# allowing the CLI to control the configuration source.
_settings_instance: Optional["Settings"] = None

class DatabaseSettings(BaseModel):
    """Pydantic model for database connection settings."""
    host: str = "localhost"
    port: int = 5432
    user: str = "user"
    password: str = "password"
    dbname: str = "uniprot"

class UrlsSettings(BaseModel):
    """Pydantic model for UniProt URLs."""
    uniprot_ftp_base_url: str = "https://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/complete/"
    release_notes_filename: str = "reldate.txt"
    checksums_filename: str = "md5"

    @property
    def swissprot_xml_url(self) -> str:
        return f"{self.uniprot_ftp_base_url}uniprot_sprot.xml.gz"

    @property
    def trembl_xml_url(self) -> str:
        return f"{self.uniprot_ftp_base_url}uniprot_trembl.xml.gz"

    @property
    def release_notes_url(self) -> str:
        return f"{self.uniprot_ftp_base_url}{self.release_notes_filename}"

    @property
    def checksums_url(self) -> str:
        return f"{self.uniprot_ftp_base_url}{self.checksums_filename}"


class Settings(BaseSettings):
    """
    Manages the configuration for the py-load-uniprot package.
    Settings can be loaded from a YAML file, environment variables, or a .env file.
    Environment variables will override values from a YAML file.
    """
    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        extra='ignore',
        env_nested_delimiter='__' # e.g., PY_LOAD_UNIPROT_DB__HOST
    )

    data_dir: Path = Path("data")
    db: DatabaseSettings = Field(default_factory=DatabaseSettings)
    urls: UrlsSettings = Field(default_factory=UrlsSettings)

    @property
    def db_connection_string(self) -> str:
        """Constructs the database connection string from the 'db' model."""
        return (
            f"dbname='{self.db.dbname}' "
            f"user='{self.db.user}' "
            f"host='{self.db.host}' "
            f"password='{self.db.password}' "
            f"port='{self.db.port}'"
        )

    @classmethod
    def from_yaml(cls, path: Path) -> "Settings":
        """
        Loads configuration from a YAML file and merges it with environment variables.
        """
        if not path.exists():
            raise FileNotFoundError(f"Configuration file not found at: {path}")

        with open(path, 'r') as f:
            yaml_data = yaml.safe_load(f) or {}

        # Pydantic-settings will automatically load from env vars and .env file,
        # and values from env vars will override those from the initial dict.
        return cls(**yaml_data)

def get_settings() -> Settings:
    """
    Retrieves the global settings instance.
    This function ensures that the settings have been initialized before use.
    """
    if _settings_instance is None:
        raise RuntimeError(
            "Settings have not been initialized. "
            "Please call initialize_settings() from the CLI entrypoint."
        )
    return _settings_instance

def initialize_settings(config_file: Optional[Path] = None) -> Settings:
    """
    Initializes the global settings from a YAML file or environment variables.
    This should be called once at the start of the application from the CLI.
    """
    global _settings_instance
    if config_file:
        print(f"Loading configuration from: {config_file.resolve()}")
        _settings_instance = Settings.from_yaml(config_file)
    else:
        print("Loading configuration from environment variables.")
        _settings_instance = Settings()
    return _settings_instance
