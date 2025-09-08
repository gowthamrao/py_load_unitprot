from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path

class Settings(BaseSettings):
    """
    Manages the configuration for the py-load-uniprot package.
    Reads settings from environment variables or a .env file.
    """
    model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8', extra='ignore')

    # --- Local data storage ---
    DATA_DIR: Path = Path("data")

    # --- Database Connection ---
    POSTGRES_HOST: str = "localhost"
    POSTGRES_PORT: int = 5432
    POSTGRES_USER: str = "user"
    POSTGRES_PASSWORD: str = "password"
    POSTGRES_DB: str = "uniprot"

    @property
    def db_connection_string(self) -> str:
        """Constructs the database connection string."""
        return f"dbname='{self.POSTGRES_DB}' user='{self.POSTGRES_USER}' host='{self.POSTGRES_HOST}' password='{self.POSTGRES_PASSWORD}' port='{self.POSTGRES_PORT}'"

    # --- UniProt URLs ---
    # Base URL for the current UniProt release data
    UNIPROT_FTP_BASE_URL: str = "https://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/complete/"

    @property
    def swissprot_xml_url(self) -> str:
        return f"{self.UNIPROT_FTP_BASE_URL}uniprot_sprot.xml.gz"

    @property
    def trembl_xml_url(self) -> str:
        return f"{self.UNIPROT_FTP_BASE_URL}uniprot_trembl.xml.gz"

    @property
    def release_notes_url(self) -> str:
        # Note: Filename is a best-guess based on common FTP site conventions.
        return f"{self.UNIPROT_FTP_BASE_URL}reldate.txt"

    @property
    def checksums_url(self) -> str:
        # Note: Filename is a best-guess and might be 'md5sum.txt' or similar.
        return f"{self.UNIPROT_FTP_BASE_URL}md5"


# Single, reusable instance of the settings
settings = Settings()
