from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from py_load_uniprot.config import DBSettings, Settings, load_settings


def test_db_settings_connection_string():
    """Tests the connection_string property of DBSettings."""
    db_settings = DBSettings(
        host="myhost",
        port=1234,
        user="myuser",
        password="mypassword",
        dbname="mydb",
    )
    expected = "dbname='mydb' user='myuser' host='myhost' port='1234' password='mypassword'"
    assert db_settings.connection_string == expected


def test_load_settings_from_yaml(tmp_path: Path):
    """Tests loading settings from a YAML file."""
    config_content = {
        "profile": "standard",
        "data_dir": "/tmp/data",
        "db": {"host": "yaml_host"},
    }
    config_file = tmp_path / "config.yaml"
    with open(config_file, "w") as f:
        yaml.dump(config_content, f)

    settings = load_settings(config_file)

    assert settings.profile == "standard"
    assert settings.data_dir == Path("/tmp/data")
    assert settings.db.host == "yaml_host"
    assert settings.db.user == "postgres"  # Default value


def test_load_settings_file_not_found():
    """Tests that FileNotFoundError is raised for a non-existent config file."""
    with pytest.raises(FileNotFoundError):
        load_settings(Path("/non/existent/file.yaml"))


@patch.dict(
    "os.environ",
    {
        "PY_LOAD_UNIPROT_PROFILE": "standard",
        "PY_LOAD_UNIPROT_DB__HOST": "env_host",
    },
)
def test_load_settings_from_env():
    """Tests loading settings from environment variables."""
    settings = load_settings()
    assert settings.profile == "standard"
    assert settings.db.host == "env_host"


@patch.dict(
    "os.environ",
    {
        "PY_LOAD_UNIPROT_DB__HOST": "env_host",
    },
)
def test_load_settings_yaml_overrides_env(tmp_path: Path):
    """Tests that values from a YAML file override environment variables."""
    config_content = {"db": {"host": "yaml_host"}}
    config_file = tmp_path / "config.yaml"
    with open(config_file, "w") as f:
        yaml.dump(config_content, f)

    settings = load_settings(config_file)

    assert settings.db.host == "yaml_host"
