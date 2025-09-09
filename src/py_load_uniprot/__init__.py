"""
py-load-uniprot

A high-performance Python package for ETL processing of UniProtKB data.
"""

__version__ = "1.0.0"

from .config import Settings, load_settings
from .core import PyLoadUniprotPipeline
from .db_manager import DatabaseAdapter

__all__ = ["PyLoadUniprotPipeline", "Settings", "load_settings", "DatabaseAdapter"]
