"""
logistica_utils — Libreria condivisa per il progetto Logistico 2.0.

Migrazione Oracle → Databricks, Medallion Architecture su Azure.
Importa con: from logistica_utils import SecretHelper, Logger, DeltaHelper, DQHelper, get_run_date, get_catalog
"""

from .secret_helper import SecretHelper
from .logging_helper import Logger
from .delta_helper import DeltaHelper
from .dq_helper import DQHelper
from .utils import get_run_date, get_catalog, get_condiviso_schema, surrogate_key_fallback, cast_decimal, add_ingestion_metadata
from .storage import is_databricks, get_landing_root, get_warehouse_root

__all__ = [
    "SecretHelper",
    "Logger",
    "DeltaHelper",
    "DQHelper",
    "get_run_date",
    "get_catalog",
    "get_condiviso_schema",
    "surrogate_key_fallback",
    "cast_decimal",
    "add_ingestion_metadata",
    "is_databricks",
    "get_landing_root",
    "get_warehouse_root",
]

__version__ = "1.0.0"
