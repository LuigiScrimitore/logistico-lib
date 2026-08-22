"""
storage — Astrazione dei path di storage per ambiente (locale vs Azure Databricks).

Scopo: centralizzare in UN punto la risoluzione dei percorsi di landing zone e warehouse,
così la migrazione a Databricks richiede solo di configurare qui (o via variabili d'ambiente),
non di modificare i notebook. Vedi `DOCS/main/10_piano_migrazione_databricks.md` (DBR-01).

Detection ambiente:
  - Databricks: presente la variabile ``DATABRICKS_RUNTIME_VERSION`` (settata dal runtime del cluster).
  - Locale (Docker/Spark test): assente → si usano i path su filesystem.

Override espliciti (utili in test e in CI):
  - ``LOGISTICO_LANDING_ROOT``  — forza la landing root.
  - ``LOGISTICO_WAREHOUSE_ROOT``— forza la warehouse root.
  - ``LOGISTICO_DATA``          — base locale (default ``C:\\PROGETTI\\LOGISTICO_DATA``); da qui
                                  si derivano ``<base>/data/landing`` e ``<base>/data/warehouse``.
  - ``ADLS_ACCOUNT`` / ``ADLS_CONTAINER`` — per costruire path ``abfss://`` su Databricks (se non
                                  si usano UC Volumes).

NB: non impone una scelta D3 (Volume vs ADLS): di default su Databricks punta a un UC Volume
``/Volumes/landing_<env>/logistica/files``; se sono valorizzate ADLS_ACCOUNT/ADLS_CONTAINER usa abfss.
"""

from __future__ import annotations

import os


# ──────────────────────────────────────────────────────────────────────────────
# Detection ambiente
# ──────────────────────────────────────────────────────────────────────────────
def is_databricks() -> bool:
    """True se il codice gira su un cluster Azure Databricks."""
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def _local_data_base() -> str:
    return os.environ.get("LOGISTICO_DATA", r"C:\PROGETTI\LOGISTICO_DATA")


# ──────────────────────────────────────────────────────────────────────────────
# Landing zone
# ──────────────────────────────────────────────────────────────────────────────
def get_landing_root(env: str = "dev") -> str:
    """Root della landing zone (dove atterrano i file pushati dai sorgenti).

    Precedenza: override esplicito > Databricks (Volume/abfss) > locale filesystem.
    """
    override = os.environ.get("LOGISTICO_LANDING_ROOT")
    if override:
        return override

    if is_databricks():
        account = os.environ.get("ADLS_ACCOUNT")
        container = os.environ.get("ADLS_CONTAINER")
        if account and container:
            return f"abfss://{container}@{account}.dfs.core.windows.net/landing"
        # Default: UC Volume (D3 = Volume). Schema/volume da creare in setup.
        return f"/Volumes/landing_{env}/logistica/files"

    return os.path.join(_local_data_base(), "data", "landing")


# ──────────────────────────────────────────────────────────────────────────────
# Warehouse (solo locale: su Databricks le tabelle sono gestite da Unity Catalog)
# ──────────────────────────────────────────────────────────────────────────────
def get_warehouse_root(env: str = "dev") -> str:
    """Root del warehouse Delta.

    Su Databricks lo storage delle tabelle è gestito da Unity Catalog (managed tables):
    questo path non va usato per costruire nomi tabella — usare ``get_catalog()`` + schema.
    Restituito comunque per completezza (es. tool locali, external location espliciti).
    """
    override = os.environ.get("LOGISTICO_WAREHOUSE_ROOT")
    if override:
        return override

    if is_databricks():
        account = os.environ.get("ADLS_ACCOUNT")
        container = os.environ.get("ADLS_CONTAINER")
        if account and container:
            return f"abfss://{container}@{account}.dfs.core.windows.net/warehouse"
        # UC managed: nessun path filesystem esplicito.
        return ""

    return os.path.join(_local_data_base(), "data", "warehouse")
