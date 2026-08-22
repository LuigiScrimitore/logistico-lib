"""
cost_tags.py — Convenzione tag per monitoraggio costi (release kit KIT-05).

Tag standard da applicare DAL PRIMO RUN a compute (job serverless / budget policy)
e a tabelle Delta (TBLPROPERTIES), per attribuire la spesa per pipeline/wave e
filtrare la dashboard costi (system.billing.usage).

Chiavi canoniche:
    business_unit = "logistica"     (fisso — separa dal retail sulla piattaforma condivisa)
    project       = "logistico2.0"
    env           = dev|prod
    pipeline      = <nome pipeline>  (es. gold_f_carico)
    wave          = <A|B|...|A-agg>  (fase di rilascio)
    layer         = bronze|silver|gold|config
    managed_by    = "dab"            (Databricks Asset Bundle)

NB: i tag vanno impostati anche nella **budget policy serverless** (Terraform
brownfield) e nei job DAB (KIT-06). Qui la convenzione + gli helper condivisi.
"""

from __future__ import annotations

from typing import Dict, Optional

BUSINESS_UNIT = "logistica"
PROJECT = "logistico2.0"
MANAGED_BY = "dab"


def build_tags(pipeline: str, env: str, wave: Optional[str] = None,
               layer: Optional[str] = None, extra: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    """Restituisce il dizionario di tag canonici per una pipeline.

    Usabile per: job/cluster tags (serverless), TBLPROPERTIES tabelle Delta,
    e come default_tags del bundle DAB.
    """
    tags = {
        "business_unit": BUSINESS_UNIT,
        "project": PROJECT,
        "env": env,
        "pipeline": pipeline,
        "managed_by": MANAGED_BY,
    }
    if wave:
        tags["wave"] = wave
    if layer:
        tags["layer"] = layer
    if extra:
        tags.update({str(k): str(v) for k, v in extra.items()})
    return tags


def tblproperties_clause(tags: Dict[str, str]) -> str:
    """Frammento SQL ``TBLPROPERTIES (...)`` con i tag (prefisso ``tag.``).

    Es.::

        ALTER TABLE gold_prod.logistica.F_CARICO SET
        TBLPROPERTIES ('tag.business_unit'='logistica', 'tag.pipeline'='gold_f_carico', ...)
    """
    items = ", ".join(f"'tag.{k}'='{v}'" for k, v in sorted(tags.items()))
    return f"TBLPROPERTIES ({items})"


def apply_table_tags(spark, table_fqn: str, pipeline: str, env: str,
                     wave: Optional[str] = None, layer: Optional[str] = None) -> None:
    """Applica i tag canonici a una tabella Delta via ALTER TABLE ... SET TBLPROPERTIES.

    Idempotente (SET sovrascrive). No-op se la tabella non esiste.
    """
    if not spark.catalog.tableExists(table_fqn):
        return
    tags = build_tags(pipeline=pipeline, env=env, wave=wave, layer=layer)
    spark.sql(f"ALTER TABLE {table_fqn} SET {tblproperties_clause(tags)}")
