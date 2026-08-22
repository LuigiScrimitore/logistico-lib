"""
acceptance.py — Acceptance-criteria + smoke-test per pipeline (release kit KIT-02).

Rende oggettivo il "funziona?" a ogni rilascio: criteri **dichiarativi** per
pipeline + un runner che li verifica sulla tabella gold e registra gli esiti
via ``dq_monitor`` (severità, persistenza in ``control_<env>.etl.dq_results``,
alerting, gate bloccante).

Uso tipico (dopo il run di una pipeline gold):

    from acceptance import ACCEPTANCE_REGISTRY, run_smoke_test
    dq = run_smoke_test(spark, ACCEPTANCE_REGISTRY["gold_f_carico"],
                        env="dev", run_date="2026-06-10", elapsed_s=43.0)
    # -> record row_count/not_null/unique/orphan/measure/volume/timing,
    #    persist su dq_results, alert sui fail, raise se un BLOCKING fallisce.

I criteri sono un **template**: si compila un ``AcceptanceCriteria`` per ogni
pipeline nel registry e si estende senza toccare il runner.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

try:
    from .utils import get_catalog
    from .dq_monitor import DQMonitor, Severity, Notifier
except ImportError:  # lib sul sys.path (runner locale)
    from utils import get_catalog
    from dq_monitor import DQMonitor, Severity, Notifier


@dataclass
class AcceptanceCriteria:
    """Criteri di accettazione dichiarativi per una pipeline gold.

    Campi:
      pipeline        nome pipeline (chiave dq_results)
      table           nome tabella (es. "F_CARICO") — FQN risolto via get_catalog
      layer/schema    default gold/logistica
      wave            wave di rilascio (A, B, ...)
      min_rows/max_rows  range righe atteso (max None = nessun limite)
      not_null        colonne obbligatorie (BLOCKING)
      unique_keys     grana: nessun duplicato sulla chiave (BLOCKING)
      orphan_fks      FK che devono avere orphan-rate <= orphan_max_pct (BLOCKING)
      orphan_max_pct  soglia orphan (default 0%)
      measures_nonneg misure che non devono essere negative (WARNING)
      volume_max_dev_pct  soglia anomalia volumi vs storico (WARNING)
      sla_seconds     timing atteso indicativo (report, WARNING se superato)
      sentinel        valore sentinella orphan (default "-1")
    """
    pipeline: str
    table: str
    wave: Optional[str] = None
    layer: str = "gold"
    schema: str = "logistica"
    min_rows: int = 1
    max_rows: Optional[int] = None
    not_null: List[str] = field(default_factory=list)
    unique_keys: List[str] = field(default_factory=list)
    orphan_fks: List[str] = field(default_factory=list)
    orphan_max_pct: float = 0.0
    measures_nonneg: List[str] = field(default_factory=list)
    volume_max_dev_pct: float = 30.0
    sla_seconds: Optional[float] = None
    sentinel: str = "-1"

    def fqn(self, env: str) -> str:
        return f"{get_catalog(self.layer, env)}.{self.schema}.{self.table}"


def run_smoke_test(spark: SparkSession, criteria: AcceptanceCriteria, env: str,
                   run_date: Optional[str] = None, elapsed_s: Optional[float] = None,
                   notifier: Optional[Notifier] = None, gate: bool = True,
                   logger=None) -> DQMonitor:
    """Esegue lo smoke-test dei criteri su una tabella gold e registra gli esiti.

    Ritorna il ``DQMonitor`` (con summary()); se ``gate=True`` solleva
    ``DQBlockingError`` su fallimenti BLOCKING.
    """
    fqn = criteria.fqn(env)
    dq = DQMonitor(spark, pipeline=criteria.pipeline, env=env, run_date=run_date,
                   wave=criteria.wave, notifier=notifier, logger=logger)

    if not spark.catalog.tableExists(fqn):
        dq.record("table_exists", False, Severity.BLOCKING, criteria.layer, criteria.table,
                  None, None, {"fqn": fqn})
        dq.persist()
        if gate:
            dq.gate()
        return dq

    df = spark.table(fqn)
    n = df.count()
    cols = set(df.columns)

    # 1) row_count nel range
    rc_ok = n >= criteria.min_rows and (criteria.max_rows is None or n <= criteria.max_rows)
    dq.record("row_count", rc_ok, Severity.BLOCKING, criteria.layer, criteria.table,
              float(n), float(criteria.min_rows),
              {"min": criteria.min_rows, "max": criteria.max_rows, "actual": n})

    # 2) not_null (BLOCKING)
    for c in criteria.not_null:
        if c not in cols:
            dq.record(f"not_null_{c}", False, Severity.BLOCKING, criteria.layer, criteria.table,
                      None, None, {"note": "colonna assente"})
            continue
        nulls = df.filter(F.col(c).isNull()).count()
        dq.record(f"not_null_{c}", nulls == 0, Severity.BLOCKING, criteria.layer, criteria.table,
                  float(nulls), 0.0, {"nulls": nulls})

    # 3) unique_keys / grana (BLOCKING)
    if criteria.unique_keys and all(k in cols for k in criteria.unique_keys):
        distinct = df.select(*criteria.unique_keys).distinct().count()
        dups = n - distinct
        dq.record("unique_keys", dups == 0, Severity.BLOCKING, criteria.layer, criteria.table,
                  float(dups), 0.0, {"keys": criteria.unique_keys, "duplicates": dups})

    # 4) orphan-rate per FK (BLOCKING) — ignora orphan con sentinel su FK by-design assenti
    for fk in criteria.orphan_fks:
        if fk not in cols:
            dq.record(f"orphan_{fk}", False, Severity.BLOCKING, criteria.layer, criteria.table,
                      None, None, {"note": "colonna assente"})
            continue
        orph = df.filter(F.col(fk).cast("string") == F.lit(criteria.sentinel)).count()
        rate = round(100.0 * orph / n, 4) if n else 0.0
        dq.record(f"orphan_{fk}", rate <= criteria.orphan_max_pct, Severity.BLOCKING,
                  criteria.layer, criteria.table, rate, criteria.orphan_max_pct,
                  {"orphans": orph, "rate_pct": rate})

    # 5) misure non negative (WARNING)
    for m in criteria.measures_nonneg:
        if m not in cols:
            continue
        neg = df.filter(F.col(m) < F.lit(0)).count()
        dq.record(f"nonneg_{m}", neg == 0, Severity.WARNING, criteria.layer, criteria.table,
                  float(neg), 0.0, {"negatives": neg})

    # 6) volume-anomaly vs storico (WARNING)
    dq.check_volume_anomaly(criteria.table, current_count=n, layer=criteria.layer,
                            max_dev_pct=criteria.volume_max_dev_pct)

    # 7) timing SLA (WARNING, informativo)
    if criteria.sla_seconds is not None and elapsed_s is not None:
        dq.record("sla_timing", elapsed_s <= criteria.sla_seconds, Severity.WARNING,
                  criteria.layer, criteria.table, float(elapsed_s), float(criteria.sla_seconds),
                  {"elapsed_s": elapsed_s, "sla_s": criteria.sla_seconds})

    dq.persist()
    if gate:
        dq.gate()
    return dq


# ─────────────────────────────────────────────────────────────────────────────
# Registry criteri per pipeline — TEMPLATE (compilare uno per pipeline).
# F_CARICO = pilota (valori calibrati sul re-run 2026-07-05). Gli altri sono
# scheletri con soglie prudenti da rifinire al primo rilascio cloud.
# ─────────────────────────────────────────────────────────────────────────────
ACCEPTANCE_REGISTRY = {
    "gold_f_carico": AcceptanceCriteria(
        pipeline="gold_f_carico", table="F_CARICO", wave="A",
        min_rows=1,
        # PES_CARICO/VOL_CARICO in not_null (BLOCKING) per OP-CAR-6: sono NULL **solo** se
        # il fallback su LU_ART_UNITA_LOGISTICA e' scattato (schema anagrafiche irraggiungibile).
        # Con l'anagrafica agganciata non sono mai NULL: le righe senza match danno 0 per
        # l'NVL(...,0) della formula ODI. Quindi un NULL qui NON e' un dato mancante, e' un
        # errore di configurazione — e va bloccato, non segnalato.
        # NB: measures_nonneg da solo NON li copriva: un NULL non e' un valore negativo, e
        # infatti F_CARICO ha passato 13/13 check con 59.621/59.621 righe a NULL (ACT_9015).
        not_null=["SITO_COD", "DATA_CARICO", "NUM_ETICH", "PES_CARICO", "VOL_CARICO"],
        unique_keys=["SITO_COD", "NUM_DOC_CARICO", "NUM_ETICH", "NUM_BOLLA_FORN"],
        orphan_fks=["SITO_COD", "FORNITORE_COD", "ART_RADICE_COD"],  # CORRIERE escluso (by-design -1)
        orphan_max_pct=0.0,
        measures_nonneg=["QTA_CARICO", "QTA_UF_CARICO", "PES_CARICO", "VOL_CARICO", "QTA_ORD_FORN"],
        volume_max_dev_pct=30.0,
    ),
    # ── Wave A/C/B/D/E: target aggiunti con ACT_9010 ─────────────────────────
    # Colonne VERIFICATE sui notebook Gold. Soglie orphan calibrate sui residui noti:
    # un orphan_max_pct=0 su una FK con residui fisiologici produrrebbe FAIL BLOCKING falsi.
    "gold_f_prep_sped": AcceptanceCriteria(
        pipeline="gold_f_prep_sped", table="F_PREP_SPED", wave="C", min_rows=1,
        # DATA_BOLLA_SPED NON e' not_null: e' NULL by-design sulle righe scartate
        # (TIPO_SCAR 09/10, OP-PSP-1). Si usa DATA_PREL (partizione, sempre valorizzata dopo OP-PSP-2).
        not_null=["MAG_SITO_COD", "DATA_PREL", "ART_RADICE_COD"],
        unique_keys=["MAG_SITO_COD", "GIORNO_ORD_ID", "SOCIO_COD", "NUM_RIEP", "NUM_GABBIA",
                     "NUM_ORD", "ART_RADICE_COD", "ART_VAR_LOGIS_COD", "SEQ_PREL_PREP"],
        # ART_RADICE_COD escluso dagli orphan: ha residui fisiologici (articoli non nel
        # Retail Master) gated su OP-02 — vedi ACT_OP-32 / ACT_OP-02.
        orphan_fks=["MAG_SITO_COD"],
        orphan_max_pct=0.0,
        measures_nonneg=["QTA_PREP"],
        volume_max_dev_pct=30.0,
    ),
    "gold_f_turno_prep_sito": AcceptanceCriteria(
        pipeline="gold_f_turno_prep_sito", table="F_TURNO_PREP_SITO", wave="C", min_rows=1,
        not_null=["SITO_COD", "DATA_PREPARAZ", "PREPARATORE_COD"],
        unique_keys=["SITO_COD", "DATA_PREPARAZ", "PREPARATORE_COD", "RIEPILOGO_NRO"],
        orphan_fks=["SITO_COD", "PREPARATORE_COD"],   # PREPARATORE usa sentinel ND, non -1 (OP-28)
        orphan_max_pct=0.0,
        measures_nonneg=["ORE_LAVORATE", "ORE_PRODUTTIVE", "NUM_PREPARATI", "NUM_REFERENZE"],
        volume_max_dev_pct=30.0,
    ),
    "gold_f_giacenze_daily": AcceptanceCriteria(
        pipeline="gold_f_giacenze_daily", table="F_GIACENZE_DAILY", wave="B", min_rows=1,
        # Fact per MAG_COD (non per sito) e senza aggancio dimensionale -> nessun orphan check.
        not_null=["DATA_FOTO", "MAG_COD", "ART_COD_INTERNO"],
        unique_keys=["DATA_FOTO", "MAG_COD", "ART_COD_INTERNO"],
        measures_nonneg=["QTA_PEZZI", "QTA_UF"],   # VAL_STOCK non alimentato (ST-01)
        volume_max_dev_pct=25.0,
    ),
    "gold_f_trasporto": AcceptanceCriteria(
        pipeline="gold_f_trasporto", table="F_TRASPORTO", wave="D", min_rows=1,
        not_null=["MAG_SITO_COD", "DATA_BOLLA_SPED"],
        unique_keys=["SP_ID"],                      # grana movimento MTV (ADR-0013)
        orphan_fks=["MAG_SITO_COD", "VETTORE_SPED_COD"],
        orphan_max_pct=0.0,
        # Nessuna misura: il Gold non espone KM/COSTO_EUR (listini assenti);
        # LEAD_TIME_GG puo' essere negativo (date sorgente incoerenti) -> non nonneg.
        volume_max_dev_pct=30.0,
    ),
    "gold_f_tracciabilita_lotti": AcceptanceCriteria(
        pipeline="gold_f_tracciabilita_lotti", table="F_TRACCIABILITA_LOTTI", wave="E", min_rows=1,
        not_null=["SITO_COD", "DATA_CARICO", "MSI_COD"],
        unique_keys=["SITO_COD", "CARICO_NRO", "MSI_COD", "DATA_CARICO"],
        orphan_fks=["SITO_COD"],
        orphan_max_pct=0.0,
        measures_nonneg=["NUM_ETICHETTE", "NUM_SSCC", "NUM_ANNULLATE", "NUM_TRASFERITE_STAT"],
    ),
    "gold_f_ordini": AcceptanceCriteria(
        pipeline="gold_f_ordini", table="F_ORDINI", wave="D", min_rows=1,
        # Passthrough dal silver curated: colonne non verificabili offline -> criteri minimi
        # (esistenza + volume). not_null/unique_keys/orphan da calibrare al 1o run cloud.
        volume_max_dev_pct=30.0,
    ),
    "gold_f_movimentazione_carrellisti": AcceptanceCriteria(
        pipeline="gold_f_movimentazione_carrellisti", table="F_MOVIMENTAZIONE_CARRELLISTI",
        wave="E", min_rows=1,
        not_null=["CARRELLISTA_COD", "DATA_PRESENZA", "SITO_COD"],
        unique_keys=["CARRELLISTA_COD", "DATA_PRESENZA", "SITO_COD"],
        measures_nonneg=["NUM_MISSIONI", "NUM_PLT_MOVIMENTATI", "ORE_PRESENZA"],
    ),
    "gold_a_inbound_mensile": AcceptanceCriteria(
        pipeline="gold_a_inbound_mensile", table="A_INBOUND_MENSILE", schema="logistica_dm",
        wave="A-agg", min_rows=1,
        not_null=["FORNITORE_COD", "SITO_COD", "ANNO_MESE"],
        unique_keys=["FORNITORE_COD", "SITO_COD", "ANNO_MESE"],
        measures_nonneg=["QTA_ORDINATA_TOT", "QTA_CARICO_TOT"],  # AMMANCO puo' essere negativo
    ),
}
