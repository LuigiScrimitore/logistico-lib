"""
dq_monitor.py — DQ & alerting INTERNI (release kit KIT-03/04).

Costruito sopra ``dq_helper``. Aggiunge:
  - **Persistenza canonica** degli esiti in ``control_<env>.etl.dq_results``
    (stessa area del watermark OP-35), con pipeline/wave/run_date/severità.
  - **Severità** INFO / WARNING / BLOCKING (BLOCKING ferma la pipeline).
  - **Volume-anomaly**: confronto del row_count corrente vs media storica
    (dalla stessa tabella dq_results) → segnala deviazioni anomale.
  - **Alerting pluggable**: ``LogNotifier`` ora; ``WebhookNotifier``/email in
    cloud (config via secrets, KIT-04). Nessuna dipendenza esterna richiesta ora.
  - **gate()**: solleva ``DQBlockingError`` se almeno un check BLOCKING fallisce.

NB: soluzione interna, indipendente da un eventuale modello DQ del cliente
(OP-21). Se pronta prima, candidabile a standard.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, DateType, DoubleType, StringType,
    StructField, StructType, TimestampType,
)

try:
    from .utils import get_catalog
except ImportError:  # lib direttamente sul sys.path (runner locale)
    from utils import get_catalog


class Severity(str, Enum):
    INFO = "INFO"          # informativo, nessuna azione
    WARNING = "WARNING"    # anomalia da monitorare, non blocca
    BLOCKING = "BLOCKING"  # ferma la pipeline (gate)


class DQBlockingError(Exception):
    """Sollevata da DQMonitor.gate() quando un check BLOCKING fallisce."""


DQ_TABLE_SCHEMA = StructType([
    StructField("run_id",        StringType(),    False),
    StructField("env",           StringType(),    False),
    StructField("pipeline",      StringType(),    False),
    StructField("wave",          StringType(),    True),
    StructField("layer",         StringType(),    True),
    StructField("table_name",    StringType(),    True),
    StructField("check_name",    StringType(),    False),
    StructField("severity",      StringType(),    False),
    StructField("passed",        BooleanType(),   False),
    StructField("metric_value",  DoubleType(),    True),
    StructField("threshold",     DoubleType(),    True),
    StructField("run_date",      DateType(),      True),
    StructField("run_timestamp", TimestampType(), False),
    StructField("details",       StringType(),    True),
])


def get_dq_table(env: str) -> str:
    """FQN tabella esiti DQ: ``control_<env>.etl.dq_results`` (locale: ``control_<env>_etl.dq_results``)."""
    return f"{get_catalog('control', env)}.etl.dq_results"


def ensure_dq_table(spark: SparkSession, env: str) -> str:
    """Crea (se assenti) schema ``etl`` e tabella ``dq_results``. Idempotente. Ritorna l'FQN."""
    control = get_catalog("control", env)
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {control}.etl")
    fqn = f"{control}.etl.dq_results"
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {fqn} (
            run_id STRING, env STRING, pipeline STRING, wave STRING, layer STRING,
            table_name STRING, check_name STRING, severity STRING, passed BOOLEAN,
            metric_value DOUBLE, threshold DOUBLE, run_date DATE,
            run_timestamp TIMESTAMP, details STRING
        ) USING delta
    """)
    return fqn


# ─────────────────────────────────────────────────────────────────────────────
# Alerting pluggable
# ─────────────────────────────────────────────────────────────────────────────
class Notifier:
    """Interfaccia notifica. Implementazioni: LogNotifier (ora), WebhookNotifier (cloud)."""
    def notify(self, subject: str, body: str, severity: "Severity") -> None:  # pragma: no cover
        raise NotImplementedError


class LogNotifier(Notifier):
    """Alerting di default: scrive nel logger (o stdout). Sempre disponibile, zero dipendenze."""
    def __init__(self, logger: Any = None):
        self.logger = logger

    def notify(self, subject: str, body: str, severity: "Severity") -> None:
        msg = f"[ALERT/{severity.value}] {subject} :: {body}"
        if self.logger is not None:
            (self.logger.info if severity == Severity.INFO else self.logger.warning)(msg)
        else:
            print(msg, flush=True)


class WebhookNotifier(Notifier):
    """Alerting via webhook (Teams/Slack) — da attivare in CLOUD (KIT-04).

    URL da secret; qui solo interfaccia. Evita dipendenze/side-effect in locale.
    """
    def __init__(self, url: str, min_severity: "Severity" = Severity.WARNING):
        self.url = url
        self.min_severity = min_severity

    def notify(self, subject: str, body: str, severity: "Severity") -> None:  # pragma: no cover
        raise NotImplementedError(
            "WebhookNotifier: attivare in cloud (requests.post con URL da secret) — KIT-04."
        )


# ─────────────────────────────────────────────────────────────────────────────
# Monitor
# ─────────────────────────────────────────────────────────────────────────────
class DQMonitor:
    """Raccoglie esiti DQ di una pipeline, li persiste, allerta e applica il gate.

    Example::

        dq = DQMonitor(spark, pipeline="gold_f_carico", env="dev",
                       run_date="2026-06-10", wave="A")
        dq.record("orphan_ART_RADICE_COD", passed=True, severity=Severity.BLOCKING,
                  layer="gold", table_name="F_CARICO", metric_value=0.0, threshold=0.0)
        dq.check_volume_anomaly("F_CARICO", current_count=59621, layer="gold")
        dq.persist()
        dq.gate()   # -> DQBlockingError se un BLOCKING e' fallito (+ alert)
    """

    def __init__(self, spark: SparkSession, pipeline: str, env: str = "dev",
                 run_date: Optional[str] = None, wave: Optional[str] = None,
                 notifier: Optional[Notifier] = None, logger: Any = None,
                 run_id: Optional[str] = None):
        self.spark = spark
        self.pipeline = pipeline
        self.env = env
        self.run_date = run_date
        self.wave = wave
        self.logger = logger
        self.notifier = notifier or LogNotifier(logger)
        self._ts = datetime.now(tz=timezone.utc)
        self.run_id = run_id or f"{pipeline}_{self._ts.strftime('%Y%m%dT%H%M%S')}"
        self._results: List[Dict[str, Any]] = []

    # ------------------------------------------------------------------
    def record(self, check_name: str, passed: bool,
               severity: "Severity" = Severity.WARNING,
               layer: Optional[str] = None, table_name: Optional[str] = None,
               metric_value: Optional[float] = None, threshold: Optional[float] = None,
               details: Optional[Dict[str, Any]] = None) -> bool:
        """Registra un esito DQ (in memoria; persistito da persist())."""
        sev = Severity(severity)
        rec = {
            "run_id": self.run_id, "env": self.env, "pipeline": self.pipeline,
            "wave": self.wave, "layer": layer, "table_name": table_name,
            "check_name": check_name, "severity": sev.value, "passed": bool(passed),
            "metric_value": float(metric_value) if metric_value is not None else None,
            "threshold": float(threshold) if threshold is not None else None,
            "run_date": self.run_date, "run_timestamp": self._ts,
            "details": json.dumps(details or {}, default=str),
        }
        self._results.append(rec)
        line = (f"DQ [{sev.value}] {self.pipeline}.{check_name} "
                f"= {'PASS' if passed else 'FAIL'}"
                + (f" ({table_name})" if table_name else ""))
        if self.logger is not None:
            (self.logger.info if passed else self.logger.warning)(line)
        else:
            print(line, flush=True)
        return passed

    # ------------------------------------------------------------------
    def check_volume_anomaly(self, table_name: str, current_count: int,
                             layer: str = "gold", max_dev_pct: float = 30.0,
                             min_history: int = 3,
                             severity: "Severity" = Severity.WARNING) -> bool:
        """Confronta il row_count corrente con la media storica (da dq_results).

        Registra check ``volume``. PASS se |dev%| <= max_dev_pct o storico insufficiente.
        """
        fqn = get_dq_table(self.env)
        hist: List[float] = []
        if self.spark.catalog.tableExists(fqn):
            rows = (self.spark.table(fqn)
                    .filter((F.col("pipeline") == self.pipeline) &
                            (F.col("table_name") == table_name) &
                            (F.col("check_name") == "volume") &
                            F.col("metric_value").isNotNull())
                    .orderBy(F.col("run_timestamp").desc()).limit(30)
                    .select("metric_value").collect())
            hist = [r["metric_value"] for r in rows]

        if len(hist) < min_history:
            return self.record("volume", True, Severity.INFO, layer, table_name,
                               float(current_count), None,
                               {"note": "storico insufficiente", "n_hist": len(hist)})
        avg = sum(hist) / len(hist)
        dev = 100.0 * abs(current_count - avg) / avg if avg else 0.0
        passed = dev <= max_dev_pct
        return self.record("volume", passed,
                           Severity.INFO if passed else severity,
                           layer, table_name, float(current_count), max_dev_pct,
                           {"avg": round(avg, 1), "dev_pct": round(dev, 1), "n_hist": len(hist)})

    # ------------------------------------------------------------------
    def persist(self) -> str:
        """Append degli esiti accumulati su ``control_<env>.etl.dq_results``."""
        from datetime import date as _date
        fqn = ensure_dq_table(self.spark, self.env)
        if not self._results:
            return fqn

        def _as_date(v):
            if v is None or isinstance(v, _date):
                return v
            try:
                return _date.fromisoformat(str(v)[:10])
            except ValueError:
                return None

        rows = [(
            r["run_id"], r["env"], r["pipeline"], r["wave"], r["layer"],
            r["table_name"], r["check_name"], r["severity"], r["passed"],
            r["metric_value"], r["threshold"],
            _as_date(r["run_date"]),
            r["run_timestamp"], r["details"],
        ) for r in self._results]
        df = self.spark.createDataFrame(rows, DQ_TABLE_SCHEMA)
        df.write.format("delta").mode("append").saveAsTable(fqn)
        return fqn

    # ------------------------------------------------------------------
    def blocking_failures(self) -> List[Dict[str, Any]]:
        return [r for r in self._results
                if (not r["passed"]) and r["severity"] == Severity.BLOCKING.value]

    def failures(self) -> List[Dict[str, Any]]:
        return [r for r in self._results if not r["passed"]]

    def alert_on_failures(self) -> None:
        """Invia una notifica per ogni check fallito (via notifier)."""
        for r in self.failures():
            self.notifier.notify(
                subject=f"DQ {r['severity']} {self.pipeline}.{r['check_name']}",
                body=r["details"],
                severity=Severity(r["severity"]),
            )

    def gate(self) -> None:
        """Alert su tutti i fallimenti + raise se almeno un BLOCKING e' fallito."""
        self.alert_on_failures()
        blk = self.blocking_failures()
        if blk:
            raise DQBlockingError(
                f"{len(blk)} check BLOCKING falliti in {self.pipeline}: "
                + ", ".join(r["check_name"] for r in blk)
            )

    def summary(self) -> Dict[str, Any]:
        fails = self.failures()
        return {
            "pipeline": self.pipeline, "run_id": self.run_id,
            "total": len(self._results), "failed": len(fails),
            "blocking_failed": len(self.blocking_failures()),
            "all_passed": len(fails) == 0,
        }
