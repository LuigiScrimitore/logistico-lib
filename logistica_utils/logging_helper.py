"""
logging_helper.py — Logging strutturato JSON per notebook Databricks.

Ogni log message è una singola riga JSON stampata su stdout, compatibile
con il log aggregator di Databricks e con strumenti come Azure Monitor / Splunk.
"""

import json
import logging
import sys
import traceback
from datetime import datetime, timezone
from typing import Any, Optional


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """
    Factory standard usata da TUTTI i notebook (Bronze/Silver/Gold).

    Restituisce un ``logging.Logger`` stdlib configurato per stampare su stdout
    con un formato leggibile. Idempotente: piu' chiamate con lo stesso ``name``
    restituiscono lo stesso logger senza duplicare handler.

    Args:
        name: Nome del logger (tipicamente il nome del notebook, es. "bronze_tabgen").
        level: Livello (default INFO).

    Returns:
        ``logging.Logger`` configurato. I metodi disponibili sono quelli standard:
        ``.info()``, ``.warning()``, ``.error(exc_info=...)``, ``.debug()``, ...
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        ))
        logger.addHandler(handler)
        logger.propagate = False
    logger.setLevel(level)
    return logger


class Logger:
    """
    Logger strutturato JSON per notebook e job Databricks.

    Ogni chiamata stampa una riga JSON su stdout con campi standard:
    timestamp, level, notebook, area, layer, message + kwargs addizionali.

    Example::

        log = Logger("bronze_carichi", area="logistica", layer="bronze")
        log.info("Avvio ingestion", source="oracle-logistica", table="CARICHI_TESTATE")
        log.log_run_start("CARICHI_TESTATE", "bronze.logistica.carichi_testate", "2026-05-29")
    """

    LEVEL_INFO = "INFO"
    LEVEL_WARNING = "WARNING"
    LEVEL_ERROR = "ERROR"
    LEVEL_DEBUG = "DEBUG"

    def __init__(self, notebook_name: str, area: str, layer: str):
        """
        Args:
            notebook_name: Nome del notebook corrente (es. "bronze_carichi").
            area: Area funzionale (es. "logistica", "acquisti", "produzione").
            layer: Layer Medallion (es. "bronze", "silver", "gold").
        """
        self.notebook_name = notebook_name
        self.area = area
        self.layer = layer
        self._run_id: Optional[str] = None

    # ------------------------------------------------------------------
    # Metodi interni
    # ------------------------------------------------------------------

    def _now_iso(self) -> str:
        return datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    def _emit(self, level: str, message: str, **kwargs: Any) -> None:
        record: dict[str, Any] = {
            "timestamp": self._now_iso(),
            "level": level,
            "notebook": self.notebook_name,
            "area": self.area,
            "layer": self.layer,
            "message": message,
        }
        if self._run_id is not None:
            record["run_id"] = self._run_id
        if kwargs:
            record.update(kwargs)
        print(json.dumps(record, default=str, ensure_ascii=False), flush=True)

    # ------------------------------------------------------------------
    # API pubblica
    # ------------------------------------------------------------------

    def debug(self, msg: str, **kwargs: Any) -> None:
        """Log a livello DEBUG."""
        self._emit(self.LEVEL_DEBUG, msg, **kwargs)

    def info(self, msg: str, **kwargs: Any) -> None:
        """
        Log a livello INFO.

        Args:
            msg: Messaggio principale.
            **kwargs: Campi aggiuntivi serializzati nel JSON (es. rows=1000, table="foo").
        """
        self._emit(self.LEVEL_INFO, msg, **kwargs)

    def warning(self, msg: str, **kwargs: Any) -> None:
        """Log a livello WARNING."""
        self._emit(self.LEVEL_WARNING, msg, **kwargs)

    def error(self, msg: str, exception: Optional[BaseException] = None, **kwargs: Any) -> None:
        """
        Log a livello ERROR.

        Args:
            msg: Messaggio di errore.
            exception: Eccezione opzionale; se fornita aggiunge 'exception_type',
                       'exception_message' e 'traceback' al record JSON.
            **kwargs: Campi aggiuntivi.
        """
        if exception is not None:
            kwargs["exception_type"] = type(exception).__name__
            kwargs["exception_message"] = str(exception)
            kwargs["traceback"] = traceback.format_exc()
        self._emit(self.LEVEL_ERROR, msg, **kwargs)

    # ------------------------------------------------------------------
    # Log strutturati di processo
    # ------------------------------------------------------------------

    def log_run_start(
        self,
        source_table: str,
        target_table: str,
        run_date: str,
    ) -> None:
        """
        Logga l'avvio di un run di ingestion/trasformazione.

        Args:
            source_table: Tabella/oggetto sorgente (es. "ORACLE.LOGISTICA.CARICHI_TESTATE").
            target_table: Tabella Delta target (es. "bronze_dev.logistica.carichi_testate").
            run_date: Data di run in formato YYYY-MM-DD.
        """
        self._run_id = f"{self.notebook_name}_{run_date}"
        self.info(
            "Run avviato",
            event="run_start",
            source_table=source_table,
            target_table=target_table,
            run_date=run_date,
        )

    def log_run_end(
        self,
        rows_read: int,
        rows_written: int,
        duration_seconds: float,
    ) -> None:
        """
        Logga la conclusione di un run.

        Args:
            rows_read: Numero di righe lette dalla sorgente.
            rows_written: Numero di righe scritte/aggiornate nel target.
            duration_seconds: Durata totale del run in secondi.
        """
        self.info(
            "Run completato",
            event="run_end",
            rows_read=rows_read,
            rows_written=rows_written,
            duration_seconds=round(duration_seconds, 3),
            rows_per_second=(
                round(rows_read / duration_seconds, 1) if duration_seconds > 0 else None
            ),
        )

    def log_dq_result(
        self,
        check_name: str,
        passed: bool,
        details: Optional[dict] = None,
    ) -> None:
        """
        Logga il risultato di un singolo check di Data Quality.

        Args:
            check_name: Nome del check (es. "check_no_duplicates").
            passed: True se il check è passato.
            details: Dizionario opzionale con dettagli (es. {"null_count": 5}).
        """
        level = self.LEVEL_INFO if passed else self.LEVEL_WARNING
        self._emit(
            level,
            f"DQ check {'PASSED' if passed else 'FAILED'}: {check_name}",
            event="dq_result",
            check_name=check_name,
            passed=passed,
            **(details or {}),
        )
