"""
dq_helper.py — Data Quality checks per tabelle Delta Lake.

Fornisce check standardizzati su DataFrame PySpark con logging integrato
e possibilità di salvare il report DQ come tabella Delta.
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    FloatType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

try:
    from .logging_helper import Logger  # quando importato come package
except ImportError:
    from logging_helper import Logger   # quando lib e' direttamente sul sys.path


class DQHelper:
    """
    Esegue check di Data Quality su un DataFrame PySpark.

    I risultati vengono loggati tramite il Logger fornito e possono
    essere salvati in una tabella Delta per storicizzazione.

    Example::

        dq = DQHelper(spark, df=silver_df, table_name="silver.logistica.carichi_testate", logger=log)
        dq.check_no_nulls(["CARICO_ID", "DATA_CARICO"])
        dq.check_no_duplicates(["CARICO_ID"])
        report = dq.run_all([
            ("check_no_nulls", {"cols": ["CARICO_ID"]}),
            ("check_no_duplicates", {"key_cols": ["CARICO_ID"]}),
        ])
    """

    def __init__(
        self,
        spark: SparkSession,
        df: DataFrame,
        table_name: str,
        logger: Logger,
    ):
        """
        Args:
            spark: SparkSession attiva.
            df: DataFrame su cui eseguire i check.
            table_name: Nome fully-qualified della tabella (usato nei report).
            logger: Istanza Logger per output strutturato.
        """
        self.spark = spark
        self.df = df
        self.table_name = table_name
        self.logger = logger
        self._results: List[Dict[str, Any]] = []

    # ------------------------------------------------------------------
    # Utility interne
    # ------------------------------------------------------------------

    def _record(self, check_name: str, passed: bool, details: Dict[str, Any]) -> bool:
        """Registra risultato e logga."""
        self._results.append(
            {
                "table_name": self.table_name,
                "check_name": check_name,
                "passed": passed,
                "run_timestamp": datetime.now(tz=timezone.utc),
                **details,
            }
        )
        self.logger.log_dq_result(check_name, passed, details)
        return passed

    # ------------------------------------------------------------------
    # Check singoli
    # ------------------------------------------------------------------

    def check_no_nulls(self, cols: List[str]) -> bool:
        """
        Verifica che le colonne indicate non contengano valori NULL.

        Args:
            cols: Lista di colonne da controllare.

        Returns:
            True se nessuna delle colonne ha NULL.
        """
        null_counts: Dict[str, int] = {}
        for col in cols:
            cnt = self.df.filter(F.col(col).isNull()).count()
            if cnt > 0:
                null_counts[col] = cnt

        passed = len(null_counts) == 0
        return self._record(
            "check_no_nulls",
            passed,
            {"checked_cols": cols, "null_counts": null_counts},
        )

    def check_no_duplicates(self, key_cols: List[str]) -> bool:
        """
        Verifica che la combinazione di colonne chiave sia univoca.

        Args:
            key_cols: Lista di colonne che formano la chiave.

        Returns:
            True se non ci sono righe duplicate sulla chiave.
        """
        total = self.df.count()
        distinct = self.df.select(key_cols).distinct().count()
        duplicate_count = total - distinct
        passed = duplicate_count == 0
        return self._record(
            "check_no_duplicates",
            passed,
            {
                "key_cols": key_cols,
                "total_rows": total,
                "distinct_keys": distinct,
                "duplicate_count": duplicate_count,
            },
        )

    def check_row_count(self, expected_min: int) -> bool:
        """
        Verifica che il DataFrame abbia almeno ``expected_min`` righe.

        Args:
            expected_min: Numero minimo atteso di righe.

        Returns:
            True se il count >= expected_min.
        """
        actual = self.df.count()
        passed = actual >= expected_min
        return self._record(
            "check_row_count",
            passed,
            {"expected_min": expected_min, "actual_count": actual},
        )

    def check_numeric_range(
        self,
        col: str,
        min_val: Optional[float] = None,
        max_val: Optional[float] = None,
    ) -> bool:
        """
        Verifica che i valori di una colonna numerica rientrino nel range [min_val, max_val].
        I NULL vengono ignorati (usare check_no_nulls per verificarli separatamente).

        Args:
            col: Colonna numerica da controllare.
            min_val: Valore minimo consentito (None = nessun limite inferiore).
            max_val: Valore massimo consentito (None = nessun limite superiore).

        Returns:
            True se tutti i valori non-NULL sono nel range.
        """
        filter_expr = F.lit(False)
        if min_val is not None:
            filter_expr = filter_expr | (F.col(col) < F.lit(min_val))
        if max_val is not None:
            filter_expr = filter_expr | (F.col(col) > F.lit(max_val))

        out_of_range = self.df.filter(F.col(col).isNotNull() & filter_expr).count()
        passed = out_of_range == 0
        return self._record(
            "check_numeric_range",
            passed,
            {
                "col": col,
                "min_val": min_val,
                "max_val": max_val,
                "out_of_range_count": out_of_range,
            },
        )

    def check_referential(
        self,
        fk_col: str,
        ref_df: DataFrame,
        ref_col: str,
    ) -> float:
        """
        Verifica l'integrità referenziale di una FK verso una dimensione.

        Args:
            fk_col: Colonna FK nel DataFrame corrente.
            ref_df: DataFrame della dimensione di riferimento.
            ref_col: Colonna PK nella dimensione.

        Returns:
            Percentuale (0.0–1.0) di FK trovate nella dimensione.
            1.0 = integrità referenziale completa.
        """
        total = self.df.filter(F.col(fk_col).isNotNull()).count()
        if total == 0:
            self._record(
                "check_referential",
                True,
                {"fk_col": fk_col, "ref_col": ref_col, "match_rate": 1.0, "total_fk": 0},
            )
            return 1.0

        ref_keys = ref_df.select(F.col(ref_col).alias("_ref_key")).distinct()
        matched = (
            self.df
            .filter(F.col(fk_col).isNotNull())
            .join(ref_keys, F.col(fk_col) == F.col("_ref_key"), "left_semi")
            .count()
        )
        match_rate = matched / total
        # Soglia warning: < 98%
        passed = match_rate >= 0.98
        self._record(
            "check_referential",
            passed,
            {
                "fk_col": fk_col,
                "ref_col": ref_col,
                "total_fk": total,
                "matched": matched,
                "unmatched": total - matched,
                "match_rate": round(match_rate, 6),
            },
        )
        return match_rate

    # ------------------------------------------------------------------
    # Esecuzione batch
    # ------------------------------------------------------------------

    def run_all(self, checks: List[tuple]) -> Dict[str, Any]:
        """
        Esegue una lista di check in sequenza e restituisce un report aggregato.

        Args:
            checks: Lista di tuple (check_name, kwargs_dict).
                    Esempio::

                        [
                            ("check_no_nulls", {"cols": ["CARICO_ID"]}),
                            ("check_no_duplicates", {"key_cols": ["CARICO_ID"]}),
                            ("check_row_count", {"expected_min": 1}),
                        ]

        Returns:
            Dizionario con ``all_passed`` (bool), ``results`` (lista dettagli),
            ``passed_count`` e ``failed_count``.

        Raises:
            AttributeError: Se il check_name non corrisponde a un metodo esistente.
        """
        for check_name, kwargs in checks:
            method = getattr(self, check_name)
            method(**kwargs)

        passed = [r for r in self._results if r["passed"]]
        failed = [r for r in self._results if not r["passed"]]
        return {
            "all_passed": len(failed) == 0,
            "passed_count": len(passed),
            "failed_count": len(failed),
            "results": self._results,
        }

    # ------------------------------------------------------------------
    # Persistenza report
    # ------------------------------------------------------------------

    def save_report(self, output_table: str) -> None:
        """
        Salva i risultati DQ accumulati in una tabella Delta.

        La tabella viene creata se non esiste; le righe vengono aggiunte
        in append (per storicizzazione).

        Schema tabella::

            table_name      STRING
            check_name      STRING
            passed          BOOLEAN
            run_timestamp   TIMESTAMP
            details         STRING   (JSON serializzato dei dettagli)

        Args:
            output_table: Nome fully-qualified della tabella DQ
                          (es. "silver_dev.logistica.dq_results").
        """
        import json as _json  # noqa: PLC0415

        if not self._results:
            self.logger.warning("save_report: nessun risultato DQ da salvare.")
            return

        schema = StructType(
            [
                StructField("table_name", StringType(), nullable=False),
                StructField("check_name", StringType(), nullable=False),
                StructField("passed", BooleanType(), nullable=False),
                StructField("run_timestamp", TimestampType(), nullable=False),
                StructField("match_rate", FloatType(), nullable=True),
                StructField("details", StringType(), nullable=True),
            ]
        )

        rows = []
        for r in self._results:
            details_copy = {k: v for k, v in r.items() if k not in ("table_name", "check_name", "passed", "run_timestamp")}
            rows.append(
                (
                    r["table_name"],
                    r["check_name"],
                    r["passed"],
                    r["run_timestamp"],
                    float(r.get("match_rate", 0.0)) if r.get("match_rate") is not None else None,
                    _json.dumps(details_copy, default=str),
                )
            )

        report_df = self.spark.createDataFrame(rows, schema=schema)
        report_df.write.format("delta").mode("append").saveAsTable(output_table)
        self.logger.info(
            "Report DQ salvato",
            output_table=output_table,
            rows_saved=len(rows),
        )


# ─────────────────────────────────────────────────────────────────────────────
# API module-level (firma usata dai notebook Bronze: trasporti/*)
# ─────────────────────────────────────────────────────────────────────────────


def check_not_null(df: DataFrame, columns: List[str], notebook_name: str) -> None:
    """
    Verifica che ``columns`` non contengano NULL nel DataFrame.

    Logga un WARNING se ci sono violazioni ma NON solleva eccezioni: i notebook
    di ingestion Bronze sono tolleranti agli errori DQ (li riportano e proseguono).

    Args:
        df: DataFrame da controllare.
        columns: Lista di colonne che devono essere NOT NULL.
        notebook_name: Nome notebook chiamante (per il logging).
    """
    logger = Logger(notebook_name=notebook_name, area="logistica", layer="bronze")
    violations = []
    for col in columns:
        if col not in df.columns:
            logger.warning(f"check_not_null: colonna '{col}' non presente nel DataFrame")
            continue
        null_count = df.filter(F.col(col).isNull()).count()
        if null_count > 0:
            violations.append((col, null_count))
    if violations:
        for col, n in violations:
            logger.warning(
                f"check_not_null FAILED: {col} ha {n} NULL",
                event="dq_result", check_name=f"not_null_{col}",
                passed=False, null_count=n,
            )
    else:
        logger.info("check_not_null PASSED", event="dq_result", columns=columns, passed=True)


def check_row_count(df: DataFrame, min_rows: int, notebook_name: str) -> None:
    """
    Verifica che il DataFrame abbia almeno ``min_rows`` righe.

    Logga WARNING se la soglia non e' rispettata (no raise).

    Args:
        df: DataFrame da controllare.
        min_rows: Soglia minima.
        notebook_name: Nome notebook chiamante (per il logging).
    """
    logger = Logger(notebook_name=notebook_name, area="logistica", layer="bronze")
    n = df.count()
    if n < min_rows:
        logger.warning(
            f"check_row_count FAILED: {n} < min_rows={min_rows}",
            event="dq_result", check_name="row_count", passed=False,
            row_count=n, min_rows=min_rows,
        )
    else:
        logger.info(
            f"check_row_count PASSED: {n} righe",
            event="dq_result", check_name="row_count", passed=True, row_count=n,
        )


def check_orphan_rate(df: DataFrame, fk_col: str, notebook_name: str,
                      sentinel: Any = "-1", warn_threshold_pct: float = 5.0) -> float:
    """
    Misura l'orphan-rate di una FK dopo l'aggancio a una dimensione/lookup.

    Da usare DOPO ``utils.surrogate_key_fallback``: conta quante righe hanno la FK
    valorizzata al sentinella (default "-1" = riga SCONOSCIUTO) e logga la percentuale.
    Utile per accorgersi di disallineamenti di chiave verso le anagrafiche condivise.

    Args:
        df: DataFrame del fatto gia' agganciato.
        fk_col: Colonna FK da controllare (es. "ART_RADICE_COD").
        notebook_name: Nome notebook chiamante (logging).
        sentinel: Valore che marca le righe orfane (default "-1").
        warn_threshold_pct: Soglia oltre la quale il check e' WARNING (default 5%).

    Returns:
        L'orphan-rate in percentuale (float).
    """
    logger = Logger(notebook_name=notebook_name, area="logistica", layer="gold")
    total = df.count()
    orphans = df.filter(F.col(fk_col) == F.lit(sentinel)).count()
    rate = round(100.0 * orphans / total, 2) if total else 0.0
    passed = rate <= warn_threshold_pct
    logger._emit(
        Logger.LEVEL_INFO if passed else Logger.LEVEL_WARNING,
        f"orphan_rate {fk_col}: {rate}% ({orphans}/{total}) "
        f"{'OK' if passed else 'OLTRE SOGLIA ' + str(warn_threshold_pct) + '%'}",
        event="dq_result", check_name=f"orphan_rate_{fk_col}", passed=passed,
        orphans=orphans, total=total, orphan_rate_pct=rate,
    )
    return rate
