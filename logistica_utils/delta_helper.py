"""
delta_helper.py — Wrapper per operazioni Delta Lake su Unity Catalog.

Fornisce MERGE INTO, replaceWhere, append con dedup e utility di inspection.
Tutte le operazioni lavorano sul catalogo/schema configurati nell'istanza.
"""

from typing import List, Optional, Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable  # type: ignore[import]


def _build_fqn(catalog: str, schema: str, table: str) -> str:
    """Costruisce l'FQN Delta gestendo il catalog vuoto (OP-TST-2).

    - catalog valorizzato → ``catalog.schema.table`` (Unity Catalog, 3 livelli).
    - catalog vuoto/None  → ``schema.table`` (namespace a 2 parti, es. lo
      ``spark_catalog`` locale usato nei test).

    Senza questo guard, un catalog vuoto produce ``.schema.table`` con punto
    iniziale, che Spark rifiuta con ParseException / REQUIRES_SINGLE_PART_NAMESPACE.
    """
    if catalog:
        return f"{catalog}.{schema}.{table}"
    return f"{schema}.{table}"


class DeltaHelper:
    """
    Helper per operazioni Delta Lake con Unity Catalog.

    Tutte le operazioni assumono che ``catalog.schema.table`` sia la forma
    fully-qualified usata in Unity Catalog.

    Example::

        dh = DeltaHelper(spark, catalog="silver_dev", schema="logistica")
        dh.merge_into(
            "carichi_testate",
            source_df,
            merge_keys=["CARICO_ID"],
        )
    """

    def __init__(self, spark: SparkSession, catalog: str, schema: str):
        """
        Args:
            spark: SparkSession attiva.
            catalog: Nome del catalogo Unity Catalog (es. "bronze_dev", "silver_prod").
            schema: Nome dello schema (es. "logistica", "condiviso").
        """
        self.spark = spark
        self.catalog = catalog
        self.schema = schema

    # ------------------------------------------------------------------
    # Utility interne
    # ------------------------------------------------------------------

    def _fqn(self, table_name: str) -> str:
        """Restituisce il nome fully-qualified `catalog.schema.table`
        (o `schema.table` se il catalog e' vuoto — vedi _build_fqn, OP-TST-2)."""
        return _build_fqn(self.catalog, self.schema, table_name)

    # ------------------------------------------------------------------
    # Lettura / Ispezione
    # ------------------------------------------------------------------

    def table_exists(self, table_name: str) -> bool:
        """
        Verifica se una tabella Delta esiste nel catalogo/schema configurato.

        Args:
            table_name: Nome della tabella (senza catalog/schema).

        Returns:
            True se la tabella esiste.
        """
        fqn = self._fqn(table_name)
        try:
            self.spark.sql(f"DESCRIBE TABLE {fqn}")
            return True
        except Exception:
            return False

    def get_max_watermark(self, table_name: str, watermark_col: str) -> Any:
        """
        Restituisce il valore massimo della colonna watermark.

        Usato per l'ingestion incrementale: carica solo righe con
        watermark > last_watermark.

        Args:
            table_name: Nome tabella Delta.
            watermark_col: Colonna watermark (timestamp o intero).

        Returns:
            Valore massimo (tipo nativo Python) oppure None se la tabella
            non esiste o è vuota.
        """
        if not self.table_exists(table_name):
            return None
        fqn = self._fqn(table_name)
        row = self.spark.sql(
            f"SELECT MAX({watermark_col}) AS max_wm FROM {fqn}"
        ).collect()[0]
        return row["max_wm"]

    # ------------------------------------------------------------------
    # Scrittura
    # ------------------------------------------------------------------

    def merge_into(
        self,
        target_table: str,
        source_df: DataFrame,
        merge_keys: List[str],
        update_cols: Optional[List[str]] = None,
    ) -> None:
        """
        Esegue un MERGE INTO Delta (upsert) tra source_df e la tabella target.

        - WHEN MATCHED: aggiorna le colonne specificate (o tutte se update_cols=None).
        - WHEN NOT MATCHED: inserisce la nuova riga.

        Se la tabella target non esiste viene creata automaticamente con
        ``source_df.write.format("delta").saveAsTable()``.

        Args:
            target_table: Nome tabella target (senza catalog/schema).
            source_df: DataFrame sorgente con i dati aggiornati.
            merge_keys: Lista di colonne che formano la chiave di merge
                        (es. ["CARICO_ID"] o ["FORNITORE_ID", "DATA"]).
            update_cols: Lista di colonne da aggiornare nel MATCHED.
                         Se None vengono aggiornate tutte le colonne del source.

        Raises:
            RuntimeError: Se il merge fallisce per motivi non gestiti.
        """
        fqn = self._fqn(target_table)

        # Crea la tabella se non esiste
        if not self.table_exists(target_table):
            source_df.write.format("delta").mode("overwrite").saveAsTable(fqn)
            return

        delta_target = DeltaTable.forName(self.spark, fqn)

        # Costruisci la join condition
        condition = " AND ".join(
            [f"target.{k} = source.{k}" for k in merge_keys]
        )

        # Colonne da aggiornare (escluse le merge keys per evitare ridondanza)
        if update_cols is None:
            update_cols = [c for c in source_df.columns if c not in merge_keys]

        update_map = {col: f"source.{col}" for col in update_cols}
        insert_map = {col: f"source.{col}" for col in source_df.columns}

        (
            delta_target.alias("target")
            .merge(source_df.alias("source"), condition)
            .whenMatchedUpdate(set=update_map)
            .whenNotMatchedInsert(values=insert_map)
            .execute()
        )

    def replace_where(
        self,
        target_table: str,
        source_df: DataFrame,
        partition_col: str,
        partition_value: Any,
    ) -> None:
        """
        Sovrascrive una singola partizione della tabella Delta (replaceWhere).

        Ideale per il layer Gold dove ogni run ricalcola una partizione data.

        Args:
            target_table: Nome tabella target (senza catalog/schema).
            source_df: DataFrame con i dati della partizione.
            partition_col: Colonna di partizionamento (es. "DATA_RIFERIMENTO").
            partition_value: Valore della partizione (es. "2026-05-29").
        """
        fqn = self._fqn(target_table)
        filter_expr = f"{partition_col} = '{partition_value}'"

        (
            source_df.write
            .format("delta")
            .mode("overwrite")
            .option("replaceWhere", filter_expr)
            .saveAsTable(fqn)
        )

    def append_bronze(
        self,
        target_table: str,
        source_df: DataFrame,
        watermark_col: Optional[str] = None,
    ) -> None:
        """
        Append-only verso Bronze con deduplication opzionale sul watermark.

        Se watermark_col è specificata, prima dell'append rimuove eventuali
        duplicati su quella colonna tenendo la riga con watermark più alto
        (protezione da double-run).

        Args:
            target_table: Nome tabella target (senza catalog/schema).
            source_df: DataFrame con i nuovi dati da aggiungere.
            watermark_col: Colonna timestamp/ID usata per deduplication.
                           Se None non viene eseguita dedup.
        """
        fqn = self._fqn(target_table)

        if watermark_col is not None:
            from pyspark.sql.window import Window  # noqa: PLC0415
            # Dedup intra-batch: per ogni valore di watermark_col tieni una sola riga
            source_df = (
                source_df
                .withColumn(
                    "_row_rank",
                    F.row_number().over(
                        Window.partitionBy(watermark_col)
                        .orderBy(F.col(watermark_col).desc())
                    ),
                )
                .filter(F.col("_row_rank") == 1)
                .drop("_row_rank")
            )

        (
            source_df.write
            .format("delta")
            .mode("append")
            .saveAsTable(fqn)
        )


# ─────────────────────────────────────────────────────────────────────────────
# API module-level (firma usata dai notebook Bronze: trasporti/*)
# ─────────────────────────────────────────────────────────────────────────────

_DEFAULT_WATERMARK = "1900-01-01"


def init_delta_table(spark: SparkSession, df: DataFrame, full_table_name: str,
                     partition_cols: Optional[List[str]] = None) -> None:
    """
    Inizializza la tabella Delta target se non esiste, con lo schema di ``df``
    e l'eventuale partizionamento. Operazione idempotente: se la tabella esiste
    e' un no-op.

    Args:
        spark: SparkSession attiva.
        df: DataFrame con lo schema desiderato (i dati vengono ignorati: viene
            scritto solo schema vuoto).
        full_table_name: FQN ``catalog.schema.table``.
        partition_cols: Colonne di partizionamento (default: nessuna).
    """
    # Already exists?
    try:
        spark.sql(f"DESCRIBE TABLE {full_table_name}")
        return
    except Exception:
        pass

    writer = df.limit(0).write.format("delta").mode("ignore")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.saveAsTable(full_table_name)


def get_watermark(catalog: str, schema: str, table: str, watermark_col: str,
                  spark: Optional[SparkSession] = None) -> str:
    """
    Restituisce il watermark corrente per (catalog, schema, table) come stringa
    ``YYYY-MM-DD``. Se la tabella non esiste o e' vuota, ritorna ``1900-01-01``.

    Args:
        catalog: Catalogo Unity Catalog (es. "bronze_dev").
        schema: Schema (es. "logistica").
        table: Nome tabella (senza catalog/schema).
        watermark_col: Colonna timestamp/data sorgente del watermark.
        spark: SparkSession (se None usa ``SparkSession.getActiveSession()``).

    Returns:
        Watermark come stringa YYYY-MM-DD.
    """
    if spark is None:
        spark = SparkSession.getActiveSession()  # type: ignore[assignment]
        if spark is None:
            raise RuntimeError("Nessuna SparkSession attiva per get_watermark")
    fqn = _build_fqn(catalog, schema, table)
    try:
        spark.sql(f"DESCRIBE TABLE {fqn}")
    except Exception:
        return _DEFAULT_WATERMARK
    row = spark.sql(
        f"SELECT MAX({watermark_col}) AS wm FROM {fqn}"
    ).collect()
    if not row or row[0]["wm"] is None:
        return _DEFAULT_WATERMARK
    return str(row[0]["wm"])[:10]  # YYYY-MM-DD


def update_watermark(catalog: str, schema: str, table: str, watermark_col: str,
                     new_value: Any) -> None:
    """
    No-op nel modello attuale: il watermark e' derivato da ``MAX(col)`` sulla
    tabella Delta stessa, quindi non serve un side-store dedicato. Mantenuta
    per compatibilita' con la firma usata nei notebook (e per allinearsi a
    future implementazioni con tabella di stato esterna).
    """
    # Per ora: nessuna azione. Se in futuro si introdurra' una tabella di stato
    # (es. ``meta.watermarks``), questa funzione conterra' l'UPSERT.
    return None
