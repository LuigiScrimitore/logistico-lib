"""
utils.py — Funzioni di utilità condivise per il progetto Logistico 2.0.

Contiene helper per gestione date, naming cataloghi, surrogate keys,
cast di tipo e arricchimento metadati.
"""

from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType


# ---------------------------------------------------------------------------
# Configurazione cataloghi per ambiente
# ---------------------------------------------------------------------------

_CATALOG_MAP: Dict[str, Dict[str, str]] = {
    # dev e prod separati e speculari su TUTTI i layer (Gold incluso).
    # D1 (2026-07-02): catalog controllo = config_dev (non control_dev — allineato al DWH aziendale).
    # D4 (CONFERMATO 2026-07-02): prod = _prod, stage = _stage (non configurato per ora).
    #   I catalog senza suffisso saranno eliminati dal cliente — non usarli.
    # D2 (2026-07-02): schema anagrafiche condivise = bronze_<env>.condiviso (push cdt_dw).
    #   -> usare get_condiviso_schema(env) per il fully-qualified "bronze_dev.condiviso".
    "dev": {
        "bronze": "bronze_dev",
        "silver": "silver_dev",
        "gold": "gold_dev",
        "control": "config_dev",   # D1: allineato a config_dev (catalog DWH esistente)
    },
    "prod": {
        "bronze": "bronze_prod",   # D4 confermato 2026-07-02
        "silver": "silver_prod",
        "gold": "gold_prod",
        "control": "config_prod",  # D4 confermato 2026-07-02 (speculare a config_dev)
    },
}


def get_condiviso_schema(env: str = "dev") -> str:
    """Ritorna lo schema fully-qualified per le anagrafiche condivise LU_* (D2).

    Decisione D2 (2026-07-02): schema proprio ``bronze_<env>.condiviso``, popolato
    dal push cdt_dw. Isolamento totale dal DWH. In futuro, quando le anagrafiche
    saranno disponibili su Gold, si potrà agganciare direttamente.

    Examples::

        get_condiviso_schema("dev")  -> "bronze_dev.condiviso"
        get_condiviso_schema("prod") -> "bronze_prod.condiviso"
    """
    return f"{get_catalog('bronze', env)}.condiviso"


def get_catalog(layer_or_env: str, env: Optional[str] = None) -> Any:
    """
    Restituisce il nome del catalogo Unity Catalog.

    Supporta due firme per compatibilità:

    - ``get_catalog(layer, env)`` -> ``str``: nome catalogo per il layer specifico.
      Es. ``get_catalog("bronze", "dev")`` -> ``"bronze_dev"``.
      E' la forma usata da tutti i notebook v3.0 (Bronze/Silver/Gold).

    - ``get_catalog(env)`` -> ``dict[str, str]``: dizionario completo dei cataloghi.
      Es. ``get_catalog("dev")`` -> ``{"bronze": "bronze_dev", "silver": "silver_dev", "gold": "gold_dev", "control": "control_dev"}``.
      Mantenuto per backward compatibility.

    Args:
        layer_or_env: "bronze"|"silver"|"gold"|"control" (firma a 2 arg) oppure "dev"|"prod" (firma a 1 arg).
        env: Ambiente target ("dev"|"prod"). Se valorizzato, ``layer_or_env`` e' il layer.

    Returns:
        ``str`` con il nome catalogo (firma a 2 arg) oppure ``dict`` completo (firma a 1 arg).

    Raises:
        ValueError: Se layer o env non sono tra i valori supportati.
    """
    if env is None:
        # Firma vecchia: get_catalog(env) -> dict
        env_val = layer_or_env.lower().strip()
        if env_val not in _CATALOG_MAP:
            raise ValueError(
                f"Ambiente '{env_val}' non riconosciuto. Valori supportati: {list(_CATALOG_MAP.keys())}"
            )
        return dict(_CATALOG_MAP[env_val])

    # Firma nuova: get_catalog(layer, env) -> str
    layer = layer_or_env.lower().strip()
    env_val = env.lower().strip()
    if env_val not in _CATALOG_MAP:
        raise ValueError(
            f"Ambiente '{env_val}' non riconosciuto. Valori supportati: {list(_CATALOG_MAP.keys())}"
        )
    if layer not in _CATALOG_MAP[env_val]:
        raise ValueError(
            f"Layer '{layer}' non riconosciuto. Valori supportati: {list(_CATALOG_MAP[env_val].keys())}"
        )
    return _CATALOG_MAP[env_val][layer]


# ---------------------------------------------------------------------------
# Gestione date di run
# ---------------------------------------------------------------------------

def get_run_date(spark: SparkSession, widget_name: str = "run_date") -> str:
    """
    Restituisce la data di run in formato YYYY-MM-DD.

    Prima tenta di leggere il widget Databricks ``run_date``; se non
    disponibile (test locale o widget non impostato) usa la data corrente UTC.

    Args:
        spark: SparkSession attiva (usata per accedere a dbutils via IPython).
        widget_name: Nome del widget Databricks da leggere. Default: "run_date".

    Returns:
        Data in formato "YYYY-MM-DD".
    """
    try:
        import IPython  # noqa: PLC0415
        shell = IPython.get_ipython()
        if shell is not None:
            dbutils = shell.user_ns.get("dbutils")
            if dbutils is not None:
                value = dbutils.widgets.get(widget_name)
                if value and value.strip():
                    return value.strip()
    except Exception:
        pass

    # Fallback: data corrente UTC
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# Conversione Julian Day Number -> date calendario
# ---------------------------------------------------------------------------

# Offset tra Julian Day Number e Unix epoch: JDN 2440588 = 1970-01-01.
# Consistente con il landing simulator (_date_bind usa toordinal()+1721425,
# e toordinal(1970-01-01)+1721425 == 2440588), quindi la conversione e'
# coerente in andata (estrazione) e ritorno (lettura Silver).
_JULIAN_UNIX_EPOCH_OFFSET = 2440588


def julian_to_date(col: "F.Column", null_if_non_positive: bool = True) -> "F.Column":
    """
    Converte una colonna contenente un Julian Day Number (NUMBER legacy Oracle,
    es. ``TO_DATE(n,'J')``) in una colonna ``DateType`` Spark.

    Le sorgenti Logistix/STAT memorizzano molte date come JDN numerico
    (es. 2461201 = 2026-06-09). Un ``cast("date")`` diretto su questo numero
    produce date assurde (anno 2.461.201) e fa overflow nelle aggregazioni a valle.
    Questa funzione applica la conversione corretta JDN -> date calendario.

    Args:
        col: Colonna sorgente (numerica o stringa numerica) con il JDN.
        null_if_non_positive: se True (default) i valori null, 0 o negativi
            diventano NULL invece di una data improbabile (sentinella legacy
            tipo 0 o NULL = "data assente").

    Returns:
        Colonna ``DateType``. NULL dove il JDN non e' valido.

    Example::

        # invece di:  F.col("STCAR_DATA_CARICO").cast("date")
        df.withColumn("DATA_CARICO", julian_to_date(F.col("STCAR_DATA_CARICO")))
    """
    j = col.cast("long")
    if null_if_non_positive:
        j = F.when(j.isNull() | (j <= 0), None).otherwise(j)
    days_from_epoch = (j - F.lit(_JULIAN_UNIX_EPOCH_OFFSET)).cast("int")
    return F.date_add(F.lit("1970-01-01").cast("date"), days_from_epoch)


def clean_dat_d(col: "F.Column") -> "F.Column":
    """
    Replica ``FN_CLEAN_DAT_D`` (CDT_ESTR.sql:147777): DATE/timestamp -> intero YYYYMMDD.
    Default su null/errore = 0 (sentinella legacy "data assente").
    Per ottenere un DateType usare invece il cast diretto; questa restituisce l'intero
    YYYYMMDD usato come chiave-giorno nel legacy (GIORNO_*_ID).
    """
    return F.coalesce(F.date_format(col.cast("date"), "yyyyMMdd").cast("int"), F.lit(0))


def clean_dat_v(col: "F.Column") -> "F.Column":
    """
    Replica ``FN_CLEAN_DAT_V`` (CDT_ESTR.sql:147821): stringa 'YYYYMMDD' -> intero.
    Default su null/errore = 19000101 (sentinella legacy, DIVERSA da clean_dat_d/j che usano 0).
    """
    return F.coalesce(F.regexp_replace(col.cast("string"), "[^0-9]", "").cast("int"),
                      F.lit(19000101))


def clean_dat_d_to_date(col: "F.Column") -> "F.Column":
    """Variante che ritorna DateType (NULL se non valido) invece dell'intero YYYYMMDD."""
    return col.cast("date")


def art_radice(col: "F.Column") -> "F.Column":
    """
    Replica ``FN_GET_RADICE`` (CDT_ESTR.sql:150451) per sorgenti LOGISTIX/SWAP/STAT:
    codice radice = codice articolo SENZA le ultime 3 cifre (variante).
    Es. '2534106004' -> '2534106'. Caso limite (len<=3): ritorna il codice intero.

    NB: per sorgente 'GOLD4' il legacy NON tronca ma fa lookup su ARTDGENE (CNDARTRADICE):
    quel caso va gestito con join esplicito, non da questa funzione.
    """
    s = col.cast("string")
    return F.when(F.length(s) > 3, F.regexp_replace(s, r".{3}$", "")).otherwise(s)


def art_variante(col: "F.Column") -> "F.Column":
    """
    Replica ``FN_GET_VARIANTE_LOGISTICA`` (CDT_ESTR.sql:150892) per LOGISTIX/SWAP/STAT:
    variante = ultime 3 cifre del codice articolo. Es. '2534106004' -> '004'.
    Caso limite (len<=3): NULL.
    """
    s = col.cast("string")
    return F.when(F.length(s) > 3, F.regexp_extract(s, r"(.{3})$", 1)) \
            .otherwise(F.lit(None).cast("string"))


# ---------------------------------------------------------------------------
# Normalizzazione codice sito/magazzino (canonico: numerico 2 cifre zero-padded)
# ---------------------------------------------------------------------------

def get_sito_alias_map(spark: SparkSession, bronze_schema: str) -> Dict[str, str]:
    """
    Costruisce la mappa alias-sito alfa -> codice numerico, leggendo TABGEN.

    Logistix usa codici sito alfa (LGAX, LGCX); il codice canonico e' numerico
    (LGAX->20, LGCX->57). La corrispondenza e' in TABGEN (tab 7, chiave 1, campo1).

    Args:
        spark: SparkSession attiva.
        bronze_schema: schema collassato che contiene 'tabgen'
                       (es. "bronze_dev.logistica" o "bronze_dev_logistica").

    Returns:
        dict {SITO_ALFA: SITO_NUM}, es. {"LGAX": "20", "LGCX": "57"}.
        Vuoto se TABGEN non e' disponibile (la normalizzazione fara' solo lpad).
    """
    try:
        tg = (spark.table(f"{bronze_schema}.tabgen")
              .filter("TGEN_NRO_TAB = 7 AND TGEN_CHIAVE1_TAB = 1 AND TGEN_CAMPO1_TAB IS NOT NULL"))
        rows = (tg.select(
                    F.col("MAG_SITO_COD").cast("string").alias("alfa"),
                    F.col("TGEN_CAMPO1_TAB").cast("int").cast("string").alias("num"))
                .distinct().collect())
        # chiavi alias normalizzate UPPERCASE: il lookup in normalize_sito e' case-insensitive
        # (es. _sito_estrazione = "lgax" minuscolo dal path landing).
        return {r["alfa"].strip().upper(): r["num"] for r in rows if r["alfa"] and r["num"]}
    except Exception:
        return {}


def normalize_sito(col: "F.Column", alias_map: Optional[Dict[str, str]] = None) -> "F.Column":
    """
    Porta un codice sito/magazzino al formato CANONICO: numerico a 2 cifre zero-padded.

    - Se ``alias_map`` e' fornito, rimappa prima gli alias alfa (es. LGAX->20).
    - Poi estrae le cifre e applica zero-padding a 2 (es. 5->"05", 9->"09", 20->"20").
    - Valori non numerici non mappati restano invariati (saranno orfani segnalati a valle).

    Args:
        col: Colonna sito sorgente (alfa LGAX/LGCX o numerica 5/9/20...).
        alias_map: dict opzionale {alfa: numerico} da ``get_sito_alias_map``
                   (necessario per i Silver Logistix; per STAT basta None).

    Returns:
        Colonna ``StringType`` con il codice sito canonico (2 cifre).
    """
    s = col.cast("string")
    if alias_map:
        # lookup case-insensitive: l'input puo' essere minuscolo (_sito_estrazione="lgax")
        # mentre le chiavi alias sono UPPERCASE; uppercase dell'input prima del map.
        key = F.upper(F.trim(s))
        mapping = F.create_map([F.lit(x) for kv in alias_map.items() for x in kv])
        s = F.coalesce(mapping[key], s)  # alias->num se presente, altrimenti invariato
    digits = F.regexp_replace(s, "[^0-9]", "")
    return F.when(digits != "", F.lpad(digits, 2, "0")).otherwise(s)


# ---------------------------------------------------------------------------
# Dimensioni Late-Arriving: surrogate key fallback
# ---------------------------------------------------------------------------

def surrogate_key_fallback(
    df: DataFrame,
    fk_col: str,
    dim_df: DataFrame,
    dim_pk: str,
    default_val: Any = -1,
    null_val: Any = None,
) -> DataFrame:
    """
    Risolve le FK verso una dimensione con fallback per Late-Arriving Dimensions.

    Esegue un LEFT JOIN tra il DataFrame dei fatti e la dimensione e distingue
    DUE casi (semantica diversa):

    - FK **valorizzata ma non agganciata** -> ``default_val`` (di default -1,
      riga "SCONOSCIUTO" della dimensione): è un VERO orfano, da investigare.
    - FK **NULL/vuota in origine** -> ``null_val`` se fornito (es. "ND", membro
      "Non rilevato"): è un dato di business legittimo (valore non registrato),
      NON un orfano. Se ``null_val`` è None, i NULL ricadono su ``default_val``
      (comportamento storico).

    Questo permette a ``check_orphan_rate`` (che conta == default_val) di
    misurare solo i veri non-agganciati, senza che i NULL legittimi gonfino la
    metrica. Le dimensioni interessate devono esporre i membri corrispondenti
    (es. dim_operatore ha il membro 'ND').

    Args:
        df: DataFrame dei fatti con la colonna FK.
        fk_col: Nome della colonna FK nel DataFrame dei fatti.
        dim_df: DataFrame della dimensione.
        dim_pk: Nome della colonna PK nella dimensione.
        default_val: Valore per FK valorizzate ma non agganciate. Default: -1.
        null_val: Valore per FK NULL/vuote in origine. Default: None (= default_val).

    Returns:
        DataFrame con la colonna ``fk_col`` risolta.

    Example::

        fact_df = surrogate_key_fallback(
            df=fact_df, fk_col="OPERATORE_COD",
            dim_df=lu_operatore, dim_pk="OPERATORE_COD",
            default_val="-1", null_val="ND",
        )
    """
    # Rinomina la PK della dimensione per evitare ambiguità nel join
    _alias = "_dim_pk_check"
    dim_keys = dim_df.select(F.col(dim_pk).alias(_alias)).distinct()

    # Maschera "FK nulla/vuota in origine" (prima del join), valutata solo se serve.
    src = F.col(fk_col).cast("string")
    is_src_null = src.isNull() | (F.trim(src) == "")

    if null_val is not None:
        resolved_fk = (
            F.when(is_src_null, F.lit(null_val))             # NULL legittimo -> membro dedicato
             .otherwise(F.coalesce(F.col(_alias), F.lit(default_val)))  # match | orfano -1
        )
    else:
        resolved_fk = F.coalesce(F.col(_alias), F.lit(default_val))

    resolved = (
        df.join(dim_keys, df[fk_col] == F.col(_alias), "left")
        .withColumn(fk_col, resolved_fk)
        .drop(_alias)
    )
    return resolved


# ---------------------------------------------------------------------------
# FASE 3 F_CARICO: aggancio anagrafiche + colonne _NAT (condiviso)
# ---------------------------------------------------------------------------

def attach_carico_dimensions(
    spark: SparkSession,
    fact: DataFrame,
    gold_catalog: str,
    retail_ms: str,
    logger: Any,
    notebook_name: str,
) -> DataFrame:
    """
    FASE 3 di F_CARICO: scrive le chiavi naturali ``_NAT`` e aggancia le anagrafiche
    condivise/logistiche con :func:`surrogate_key_fallback` (fallback -1).

    Logica UNICA condivisa tra ``gold_f_carico`` (flusso normale) e
    ``gold_late_arriving_handler`` (riprocesso partizioni passate): garantisce che
    lo schema del fact prodotto sia IDENTICO nei due percorsi, in particolare le sei
    colonne ``*_COD_NAT`` richieste da ``gold_lad_resolver`` (prerequisito L-01).

    Assume che ``fact`` esponga le chiavi naturali pre-risoluzione:
    ``ART_RADICE_COD``, ``FORNITORE_COD``, ``SITO_COD``, ``OPERATORE_COD``,
    ``RICEVITORE_COD``, ``CORRIERE_COD`` (come da ``silver.logistica_curated.carico``).

    Args:
        spark: SparkSession attiva.
        fact: DataFrame del fact carico (grain riga dettaglio).
        gold_catalog: catalogo Gold (es. "gold_dev"); ospita le LU logistiche.
        retail_ms: schema lookup master condiviso (es. "cdtdw.condiviso").
        logger: logger del notebook chiamante (per i warning di aggancio saltato).
        notebook_name: nome del notebook chiamante (passato a check_orphan_rate).

    Returns:
        DataFrame con colonne ``_NAT`` e FK risolte (o -1/ND in fallback).
    """
    from dq_helper import check_orphan_rate  # noqa: PLC0415

    # Chiavi naturali pre-risoluzione surrogate — necessarie per LAD resolver (OP-32).
    fact = (fact
            .withColumn("ART_RADICE_COD_NAT", F.col("ART_RADICE_COD"))
            .withColumn("FORNITORE_COD_NAT",  F.col("FORNITORE_COD"))
            .withColumn("SITO_COD_NAT",       F.col("SITO_COD"))
            .withColumn("OPERATORE_COD_NAT",  F.col("OPERATORE_COD"))
            .withColumn("RICEVITORE_COD_NAT", F.col("RICEVITORE_COD"))
            .withColumn("CORRIERE_COD_NAT",   F.col("CORRIERE_COD")))

    # ── Anagrafiche condivise (codice naturale, fallback -1) ────────────────────
    try:
        lu_art = spark.read.table(f"{retail_ms}.LU_ART_RADICE")
        fact = surrogate_key_fallback(fact, "ART_RADICE_COD", lu_art, "ART_RADICE_COD", default_val="-1")
        check_orphan_rate(fact, "ART_RADICE_COD", notebook_name)
    except Exception as e:  # noqa: BLE001
        logger.warning(f"LU_ART_RADICE non disponibile in {retail_ms}: aggancio articolo saltato ({str(e)[:80]})")

    try:
        lu_for = spark.read.table(f"{retail_ms}.LU_FORNITORE")
        fact = surrogate_key_fallback(fact, "FORNITORE_COD", lu_for, "FORN_COD", default_val="-1")
        check_orphan_rate(fact, "FORNITORE_COD", notebook_name)
    except Exception as e:  # noqa: BLE001
        logger.warning(f"LU_FORNITORE non disponibile in {retail_ms}: aggancio fornitore saltato ({str(e)[:80]})")

    # Anagrafiche logistiche proprietarie (gold_prod.logistica), fallback -1.
    logistic_lu = [
        ("SITO_COD",       "LU_SITO",      "SITO_COD"),
        ("OPERATORE_COD",  "LU_OPERATORE", "OPERATORE_COD"),   # operatore validante
        ("RICEVITORE_COD", "LU_OPERATORE", "OPERATORE_COD"),   # operatore ricevente
        ("CORRIERE_COD",   "LU_CORRIERE",  "CORRIERE_COD"),    # vettore carico inbound
    ]
    for fk, lu_tab, lu_pk in logistic_lu:
        try:
            lu_df = spark.read.table(f"{gold_catalog}.logistica.{lu_tab}")
            # operatori: NULL in origine -> membro 'ND' (Non rilevato), non orfano.
            _nv = "ND" if fk in ("OPERATORE_COD", "RICEVITORE_COD") else None
            fact = surrogate_key_fallback(fact, fk, lu_df, lu_pk, default_val="-1", null_val=_nv)
            check_orphan_rate(fact, fk, notebook_name)
        except Exception as e:  # noqa: BLE001
            logger.warning(f"{lu_tab} non disponibile: aggancio {fk} saltato ({str(e)[:80]})")

    return fact


def attach_carico_peso_volume(
    spark: SparkSession,
    fact: DataFrame,
    retail_ms: str,
    logger: Any,
    notebook_name: str,
) -> DataFrame:
    """
    FASE 3b di F_CARICO: calcola ``PES_CARICO`` / ``VOL_CARICO`` dall'anagrafica
    ``LU_ART_UNITA_LOGISTICA`` (unità logistica, ``ART_UNITA_LOGISTICA_COD=1``),
    replicando la formula ODI di ``CDT_SA.SP_LOAD_F_CARICO``::

        PES_CARICO = CASE WHEN ART_MODL_PES_COD > 1 THEN QTA_UF_CARICO
                          ELSE NVL(ART_UNITA_LOGISTICA_PESO_LORDO,0) * QTA_UF_CARICO END
        VOL_CARICO = QTA_CARICO * ALT_PZ * LAR_PZ * PRO_PZ / 1000

    Espone anche ``ART_MODL_PES_COD`` e la FK dimensionale ``ART_VARIANTE_LOGISTICA_ID``.

    Join su ``ART_RADICE_COD_NAT`` (chiave naturale pre-risoluzione) + variante
    logistica: il nostro ``ART_VAR_LOGIS_COD`` è zero-padded a 3 cifre (OP-12,
    es. '002') mentre l'anagrafica usa il codice intero ('2') → confronto castato a int.

    Fallback (OP-CAR-6): se l'anagrafica non è disponibile, ``PES_CARICO``/``VOL_CARICO``/
    ``ART_VARIANTE_LOGISTICA_ID`` restano NULL e ``ART_MODL_PES_COD`` = 1 — ma il fallback
    **non è silenzioso**: emette un log ERROR con ``event=dq_result``,
    ``check_name=anagrafica_peso_volume_disponibile``, ``passed=False``. Il blocco effettivo
    avviene al gate: ``PES_CARICO``/``VOL_CARICO`` sono in ``not_null`` (BLOCKING) nei criteri
    di accettazione di ``gold_f_carico``.

    Nota diagnostica: con l'anagrafica agganciata queste colonne non sono **mai** NULL — le
    righe senza match danno 0 per l'``NVL(...,0)`` della formula. Quindi un NULL qui significa
    sempre "schema anagrafiche sbagliato", non "dato assente".
    """
    try:
        ul = spark.read.table(f"{retail_ms}.LU_ART_UNITA_LOGISTICA")
    except Exception as e:  # noqa: BLE001
        # OP-CAR-6: il fallback NON deve essere silenzioso. Prima emetteva un solo warning e
        # la pipeline chiudeva in successo con PES_CARICO/VOL_CARICO NULL su TUTTE le righe
        # (59.621/59.621 in ACT_9015), superando 13/13 check DQ. Un errore di configurazione
        # travestito da dato mancante e' peggio di un fallimento.
        # Ora: log ERROR + dq_result machine-readable (passed=False). Il blocco effettivo
        # avviene al gate, dove PES_CARICO/VOL_CARICO sono in not_null (BLOCKING).
        msg = (f"LU_ART_UNITA_LOGISTICA NON disponibile in '{retail_ms}': PES_CARICO/VOL_CARICO "
               f"saranno NULL su TUTTE le righe. Verificare il widget retail_master_schema "
               f"(atteso bronze_<env>.condiviso). Dettaglio: {str(e)[:120]}")
        # `logger` qui e' un logging.Logger stdlib (get_logger): NON accetta kwargs custom.
        # Per l'evento strutturato si istanzia il nostro Logger, come fa check_orphan_rate.
        logger.error(msg)
        try:
            from logging_helper import Logger as _StructLogger  # lib sul sys.path (notebook)
        except ImportError:  # pragma: no cover - import come package
            from .logging_helper import Logger as _StructLogger
        _StructLogger(notebook_name=notebook_name, area="logistica", layer="gold")._emit(
            _StructLogger.LEVEL_ERROR, msg,
            event="dq_result", check_name="anagrafica_peso_volume_disponibile", passed=False,
            retail_master_schema=retail_ms,
        )
        return (fact
                .withColumn("ART_MODL_PES_COD", F.lit(1).cast("int"))
                .withColumn("ART_VARIANTE_LOGISTICA_ID", F.lit(None).cast("string"))
                .withColumn("PES_CARICO", F.lit(None).cast("double"))
                .withColumn("VOL_CARICO", F.lit(None).cast("double")))

    ul_norm = (ul.select(
        F.col("ART_RADICE_COD").cast("string").alias("_ul_radice"),
        F.col("ART_VARIANTE_LOGIS_COD").cast("int").alias("_ul_var"),
        F.col("ART_VARIANTE_LOGISTICA_ID").cast("string").alias("_ul_var_id"),
        F.col("ART_MODL_PES_COD").cast("int").alias("_ul_modl"),
        F.col("ART_UNITA_LOGISTICA_PESO_LORDO").cast("double").alias("_ul_peso"),
        F.col("ART_UNITA_LOGISTICA_ALT_PZ").cast("double").alias("_ul_alt"),
        F.col("ART_UNITA_LOGISTICA_LAR_PZ").cast("double").alias("_ul_lar"),
        F.col("ART_UNITA_LOGISTICA_PRO_PZ").cast("double").alias("_ul_pro"),
    ).dropDuplicates(["_ul_radice", "_ul_var"]))

    joined = fact.join(
        ul_norm,
        (F.col("ART_RADICE_COD_NAT") == F.col("_ul_radice"))
        & (F.col("ART_VAR_LOGIS_COD").cast("int") == F.col("_ul_var")),
        "left",
    )

    modl = F.coalesce(F.col("_ul_modl"), F.lit(1))
    out = (joined
        .withColumn("ART_MODL_PES_COD", modl.cast("int"))
        .withColumn("ART_VARIANTE_LOGISTICA_ID", F.col("_ul_var_id"))
        .withColumn("PES_CARICO",
                    F.when(modl > 1, F.col("QTA_UF_CARICO"))
                     .otherwise(F.coalesce(F.col("_ul_peso"), F.lit(0.0)) * F.col("QTA_UF_CARICO"))
                     .cast("double"))
        .withColumn("VOL_CARICO",
                    (F.col("QTA_CARICO")
                     * F.coalesce(F.col("_ul_alt"), F.lit(0.0))
                     * F.coalesce(F.col("_ul_lar"), F.lit(0.0))
                     * F.coalesce(F.col("_ul_pro"), F.lit(0.0)) / F.lit(1000.0)).cast("double"))
        .drop("_ul_radice", "_ul_var", "_ul_var_id", "_ul_modl",
              "_ul_peso", "_ul_alt", "_ul_lar", "_ul_pro"))
    return out


# ---------------------------------------------------------------------------
# Cast decimali in batch
# ---------------------------------------------------------------------------

def cast_decimal(
    df: DataFrame,
    cols: List[str],
    precision: int = 18,
    scale: int = 4,
) -> DataFrame:
    """
    Esegue il cast di un gruppo di colonne al tipo DECIMAL(precision, scale).

    Utile nello strato Silver per normalizzare misure provenienti da Oracle
    (NUMBER, FLOAT) verso il tipo corretto per Delta Lake.

    Args:
        df: DataFrame di input.
        cols: Lista di nomi colonne da castare.
        precision: Precisione totale (cifre significative). Default: 18.
        scale: Numero di cifre decimali. Default: 4.

    Returns:
        DataFrame con le colonne indicate castate a DECIMAL(precision, scale).

    Raises:
        ValueError: Se una delle colonne non esiste nel DataFrame.

    Example::

        df = cast_decimal(df, ["PESO_NETTO", "PESO_LORDO", "IMPORTO"], precision=18, scale=4)
    """
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"Colonne non trovate nel DataFrame: {missing}")

    decimal_type = DecimalType(precision, scale)
    for col in cols:
        df = df.withColumn(col, F.col(col).cast(decimal_type))
    return df


# ---------------------------------------------------------------------------
# Metadati di ingestion
# ---------------------------------------------------------------------------

def add_ingestion_metadata(df: DataFrame, source_system: str) -> DataFrame:
    """
    Aggiunge colonne di metadati standard a un DataFrame Bronze.

    Colonne aggiunte:
    - ``_ingestion_timestamp``: Timestamp UTC del momento dell'ingestion.
    - ``_source_system``: Stringa identificativa del sistema sorgente.

    Queste colonne sono usate per deduplication nello strato Silver
    e per auditing/lineage.

    Args:
        df: DataFrame di input.
        source_system: Identificativo del sistema sorgente
                       (es. "oracle-logistica", "sap-wm").

    Returns:
        DataFrame con le due colonne aggiuntive.

    Example::

        df = add_ingestion_metadata(df, source_system="oracle-logistica")
    """
    return df.withColumn(
        "_ingestion_timestamp", F.current_timestamp()
    ).withColumn(
        "_source_system", F.lit(source_system)
    )


# Alias semantico usato dai notebook Bronze (trasporti/*): allinea le colonne
# di audit Bronze ai metadati di ingestion.
add_audit_cols = add_ingestion_metadata


# ---------------------------------------------------------------------------
# Bronze MERGE upsert con PRUNING degli update (row-hash)
# ---------------------------------------------------------------------------

def add_row_hash(df: DataFrame, col_name: str = "_row_hash", exclude_prefix: str = "_") -> DataFrame:
    """
    Aggiunge una firma SHA-256 del contenuto riga, calcolata SOLO sulle colonne business
    (esclude i metadati con prefisso ``_``). Coalesce con sentinella per non confondere
    NULL e stringa vuota.

    Usata dal MERGE Bronze per il PRUNING: righe con hash invariato non vengono ri-scritte
    (ne' ri-datate ``_bronze_load_date``), cosi' il delta propagato a clean/prep e' solo
    quello realmente nuovo o modificato.
    """
    biz = [c for c in df.columns if not c.startswith(exclude_prefix)]
    sig = F.sha2(F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("~")) for c in biz]), 256)
    return df.withColumn(col_name, sig)


def bronze_merge_upsert(spark, df: DataFrame, full_target: str, merge_keys: List[str],
                        insert_ts_col: str = "_bronze_insert_ts") -> str:
    """
    Scrittura Bronze idempotente con pruning. CTAS alla prima esecuzione; altrimenti MERGE:
      - chiavi null-safe (``<=>``) -> NULL matcha NULL;
      - ``whenMatchedUpdate`` SOLO se ``_row_hash`` differisce (no re-scrittura di righe identiche);
      - ``whenNotMatchedInsertAll``.
    Aggiunge ``_row_hash`` se assente. Restituisce "CTAS" o "MERGE".
    """
    from delta.tables import DeltaTable
    if "_row_hash" not in df.columns:
        df = add_row_hash(df)
    if not spark.catalog.tableExists(full_target):
        df.write.format("delta").option("mergeSchema", "true").saveAsTable(full_target)
        return "CTAS"
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    cond = " AND ".join(f"tgt.{k} <=> src.{k}" for k in merge_keys)
    update_set = {c: f"src.{c}" for c in df.columns if c not in merge_keys and c != insert_ts_col}
    (DeltaTable.forName(spark, full_target).alias("tgt")
     .merge(df.alias("src"), cond)
     .whenMatchedUpdate(condition="tgt._row_hash <> src._row_hash", set=update_set)
     .whenNotMatchedInsertAll()
     .execute())
    return "MERGE"


# ---------------------------------------------------------------------------
# Lettura landing Bronze (CSV / Parquet con auto-detect) — G-01
# ---------------------------------------------------------------------------

def detect_format(path: str, file_format: str, dbutils_obj=None) -> str:
    """Auto-rileva il formato dei file di landing (``"parquet"`` o ``"csv"``).

    Se ``file_format != "auto"``, lo restituisce direttamente senza ispezione.
    Tenta prima con ``dbutils.fs.ls`` (Databricks cloud o DBUtilsStub locale),
    poi con pathlib come fallback. Default finale: ``"csv"``.

    Args:
        path: Directory landing (stringa, accetta file:/// o abfss://).
        file_format: Valore del widget Databricks ("csv" | "parquet" | "auto").
        dbutils_obj: Riferimento a dbutils (Databricks o DBUtilsStub). Opzionale.

    Returns:
        ``"parquet"`` oppure ``"csv"``.
    """
    if file_format != "auto":
        return file_format
    if dbutils_obj is not None:
        try:
            for f in dbutils_obj.fs.ls(path):
                if f.name.endswith(".parquet"):
                    return "parquet"
                if f.name.endswith(".csv"):
                    return "csv"
        except Exception:
            pass
    # Fallback pathlib (ambienti senza dbutils o path locali puri)
    try:
        from pathlib import Path as _Path
        s = path.strip()
        if s.startswith("file:///"):
            s = s[len("file:///"):]
        elif s.startswith("file://"):
            s = s[len("file://"):]
        for f in sorted(_Path(s).iterdir()):
            if f.suffix == ".parquet":
                return "parquet"
            if f.suffix == ".csv":
                return "csv"
    except Exception:
        pass
    return "csv"


def read_landing(spark, path: str, effective_fmt: str):
    """Legge i file di landing (CSV o Parquet) in un DataFrame Spark grezzo.

    Per CSV: tutte StringType, sep=";", encoding UTF-8, header=true.
    Per Parquet: schema nativo del file.

    Args:
        spark: SparkSession attiva.
        path: Directory di landing (parquet) o base dir (csv: viene aggiunto ``*.csv``).
        effective_fmt: ``"parquet"`` o ``"csv"`` (output di :func:`detect_format`).

    Returns:
        DataFrame grezzo.
    """
    if effective_fmt == "parquet":
        return spark.read.format("parquet").load(path)
    csv_path = path if path.endswith("*.csv") else f"{path}*.csv"
    return (spark.read
            .option("header", "true")
            .option("inferSchema", "false")
            .option("sep", ";")
            .option("encoding", "UTF-8")
            .csv(csv_path))


# ---------------------------------------------------------------------------
# Watermark / controllo incrementale ETL (OP-35)
# Tabella di controllo per-ambiente: control_<env>.etl.watermark
# Chiave logica: (stage, sistema, tabella, sito). Vedi DOCS/Design - Watermark ETL (OP-35).md
# ---------------------------------------------------------------------------

# Sito convenzionale per sorgenti non multi-sito (stat/track/cdt_estr_raw/cdtdw).
WATERMARK_ALL_SITES = "_ALL_"

_WATERMARK_COLS = [
    "stage", "sistema", "tabella", "sito",
    "last_processed_date", "last_run_ts", "rows_processed",
    "esito", "message", "_updated_at",
]
_WATERMARK_KEYS = ["stage", "sistema", "tabella", "sito"]


def get_control_table(env: str) -> str:
    """FQN della tabella watermark: ``control_<env>.etl.watermark``.

    In locale il runner collassa l'FQN a 3 livelli in ``control_<env>_etl.watermark``.
    """
    return f"{get_catalog('control', env)}.etl.watermark"


def ensure_watermark_table(spark: SparkSession, env: str) -> str:
    """Crea (se assenti) lo schema ``etl`` e la tabella ``watermark``. Idempotente.

    Returns:
        FQN della tabella watermark.
    """
    control = get_catalog("control", env)
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {control}.etl")
    fqn = f"{control}.etl.watermark"
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {fqn} (
            stage               STRING    COMMENT 'confine: landing_to_bronze|bronze_to_clean|clean_to_prep',
            sistema             STRING    COMMENT 'logistix|stat|track|cdt_estr_raw|cdtdw',
            tabella             STRING    COMMENT 'nome tabella landing/bronze',
            sito                STRING    COMMENT 'sito logistix oppure _ALL_',
            last_processed_date DATE      COMMENT 'ultima data processata con SUCCESSO',
            last_run_ts         TIMESTAMP COMMENT 'timestamp ultimo aggiornamento',
            rows_processed      BIGINT    COMMENT 'righe processate ultimo step (diagnostico)',
            esito               STRING    COMMENT 'OK|FAIL',
            message             STRING    COMMENT 'nota/errore sintetico',
            _updated_at         TIMESTAMP COMMENT 'audit tecnico scrittura'
        ) USING delta
    """)
    return fqn


def read_watermark(spark: SparkSession, env: str, stage: str, sistema: str,
                   tabella: str, sito: str = WATERMARK_ALL_SITES) -> Optional[date]:
    """Ultima data processata con successo per la chiave (stage, sistema, tabella, sito).

    ``last_processed_date`` è per costruzione sempre l'ultima data andata a buon fine
    (su FAIL non viene avanzata): è il punto da cui riprendere il catch-up.

    Returns:
        ``datetime.date`` se la riga esiste, altrimenti ``None`` (mai processato).
    """
    fqn = get_control_table(env)
    if not spark.catalog.tableExists(fqn):
        return None
    cond = " AND ".join(f"{k} = '{v}'" for k, v in
                        zip(_WATERMARK_KEYS, [stage, sistema, tabella, sito]))
    row = (spark.table(fqn).where(cond)
           .select("last_processed_date").limit(1).collect())
    return row[0]["last_processed_date"] if row else None


def update_watermark(spark: SparkSession, env: str, stage: str, sistema: str,
                     tabella: str, sito: str = WATERMARK_ALL_SITES,
                     last_processed_date=None, rows_processed: int = 0,
                     esito: str = "OK", message: Optional[str] = None) -> None:
    """MERGE upsert della riga (stage, sistema, tabella, sito) nella tabella watermark.

    Regola di transazionalità (OP-35): chiamare SOLO dopo aver scritto con successo il
    target. Su ``esito='FAIL'`` la ``last_processed_date`` NON viene avanzata (si preserva
    l'ultima buona, così il retry riparte dalla data fallita) e si registrano esito+message.

    Args:
        last_processed_date: data (date o 'YYYY-MM-DD') processata con successo; usata solo se esito='OK'.
    """
    from delta.tables import DeltaTable  # noqa: PLC0415

    fqn = ensure_watermark_table(spark, env)

    # Su FAIL preserva la data esistente (non avanzare); su OK usa quella passata.
    if esito != "OK":
        last_processed_date = read_watermark(spark, env, stage, sistema, tabella, sito)

    lpd = None
    if last_processed_date is not None:
        lpd = (last_processed_date if isinstance(last_processed_date, date)
               else datetime.strptime(str(last_processed_date), "%Y-%m-%d").date())

    src = spark.createDataFrame(
        [(stage, sistema, tabella, sito, lpd, int(rows_processed), esito, message)],
        schema="stage string, sistema string, tabella string, sito string, "
               "last_processed_date date, rows_processed bigint, esito string, message string",
    ).withColumn("last_run_ts", F.current_timestamp()) \
     .withColumn("_updated_at", F.current_timestamp())

    cond = " AND ".join(f"tgt.{k} <=> src.{k}" for k in _WATERMARK_KEYS)
    (DeltaTable.forName(spark, fqn).alias("tgt")
     .merge(src.alias("src"), cond)
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute())


def pending_landing_dates(spark: SparkSession, env: str, sistema: str, tabella: str,
                          sito: str, available_dates: List[Any]) -> List[date]:
    """Date landing disponibili NON ancora processate per la chiave (stage landing_to_bronze).

    Args:
        available_dates: elenco partizioni landing presenti (date o 'YYYY-MM-DD').

    Returns:
        Lista ordinata di ``date`` > last_processed_date (tutte se mai processato).
    """
    def _to_date(d):
        return d if isinstance(d, date) else datetime.strptime(str(d), "%Y-%m-%d").date()

    last = read_watermark(spark, env, "landing_to_bronze", sistema, tabella, sito)
    dates = sorted(_to_date(d) for d in available_dates)
    return [d for d in dates if last is None or d > last]
