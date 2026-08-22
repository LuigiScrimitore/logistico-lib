"""
secret_helper.py — Gestione credenziali tramite Azure Key Vault / Databricks Secrets.

In ambiente Databricks usa dbutils.secrets.get().
In ambiente locale (test/sviluppo) usa variabili d'ambiente come fallback.
"""

import os
from typing import Optional


class SecretHelper:
    """
    Wrapper per la lettura di segreti da Databricks Secret Scope (Azure Key Vault backed).

    Il naming convention per i segreti nel vault è:
        {source}-jdbc-url
        {source}-jdbc-user
        {source}-jdbc-password

    Esempio per source="oracle-logistica":
        oracle-logistica-jdbc-url
        oracle-logistica-jdbc-user
        oracle-logistica-jdbc-password
    """

    def __init__(self, scope: str = "logistica-kv"):
        """
        Args:
            scope: Nome dello Databricks Secret Scope (mappato ad Azure Key Vault).
        """
        self.scope = scope
        self._dbutils = self._resolve_dbutils()

    # ------------------------------------------------------------------
    # Risoluzione dbutils
    # ------------------------------------------------------------------

    def _resolve_dbutils(self):
        """
        Prova a importare dbutils dall'ambiente Databricks.
        Se non disponibile (test locale) restituisce None — il fallback
        su os.environ verrà usato automaticamente in get_secret().
        """
        try:
            # Databricks IPython shell espone dbutils come variabile globale
            import IPython  # noqa: PLC0415
            shell = IPython.get_ipython()
            if shell is not None:
                dbutils = shell.user_ns.get("dbutils")
                if dbutils is not None:
                    return dbutils
        except ImportError:
            pass

        # Databricks Connect / jobs: dbutils è iniettato nel builtins
        try:
            import builtins  # noqa: PLC0415
            if hasattr(builtins, "dbutils"):
                return builtins.dbutils  # type: ignore[attr-defined]
        except Exception:
            pass

        return None

    # ------------------------------------------------------------------
    # Lettura segreti
    # ------------------------------------------------------------------

    def get_secret(self, key: str) -> str:
        """
        Legge un segreto dallo scope configurato.

        Prima tenta dbutils.secrets.get(); se non disponibile usa os.environ
        con il nome della variabile ricavato sostituendo '-' con '_' e
        portando in maiuscolo (es. oracle-jdbc-url → ORACLE_JDBC_URL).

        Args:
            key: Nome del segreto nel vault.

        Returns:
            Valore del segreto come stringa.

        Raises:
            KeyError: Se il segreto non è trovato né in Key Vault né in os.environ.
        """
        if self._dbutils is not None:
            try:
                return self._dbutils.secrets.get(scope=self.scope, key=key)
            except Exception as exc:  # pragma: no cover
                raise KeyError(
                    f"Segreto '{key}' non trovato nello scope '{self.scope}': {exc}"
                ) from exc

        # Fallback: variabile d'ambiente
        env_key = key.upper().replace("-", "_")
        value = os.environ.get(env_key)
        if value is None:
            raise KeyError(
                f"Segreto '{key}' non trovato. "
                f"Imposta la variabile d'ambiente '{env_key}' per i test locali."
            )
        return value

    # ------------------------------------------------------------------
    # Metodi di convenience per JDBC
    # ------------------------------------------------------------------

    def get_jdbc_url(self, source: str) -> str:
        """
        Restituisce la JDBC URL per la sorgente indicata.

        Args:
            source: Identificativo sorgente (es. "oracle-logistica").

        Returns:
            JDBC URL (es. jdbc:oracle:thin:@//host:1521/service).
        """
        return self.get_secret(f"{source}-jdbc-url")

    def get_jdbc_user(self, source: str) -> str:
        """
        Restituisce lo username JDBC per la sorgente indicata.

        Args:
            source: Identificativo sorgente.

        Returns:
            Username come stringa.
        """
        return self.get_secret(f"{source}-jdbc-user")

    def get_jdbc_password(self, source: str) -> str:
        """
        Restituisce la password JDBC per la sorgente indicata.

        Args:
            source: Identificativo sorgente.

        Returns:
            Password come stringa.
        """
        return self.get_secret(f"{source}-jdbc-password")

    def get_jdbc_options(self, source: str, extra_options: Optional[dict] = None) -> dict:
        """
        Costruisce un dizionario completo di opzioni per spark.read.format("jdbc").

        Combina url, user, password e le eventuali opzioni aggiuntive passate
        via extra_options (es. numPartitions, partitionColumn, lowerBound, upperBound).

        Args:
            source: Identificativo sorgente.
            extra_options: Dizionario opzionale con opzioni aggiuntive JDBC
                           (es. {"numPartitions": "8", "fetchsize": "10000"}).

        Returns:
            Dizionario pronto per spark.read.format("jdbc").options(**opts).load().

        Example::

            opts = secret_helper.get_jdbc_options(
                "oracle-logistica",
                {"numPartitions": "8", "partitionColumn": "ID_CARICO",
                 "lowerBound": "1", "upperBound": "1000000"}
            )
            df = spark.read.format("jdbc").options(**opts).load()
        """
        options = {
            "url": self.get_jdbc_url(source),
            "user": self.get_jdbc_user(source),
            "password": self.get_jdbc_password(source),
            "driver": "oracle.jdbc.OracleDriver",
            "fetchsize": "10000",
        }
        if extra_options:
            options.update(extra_options)
        return options


# ─────────────────────────────────────────────────────────────────────────────
# API module-level (firma usata dai notebook Bronze: trasporti/*)
# ─────────────────────────────────────────────────────────────────────────────

# Convenzione scope per env:
#   dev  -> logistica-kv-dev
#   prod -> logistica-kv-prod
# In locale (test) gli env-var hanno la stessa forma indipendentemente da env.
_SCOPE_BY_ENV = {
    "dev":  "logistica-kv-dev",
    "prod": "logistica-kv-prod",
}


def get_jdbc_options(source: str, env: str = "dev",
                     extra_options: Optional[dict] = None) -> dict:
    """
    Wrapper module-level di ``SecretHelper.get_jdbc_options``.

    Firma usata dai notebook Bronze (trasporti/*): ``get_jdbc_options(source, env)``.
    Risolve lo scope corretto per env e restituisce il dict di opzioni JDBC
    pronto per ``spark.read.format("jdbc").options(**opts).load()``.

    Args:
        source: Identificativo sorgente (es. "oracle-trasporti").
        env: Ambiente ("dev" | "prod"). Default "dev".
        extra_options: Opzioni JDBC aggiuntive (numPartitions, partitionColumn, ...).

    Returns:
        dict con chiavi: url, user, password, driver, fetchsize (+ extra_options).
    """
    env_key = (env or "dev").lower().strip()
    scope = _SCOPE_BY_ENV.get(env_key, f"logistica-kv-{env_key}")
    helper = SecretHelper(scope=scope)
    return helper.get_jdbc_options(source, extra_options=extra_options)
