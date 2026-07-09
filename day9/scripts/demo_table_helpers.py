"""Setup cell + write_demo_table / read_demo_table helpers for 50-problems notebook.

Handles PERMISSION_DENIED on workspace catalog (dbw_shared_qgr7mj.demo_problems):
prefers main.demo_problems, tests write access, falls back to ADLS Delta paths.
"""

DEMO_TABLE_HELPERS = '''# =============================================================================
# SETUP — writable Unity Catalog schema OR ADLS fallback
# =============================================================================
# WHAT: Find a schema you are allowed to write to (main.demo_problems preferred).
# WHY:  Shared workspace catalog dbw_shared_* often denies USE SCHEMA to learners.
# HOW:  Probe write permission; if denied, save Delta under DEMO_BASE on ADLS.
# =============================================================================
from pyspark.sql import functions as F

PREFERRED_CATALOG = "finledger"
FALLBACK_CATALOG = "main"
SCHEMA = "demo_problems"
DEMO_BASE = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/demo_50_problems"
CATALOG_ROOT = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/_unity_catalog/{PREFERRED_CATALOG}"

dbutils.fs.mkdirs(DEMO_BASE)
dbutils.fs.mkdirs(CATALOG_ROOT)

UC_WRITE_ENABLED = False
WRITTEN_TABLES: list[str] = []


def _try_catalog_write(catalog: str) -> bool:
    """Return True if current user can CREATE TABLE in catalog.demo_problems."""
    full_schema = f"{catalog}.{SCHEMA}"
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {full_schema}")
    except Exception as exc:
        print(f"  CREATE SCHEMA {full_schema} failed:", str(exc)[:160])
        return False
    try:
        spark.sql(f"USE CATALOG {catalog}")
        spark.sql(f"USE SCHEMA {SCHEMA}")
    except Exception as exc:
        print(f"  USE SCHEMA {full_schema} failed:", str(exc)[:160])
        return False
    test_table = f"{full_schema}._write_probe"
    try:
        spark.createDataFrame([("ok",)], ["ping"]).write.format("delta").mode("overwrite").saveAsTable(
            test_table
        )
        spark.sql(f"DROP TABLE IF EXISTS {test_table}")
        return True
    except Exception as exc:
        print(f"  Write probe on {full_schema} failed:", str(exc)[:160])
        return False


def _init_storage():
    global CATALOG, UC_TABLE, UC_WRITE_ENABLED
    names = [r.catalog for r in spark.sql("SHOW CATALOGS").collect()]
    print("Catalogs visible:", names)

    # Try finledger, then main — never blindly use dbw_* workspace catalog
    candidates = []
    for c in (PREFERRED_CATALOG, FALLBACK_CATALOG):
        if c in names:
            candidates.append(c)
    if PREFERRED_CATALOG not in names:
        try:
            spark.sql(
                f"CREATE CATALOG IF NOT EXISTS {PREFERRED_CATALOG} MANAGED LOCATION '{CATALOG_ROOT}'"
            )
            candidates.insert(0, PREFERRED_CATALOG)
        except Exception as exc:
            print("CREATE CATALOG finledger skipped:", str(exc)[:160])

    for catalog in candidates:
        print(f"Testing write access: {catalog}.{SCHEMA} ...")
        if _try_catalog_write(catalog):
            CATALOG = catalog
            UC_TABLE = f"{catalog}.{SCHEMA}"
            UC_WRITE_ENABLED = True
            print("UC write mode: ENABLED →", UC_TABLE)
            return

    CATALOG = "adls"
    UC_TABLE = SCHEMA
    UC_WRITE_ENABLED = False
    print("UC write mode: DISABLED (permission denied on all catalogs)")
    print("Tables will save as Delta under:", DEMO_BASE)


_init_storage()


def write_demo_table(df, table_name: str, mode: str = "overwrite") -> str:
    """Save demo output — Unity Catalog if permitted, else ADLS Delta path."""
    path = f"{DEMO_BASE}/{table_name}"
    writer = df.write.format("delta").mode(mode)
    if mode.lower() == "overwrite":
        # Prevent rerun failures when schema evolves between class attempts.
        writer = writer.option("overwriteSchema", "true")
    if UC_WRITE_ENABLED:
        full_name = f"{UC_TABLE}.{table_name}"
        try:
            writer.saveAsTable(full_name)
            print("UC table :", full_name)
            WRITTEN_TABLES.append(full_name)
            return full_name
        except Exception as exc:
            err = str(exc)
            if "PERMISSION_DENIED" not in err and "UNAUTHORIZED" not in err:
                raise
            print("UC write denied — falling back to ADLS path")
    writer.save(path)
    print("Delta path:", path)
    WRITTEN_TABLES.append(path)
    return path


def read_demo_table(table_name: str):
    """Read demo table from UC or ADLS fallback path."""
    if UC_WRITE_ENABLED:
        try:
            return spark.table(f"{UC_TABLE}.{table_name}")
        except Exception:
            pass
    return spark.read.format("delta").load(f"{DEMO_BASE}/{table_name}")


def demo_table_ref(table_name: str) -> str:
    """SQL-safe name for COMMENT / DESCRIBE (UC only)."""
    return f"{UC_TABLE}.{table_name}" if UC_WRITE_ENABLED else f"delta.`{DEMO_BASE}/{table_name}`"


print("=" * 60)
print("SETUP COMPLETE")
print("  Catalog       :", CATALOG)
print("  Schema / mode :", UC_TABLE, "| UC writes:", UC_WRITE_ENABLED)
print("  ADLS base     :", DEMO_BASE)
print("  Bronze rows   :", bronze.count())
print("  Auth mode     :", BRONZE_AUTH_MODE)
print("=" * 60)'''
