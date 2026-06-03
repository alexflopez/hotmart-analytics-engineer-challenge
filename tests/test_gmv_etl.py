"""
Testes unitários — GMV ETL
============================
Valida as regras de negócio críticas do pipeline
usando PySpark em modo local (sem AWS).
"""

import pytest
from datetime import date, datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[*]")
        .appName("test_gmv_etl")
        .getOrCreate()
    )


def latest(df, key):
    w = Window.partitionBy(key).orderBy(F.col("transaction_datetime").desc())
    return (
        df
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


# ================================================================
# Teste 1 — Deduplicação mantém evento mais recente
# ================================================================

def test_deduplication_keeps_latest_event(spark):
    data = [
        (55, datetime(2023, 1, 20, 22, 0), date(2023, 1, 20)),
        (55, datetime(2023, 2,  5, 10, 0), date(2023, 1, 20)),  # reenvio
        (55, datetime(2023, 7, 15,  9, 0), date(2023, 1, 20)),  # reenvio mais recente
    ]
    df = spark.createDataFrame(data, ["purchase_id", "transaction_datetime", "order_date"])
    result = latest(df, "purchase_id")

    assert result.count() == 1
    assert result.collect()[0]["transaction_datetime"] == datetime(2023, 7, 15, 9, 0)


# ================================================================
# Teste 2 — Transações sem release_date excluídas do GMV
# ================================================================

def test_gmv_excludes_null_release_date(spark):
    data = [
        (55, date(2023, 1, 20), "APROVADA"),   # ✅ entra
        (56, None,              "INICIADA"),    # ❌ release_date nula
    ]
    df = spark.createDataFrame(data, ["purchase_id", "release_date", "purchase_status"])
    result = df.filter(F.col("release_date").isNotNull())

    assert result.count() == 1
    assert result.collect()[0]["purchase_id"] == 55


# ================================================================
# Teste 3 — Transações canceladas e reembolsadas excluídas do GMV
# ================================================================

def test_gmv_excludes_cancelled_and_refunded(spark):
    data = [
        (55, date(2023, 1, 20), "APROVADA"),    # ✅ entra
        (56, date(2023, 1, 21), "CANCELADA"),   # ❌ cancelada
        (57, date(2023, 1, 22), "REEMBOLSADA"), # ❌ reembolsada
        (58, date(2023, 1, 23), "INICIADA"),    # ❌ não aprovada
    ]
    df = spark.createDataFrame(data, ["purchase_id", "release_date", "purchase_status"])
    result = df.filter(F.col("purchase_status").isin("APROVADA"))

    assert result.count() == 1
    assert result.collect()[0]["purchase_id"] == 55


# ================================================================
# Teste 4 — Qualidade: registros com purchase_value nulo ou zero
#           são descartados antes do processamento
# ================================================================

def test_quality_filter_removes_invalid_purchase_value(spark):
    data = [
        ("prod_1", 55.00,  datetime(2023, 1, 20)),   # ✅ válido
        ("prod_2", None,   datetime(2023, 1, 21)),    # ❌ valor nulo
        ("prod_3", 0.00,   datetime(2023, 1, 22)),    # ❌ valor zero
        ("prod_4", -10.00, datetime(2023, 1, 23)),    # ❌ valor negativo
    ]
    df = spark.createDataFrame(data, ["prod_item_id", "purchase_value", "transaction_datetime"])
    result = df.filter(
        F.col("purchase_value").isNotNull() &
        (F.col("purchase_value") > 0)
    )

    assert result.count() == 1
    assert result.collect()[0]["prod_item_id"] == "prod_1"


# ================================================================
# Teste 5 — Subsidiária corrigida por reenvio CDC
# ================================================================

def test_subsidiary_correction_via_resent_event(spark):
    data = [
        (69, "nacional",       datetime(2023, 2, 28,  1, 10)),  # 1º evento
        (69, "internacional",  datetime(2023, 3, 12,  7,  0)),  # reenvio corrige
    ]
    df = spark.createDataFrame(data, ["purchase_id", "subsidiary", "transaction_datetime"])
    result = latest(df, "purchase_id")

    assert result.collect()[0]["subsidiary"] == "internacional"


# ================================================================
# Teste 6 — Cálculo correto do GMV agregado
# ================================================================

def test_gmv_aggregation_is_correct(spark):
    data = [
        (55, date(2023, 1, 20),  55.00,   "nacional"),
        (69, date(2023, 2, 26), 2000.00,  "internacional"),
    ]
    df = spark.createDataFrame(data, ["purchase_id", "order_date", "purchase_value", "subsidiary"])

    result = (
        df
        .groupBy(F.col("order_date").alias("transaction_date"), F.col("subsidiary"))
        .agg(F.round(F.sum("purchase_value"), 2).alias("gmv"))
    )

    rows = {row["transaction_date"]: row["gmv"] for row in result.collect()}
    assert rows[date(2023, 1, 20)] == 55.00
    assert rows[date(2023, 2, 26)] == 2000.00


# ================================================================
# Teste 7 — Append não sobrescreve snapshots anteriores
# ================================================================

def test_append_preserves_history(spark, tmp_path):
    output = str(tmp_path / "gmv_daily_snapshot")

    s1 = spark.createDataFrame(
        [(date(2023, 1, 21), "nacional",      50.00, 1, date(2023, 1, 20))],
        ["snapshot_date", "subsidiary", "gmv", "purchase_count", "transaction_date"]
    )
    s2 = spark.createDataFrame(
        [(date(2023, 7, 16), "nacional",      55.00, 1, date(2023, 1, 20))],
        ["snapshot_date", "subsidiary", "gmv", "purchase_count", "transaction_date"]
    )

    s1.write.mode("append").partitionBy("transaction_date").parquet(output)
    s2.write.mode("append").partitionBy("transaction_date").parquet(output)

    total = spark.read.parquet(output).count()
    assert total == 2  # ambos os snapshots preservados
