"""
ETL - GMV Diário por Subsidiária
==================================
Stack : PySpark | Amazon EMR | AWS S3 | Parquet / SNAPPY | AWS Athena
Execução : D-1

Arquitetura AWS:
    Eventos CDC → S3 (raw)
        → EMR / PySpark (processamento)
            → S3 gmv_daily_snapshot/ (Parquet / SNAPPY)
                → Glue Data Catalog (metadados + partições)
                    → Athena (consulta SQL)

Definição de GMV (conforme desafio):
    Soma do purchase_value de transações cujo pagamento foi efetuado
    e não foi cancelado:
        - release_date IS NOT NULL
        - purchase_status = 'APROVADA'

Modelagem de imutabilidade:
    A tabela de destino nunca sofre UPDATE — apenas INSERT (append).
    Cada execução grava um snapshot identificado por snapshot_date.
    O histórico nunca é alterado, mesmo com reprocessamento full.
    Para consultar o estado atual: WHERE snapshot_date = MAX(snapshot_date).
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import date, timedelta

spark = (
    SparkSession.builder
    .appName("gmv_daily_snapshot")
    .config("spark.sql.parquet.compression.codec", "snappy")
    # Necessário para que o Athena enxergue as partições via Glue Data Catalog
    .config("hive.metastore.client.factory.class",
            "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    .enableHiveSupport()
    .getOrCreate()
)

S3_BUCKET     = "s3://your-bucket"
snapshot_date = date.today() - timedelta(days=1)  # D-1

# ================================================================
# EXTRAÇÃO
# Lê todos os eventos CDC das três tabelas de origem no S3.
# O pipeline é sempre full — garante que reenvios de eventos
# históricos sejam considerados no snapshot do dia.
# ================================================================

purchase   = spark.read.parquet(f"{S3_BUCKET}/events/purchase/")
product    = spark.read.parquet(f"{S3_BUCKET}/events/product_item/")
extra_info = spark.read.parquet(f"{S3_BUCKET}/events/purchase_extra_info/")

# ================================================================
# QUALIDADE DE DADOS
# Remove registros com colunas críticas nulas ou inválidas
# antes de processar. Evita contaminação do GMV.
# ================================================================

purchase = purchase.filter(
    F.col("purchase_id").isNotNull() &
    F.col("order_date").isNotNull() &
    F.col("purchase_status").isNotNull()
)

product = product.filter(
    F.col("prod_item_id").isNotNull() &
    F.col("purchase_value").isNotNull() &
    (F.col("purchase_value") > 0)       # valor de compra não pode ser zero ou negativo
)

extra_info = extra_info.filter(
    F.col("purchase_id").isNotNull() &
    F.col("subsidiary").isNotNull()
)

# ================================================================
# DEDUPLICAÇÃO — coração da idempotência
# ----------------------------------------------------------------
# Eventos CDC podem ser reenviados para corrigir dados presentes
# ou passados. Para cada chave, mantemos apenas o evento com
# transaction_datetime mais recente.
# Rodar o pipeline 1x ou 10x produz o mesmo resultado.
# ================================================================

def latest(df, key):
    w = Window.partitionBy(key).orderBy(F.col("transaction_datetime").desc())
    return (
        df
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

purchase   = latest(purchase,   "purchase_id")
product    = latest(product,    "prod_item_id")
extra_info = latest(extra_info, "purchase_id")

# ================================================================
# JOIN DAS TRÊS TABELAS
# ----------------------------------------------------------------
# purchase → product_item via prod_item_id
# purchase → purchase_extra_info via purchase_id
#
# LEFT JOIN: eventos das três tabelas podem não chegar no mesmo
# dia (assincronismo). O LEFT JOIN preserva todas as compras,
# mesmo que extra_info ou product ainda não tenham chegado.
# Esses registros serão removidos no filtro GMV abaixo.
# ================================================================

joined = (
    purchase
    .join(product,    on="prod_item_id", how="left")
    .join(extra_info, on="purchase_id",  how="left")
)

# ================================================================
# FILTRO GMV
# ----------------------------------------------------------------
# Conforme definição do desafio:
#   1. release_date preenchida — pagamento confirmado
#   2. purchase_status = APROVADA — exclui CANCELADA e REEMBOLSADA
#   3. subsidiary preenchida — só contabiliza quando sabemos
#      a qual subsidiária pertence a transação
# ================================================================

gmv_df = joined.filter(
    F.col("release_date").isNotNull() &
    F.col("purchase_status").isin("APROVADA") &
    F.col("subsidiary").isNotNull()
)

# ================================================================
# AGREGAÇÃO
# ----------------------------------------------------------------
# GMV diário por subsidiária:
#   - transaction_date = order_date (data da transação)
#   - gmv = soma do purchase_value dos itens
#   - purchase_count = quantidade de transações no dia
# ================================================================

gmv = (
    gmv_df
    .groupBy(
        F.col("order_date").alias("transaction_date"),
        F.col("subsidiary")
    )
    .agg(
        F.round(F.sum("purchase_value"), 2).alias("gmv"),
        F.count("purchase_id").alias("purchase_count")
    )
    .withColumn("snapshot_date", F.lit(snapshot_date))
    .select(
        "snapshot_date",
        "subsidiary",
        "gmv",
        "purchase_count",
        "transaction_date",  # partição — última coluna por convenção Parquet
    )
)

# ================================================================
# CARGA — append puro
# ----------------------------------------------------------------
# mode("append"): nunca sobrescreve dados existentes.
# partitionBy("transaction_date"): organiza os arquivos no S3
# por data — partition pruning no Athena reduz custo e latência.
#
# Imutabilidade garantida aqui: cada execução apenas adiciona
# novas linhas. O histórico nunca é tocado.
# ================================================================

(
    gmv
    .write
    .mode("append")
    .partitionBy("transaction_date")
    .parquet(f"{S3_BUCKET}/gmv_daily_snapshot/")
)

# ================================================================
# ATUALIZAÇÃO DO GLUE DATA CATALOG
# ----------------------------------------------------------------
# Após gravar novas partições no S3, o Athena não as enxerga
# automaticamente. MSCK REPAIR TABLE atualiza o Glue Data Catalog
# com as novas partições, tornando-as imediatamente consultáveis.
# ================================================================

spark.sql("MSCK REPAIR TABLE gmv_daily_snapshot")

spark.stop()
