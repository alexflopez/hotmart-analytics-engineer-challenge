"""
ETL - Snapshot Histórico de GMV por Compra
===========================================
Stack    : PySpark | Amazon EMR | AWS S3 | Parquet / SNAPPY | AWS Athena
Execução : D-1

Arquitetura AWS:
    Eventos CDC → S3 (raw)
        → EMR / PySpark (processamento)
            → S3 gmv_purchase_snapshot/ (Parquet / SNAPPY)
                → Glue Data Catalog (metadados + partições)
                    → Athena (consulta SQL)

Definição de GMV (conforme desafio):
    Soma do purchase_value de transações cujo pagamento foi efetuado
    e não foi cancelado:
        - release_date IS NOT NULL
        - purchase_status = 'APROVADA'

Modelagem da tabela final:
    Granularidade: uma linha por (snapshot_date, purchase_id).
    A mesma compra se repete a cada snapshot em que há alteração.
    Isso permite navegar no tempo: consultar o estado de uma compra
    em qualquer data de snapshot passada.

Imutabilidade:
    A tabela nunca sofre UPDATE — apenas INSERT (append puro).
    Reprocessamento full produz o mesmo resultado (idempotência).

Registros correntes:
    Para obter o último estado de cada compra, utilizar ROW_NUMBER()
    particionado por purchase_id, ordenado por snapshot_date DESC.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import date, timedelta

# ================================================================
# SESSÃO SPARK
# Configurado para rodar no Amazon EMR com Glue Data Catalog.
# enableHiveSupport() permite executar MSCK REPAIR TABLE ao final.
# ================================================================

spark = (
    SparkSession.builder
    .appName("gmv_purchase_snapshot")
    .config("spark.sql.parquet.compression.codec", "snappy")
    .config(
        "hive.metastore.client.factory.class",
        "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
    )
    .enableHiveSupport()
    .getOrCreate()
)

S3_BUCKET     = "s3://your-bucket"
snapshot_date = date.today() - timedelta(days=1)  # D-1

# ================================================================
# EXTRAÇÃO
# Lê todos os eventos CDC das três tabelas de origem no S3.
# Pipeline full (não incremental): garante que reenvios de eventos
# históricos sejam sempre considerados no snapshot do dia.
# ================================================================

purchase   = spark.read.parquet(f"{S3_BUCKET}/events/purchase/")
product    = spark.read.parquet(f"{S3_BUCKET}/events/product_item/")
extra_info = spark.read.parquet(f"{S3_BUCKET}/events/purchase_extra_info/")

# ================================================================
# QUALIDADE DE DADOS
# Remove registros com campos críticos nulos ou inválidos
# antes do processamento para evitar contaminação do GMV.
# ================================================================

purchase = purchase.filter(
    F.col("purchase_id").isNotNull() &
    F.col("order_date").isNotNull() &
    F.col("purchase_status").isNotNull()
)

product = product.filter(
    F.col("prod_item_id").isNotNull() &
    F.col("purchase_value").isNotNull() &
    (F.col("purchase_value") > 0)  # valor não pode ser zero ou negativo
)

extra_info = extra_info.filter(
    F.col("purchase_id").isNotNull()
    # subsidiary pode ser nula aqui — evento pode ainda não ter chegado
    # será tratado no filtro GMV abaixo
)

# ================================================================
# DEDUPLICAÇÃO — coração da idempotência
# ----------------------------------------------------------------
# Eventos CDC podem ser reenviados para corrigir dados presentes
# ou passados. Para cada chave de negócio, mantemos apenas o
# evento com transaction_datetime mais recente.
# Executar o pipeline 1x ou 10x produz o mesmo resultado.
# ================================================================

def keep_latest(df, partition_key):
    """Mantém apenas o evento mais recente por chave de negócio."""
    w = Window.partitionBy(partition_key).orderBy(F.col("transaction_datetime").desc())
    return (
        df
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

purchase   = keep_latest(purchase,   "purchase_id")
product    = keep_latest(product,    "prod_item_id")
extra_info = keep_latest(extra_info, "purchase_id")

# ================================================================
# JOIN DAS TRÊS TABELAS
# ----------------------------------------------------------------
# purchase → product_item    via prod_item_id
# purchase → extra_info      via purchase_id
#
# LEFT JOIN: eventos das três tabelas chegam de forma assíncrona —
# uma compra pode existir em purchase antes de ter registro em
# product_item ou extra_info. O LEFT JOIN preserva a compra
# mesmo com dados ainda pendentes.
# ================================================================

joined = (
    purchase
    .join(product,    on="prod_item_id", how="left")
    .join(extra_info, on="purchase_id",  how="left")
)

# ================================================================
# FILTRO GMV
# ----------------------------------------------------------------
# Somente transações que atendam simultaneamente:
#   1. release_date preenchida → pagamento confirmado
#   2. purchase_status = APROVADA → exclui CANCELADA e REEMBOLSADA
#   3. subsidiary preenchida → subsidiária identificada
#
# Registros sem subsidiary ainda não entram no GMV — entrarão
# no snapshot do dia em que a informação chegar.
# ================================================================

gmv_df = joined.filter(
    F.col("release_date").isNotNull() &
    F.col("purchase_status").isin("APROVADA") &
    F.col("subsidiary").isNotNull()
)

# ================================================================
# SNAPSHOT POR COMPRA
# ----------------------------------------------------------------
# A tabela final tem granularidade por purchase_id.
# A mesma compra se repete a cada snapshot — isso é intencional
# e permite navegação temporal.
#
# Para calcular o GMV de um período, a query deve primeiro
# identificar o estado mais recente de cada purchase_id dentro
# do intervalo desejado (via ROW_NUMBER), e só então agregar.
# ================================================================

snapshot_df = (
    gmv_df
    .select(
        F.lit(str(snapshot_date)).cast("date").alias("snapshot_date"),
        F.col("purchase_id"),
        F.col("order_date"),
        F.col("release_date"),
        F.col("purchase_status"),
        F.round(F.col("purchase_value"), 2).alias("purchase_value"),
        F.col("subsidiary"),
        F.col("order_date").alias("transaction_date"),  # partição — última coluna
    )
)

# ================================================================
# CARGA — append puro
# ----------------------------------------------------------------
# mode("append"): nunca sobrescreve dados existentes.
# partitionBy("transaction_date"): organiza os arquivos no S3
# por data da transação — partition pruning no Athena reduz
# custo e latência das consultas.
#
# Imutabilidade garantida: cada execução apenas adiciona novas
# linhas com o snapshot_date do dia. O histórico nunca é tocado.
# ================================================================

(
    snapshot_df
    .write
    .mode("append")
    .partitionBy("transaction_date")
    .parquet(f"{S3_BUCKET}/gmv_purchase_snapshot/")
)

# ================================================================
# ATUALIZAÇÃO DO GLUE DATA CATALOG
# ----------------------------------------------------------------
# Após gravar novas partições no S3, o Athena não as enxerga
# automaticamente. MSCK REPAIR TABLE sincroniza o Glue Data
# Catalog com o S3, tornando as novas partições consultáveis
# imediatamente após cada execução.
# ================================================================

spark.sql("MSCK REPAIR TABLE gmv_purchase_snapshot")

spark.stop()
