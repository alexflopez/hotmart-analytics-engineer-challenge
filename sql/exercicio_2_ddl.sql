-- Criação da tabela externa no Athena para consulta dos snapshots históricos de GMV.
-- Os dados são armazenados em formato Parquet com compressão Snappy para reduzir
-- custo de armazenamento e volume de leitura.
-- O particionamento por transaction_date permite partition pruning, melhorando
-- a performance das consultas e reduzindo custos de processamento.

CREATE EXTERNAL TABLE IF NOT EXISTS gmv_daily_snapshot (
    snapshot_date    DATE      COMMENT 'Data em que o pipeline executou (D-1). Chave de navegação temporal.',
    subsidiary       STRING    COMMENT 'Subsidiária da transação: nacional ou internacional.',
    gmv              DOUBLE    COMMENT 'Soma do purchase_value das transações aprovadas com release_date preenchida.',
    purchase_count   BIGINT    COMMENT 'Quantidade de transações consideradas no cálculo do GMV.'
)
COMMENT 'Snapshot histórico e imutável do GMV diário por subsidiária. Nunca sofre UPDATE — apenas INSERT.'
PARTITIONED BY (
    transaction_date DATE      COMMENT 'Data da transação (order_date). Partição física da tabela no S3.'
)
STORED AS PARQUET
LOCATION 's3://your-bucket/gmv_daily_snapshot/'
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY'
);
