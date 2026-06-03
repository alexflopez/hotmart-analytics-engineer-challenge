-- DDL da tabela histórica e imutável de snapshots de GMV por compra.
--

CREATE EXTERNAL TABLE IF NOT EXISTS gmv_purchase_snapshot (
    snapshot_date    DATE    COMMENT 'Data de processamento (D-1). Identifica o momento em que o snapshot foi gerado.',
    purchase_id      BIGINT  COMMENT 'Identificador da compra. Repete-se a cada snapshot — comportamento intencional para rastreabilidade.',
    order_date       DATE    COMMENT 'Data em que o pedido foi efetuado pelo comprador.',
    release_date     DATE    COMMENT 'Data de liberação do pagamento. Preenchida apenas quando o pagamento foi confirmado.',
    purchase_status  STRING  COMMENT 'Status da compra no momento do snapshot: APROVADA, CANCELADA, REEMBOLSADA, INICIADA.',
    purchase_value   DOUBLE  COMMENT 'Valor do item de compra (purchase_value de product_item).',
    subsidiary       STRING  COMMENT 'Subsidiária da transação: nacional ou internacional (purchase_extra_info).'
)
COMMENT 'Snapshot histórico e imutável por compra. Granularidade: snapshot_date x purchase_id. Nunca sofre UPDATE.'
PARTITIONED BY (
    transaction_date DATE    COMMENT 'Data da transação (order_date). Partição física da tabela no S3 — uso de partition pruning no Athena.'
)
STORED AS PARQUET
LOCATION 's3://your-bucket/gmv_purchase_snapshot/'
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY'
);
