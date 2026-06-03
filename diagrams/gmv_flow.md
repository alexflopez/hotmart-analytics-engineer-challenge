# Fluxo do ETL — GMV Diário por Subsidiária

```mermaid
flowchart TD
    A([Início — D-1]) --> B

    subgraph EXTRACT["Extração — S3"]
        B[purchase\nevents/cdc]
        C[product_item\nevents/cdc]
        D[purchase_extra_info\nevents/cdc]
    end

    B --> E
    C --> F
    D --> G

    subgraph TRANSFORM["Transformação — PySpark"]
        E["Deduplica purchase\nMAX transaction_datetime\npor purchase_id"]
        F["Deduplica product_item\nMAX transaction_datetime\npor prod_item_id"]
        G["Deduplica purchase_extra_info\nMAX transaction_datetime\npor purchase_id"]

        E --> H
        F --> H
        G --> H

        H["LEFT JOIN\npurchase → product_item\npurchase → extra_info"]
        H --> I["Filtra release_date IS NOT NULL\napenas pagamentos confirmados"]
        I --> J["Agrega por\ntransaction_date × subsidiary\nSUM purchase_value"]
        J --> K["Adiciona snapshot_date = D-1"]
    end

    subgraph LOAD["Carga — S3 / Athena"]
        K --> L["mode append\npartitionBy transaction_date\nParquet SNAPPY"]
        L --> M[(gmv_daily_snapshot\ns3://your-bucket/)]
    end

    M --> N["Query corrente\nWHERE snapshot_date =\nMAX snapshot_date"]
    N --> O([GMV diário por subsidiária])

    style EXTRACT  fill:#1e3a5f,color:#fff
    style TRANSFORM fill:#1a3a2a,color:#fff
    style LOAD     fill:#3a1a1a,color:#fff
```

## Garantias da modelagem

| Requisito | Como é atendido |
|---|---|
| Imutabilidade | `mode("append")` — histórico nunca é sobrescrito |
| Idempotência | Deduplicação por `MAX(transaction_datetime)` antes do join |
| Navegação temporal | Cada execução grava um `snapshot_date` distinto |
| Registros correntes | `WHERE snapshot_date = MAX(snapshot_date)` |
| Assincronismo entre tabelas | `LEFT JOIN` preserva compras com eventos pendentes |
| Particionamento | `partitionBy("transaction_date")` — partition pruning no Athena |
