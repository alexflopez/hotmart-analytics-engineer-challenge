# Fluxo do ETL — Snapshot Histórico de GMV por Compra

```mermaid
flowchart TD
    A([Início — D-1]) --> B

    subgraph EXTRACT["Extração — S3 eventos CDC"]
        B[purchase]
        C[product_item]
        D[purchase_extra_info]
    end

    B --> E
    C --> F
    D --> G

    subgraph TRANSFORM["Transformação — PySpark"]
        E["Qualidade + Deduplica purchase\nMAX transaction_datetime\npor purchase_id"]
        F["Qualidade + Deduplica product_item\nMAX transaction_datetime\npor prod_item_id"]
        G["Qualidade + Deduplica purchase_extra_info\nMAX transaction_datetime\npor purchase_id"]
        E --> H
        F --> H
        G --> H
        H["LEFT JOIN\npurchase → product_item via prod_item_id\npurchase → extra_info via purchase_id"]
        H --> I["Filtro GMV\nrelease_date IS NOT NULL\npurchase_status = APROVADA\nsubsidiary IS NOT NULL"]
        I --> J["Seleciona campos por purchase_id\nAdiciona snapshot_date = D-1\nSem agregação — 1 linha por compra"]
    end

    subgraph LOAD["Carga — S3 / Athena"]
        J --> K["mode append\npartitionBy transaction_date\nParquet SNAPPY"]
        K --> L[(gmv_purchase_snapshot\ns3://your-bucket/)]
        L --> M["MSCK REPAIR TABLE\nAtualiza partições no Glue Data Catalog"]
    end

    subgraph QUERY["Consulta — Athena"]
        M --> N["ROW_NUMBER OVER\nPARTITION BY purchase_id\nORDER BY snapshot_date DESC"]
        N --> O["WHERE rn = 1\nSUM purchase_value\nGROUP BY order_date, subsidiary"]
        O --> P([GMV diário por subsidiária\nsem multiplicidade])
    end

    style EXTRACT   fill:#1e3a5f,color:#fff
    style TRANSFORM fill:#1a3a2a,color:#fff
    style LOAD      fill:#3a1a1a,color:#fff
    style QUERY     fill:#2a1a3a,color:#fff
```

## Por que a tabela não é agregada

A tabela final armazena **uma linha por compra por snapshot**, não o GMV já somado.

Isso é necessário porque:

- A mesma compra pode ser **corrigida retroativamente** (valor, subsidiária, status).
- Um `SELECT + SUM` direto sobre a tabela histórica traria **multiplicidade** — a mesma `purchase_id` somada N vezes.
- A navegação temporal exige saber o estado de cada `purchase_id` **em um ponto específico no tempo**, o que só é possível com granularidade por compra.

A agregação ocorre **na query**, após identificar o registro mais recente de cada `purchase_id` via `ROW_NUMBER`.

## Garantias da modelagem

| Requisito | Como é atendido |
|---|---|
| Imutabilidade | `mode("append")` — histórico nunca é sobrescrito |
| Idempotência | Deduplicação por `MAX(transaction_datetime)` antes do join |
| Rastreabilidade diária | Granularidade `snapshot_date × purchase_id` |
| Navegação temporal | Filtrar `snapshot_date <= data_referencia` + `ROW_NUMBER` |
| Registros correntes | `ROW_NUMBER` por `purchase_id` ordenado por `snapshot_date DESC` |
| Assincronismo entre tabelas | `LEFT JOIN` preserva compras com eventos ainda pendentes |
| Particionamento | `partitionBy("transaction_date")` — partition pruning no Athena |
| Sem multiplicidade na query | `ROW_NUMBER` antes do `SUM` — estado mais recente por compra |
