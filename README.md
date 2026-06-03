# Hotmart Analytics Engineer Challenge

Solução técnica para o desafio de Analytics Engineer da Hotmart, composta por dois exercícios.

## Estrutura do Repositório

```
hotmart-analytics-engineer-challenge/
├── data/
│   └── exercicio_2_amostra_dados_teste.sql     # Dataset de exemplo com navegação temporal
├── diagrams/
│   └── gmv_flow.md                             # Fluxograma do pipeline ETL (Mermaid)
├── docs/
│   ├── assumptions.md                          # Premissas e requisitos da solução
│   ├── costs.md                                # Considerações de custo da arquitetura AWS
│   ├── data_quality.md                         # Estratégia de qualidade de dados
│   ├── idempotency.md                          # Estratégia de idempotência
│   ├── monitoring.md                           # Estratégia de monitoramento e alertas
│   └── tech_stack.md                           # Descrição da stack AWS
├── spark/
│   └── gmv_etl.py                              # ETL PySpark — snapshot histórico por compra
├── sql/
│   ├── tables_ddl.sql                          # DDL das tabelas de origem (Exercício 1)
│   ├── exercicio_1_queries.sql                 # Queries SQL (Exercício 1)
│   ├── exercicio_2_ddl.sql                     # DDL da tabela gmv_purchase_snapshot
│   └── exercicio_2_gmv.sql                     # Query GMV corrente + navegação temporal
└── tests/
    └── test_gmv_etl.py                         # Testes unitários do ETL
```

---

## Exercício 1 — SQL

Queries para responder:

- Top 50 produtores em faturamento ($) em 2021
- Top 2 produtos por faturamento ($) de cada produtor

**Critério de faturamento:** `release_date IS NOT NULL` — somente compras pagas.

**Entregáveis:** `sql/exercicio_1_queries.sql`, `sql/tables_ddl.sql`

---

## Exercício 2 — Modelagem e Desenvolvimento

Pipeline ETL que processa eventos CDC de `purchase`, `product_item` e `purchase_extra_info` e materializa um snapshot histórico e imutável do GMV diário por subsidiária.

### Definição de GMV

Soma do `purchase_value` de transações que atendam simultaneamente:

- `release_date IS NOT NULL` — pagamento confirmado
- `purchase_status = 'APROVADA'` — exclui canceladas e reembolsadas
- `subsidiary IS NOT NULL` — subsidiária identificada

### Modelagem da tabela final

A tabela `gmv_purchase_snapshot` armazena **uma linha por `purchase_id` por `snapshot_date`**.

A mesma compra se repete a cada execução do pipeline — esse é o comportamento intencional que viabiliza:

- **Rastreabilidade:** histórico completo de cada compra ao longo do tempo
- **Navegação temporal:** consultar o GMV de Jan/2023 como estava em 31/01/2023 ou como está hoje
- **Imutabilidade:** snapshots anteriores nunca são alterados

> **Por que não agregar diretamente?**
> Um `SELECT + SUM` sobre a tabela histórica resultaria em multiplicidade — a mesma `purchase_id` somada N vezes. A query utiliza `ROW_NUMBER` para selecionar o estado mais recente de cada compra antes de agregar.

### Stack

| Camada | Tecnologia |
|---|---|
| Processamento | PySpark |
| Execução | Amazon EMR |
| Armazenamento | AWS S3 (Parquet / SNAPPY) |
| Catálogo | AWS Glue Data Catalog |
| Consulta | AWS Athena |
| Orquestração | Amazon MWAA (Airflow) |
| Monitoramento | Amazon CloudWatch |

### Como executar

```bash
# Submeter job no EMR
spark-submit s3://your-bucket/scripts/gmv_etl.py

# Executar testes localmente
pip install pyspark pytest
pytest tests/test_gmv_etl.py -v
```

### Entregáveis

| Arquivo | Descrição |
|---|---|
| `spark/gmv_etl.py` | Script ETL PySpark |
| `sql/exercicio_2_ddl.sql` | DDL da tabela `gmv_purchase_snapshot` |
| `data/exercicio_2_amostra_dados_teste.sql` | Dataset de exemplo com navegação temporal |
| `sql/exercicio_2_gmv.sql` | Query GMV corrente e navegação temporal |
| `docs/tech_stack.md` | Descrição da tech stack AWS |
| `diagrams/gmv_flow.md` | Fluxograma do pipeline |
| `tests/test_gmv_etl.py` | Testes unitários |
