# Hotmart Analytics Engineer Challenge

Solução técnica para o desafio de Analytics Engineer da Hotmart, composta por dois exercícios.

## Estrutura do Repositório

```
hotmart-analytics-engineer-challenge/
├── data/
│   └── exercise2_sample_data.sql     # Dataset final populado com dados do desafio
├── diagrams/
│   └── gmv_flow.md                   # Fluxograma do pipeline ETL (Mermaid)
├── docs/
│   ├── assumptions.md                # Premissas e requisitos da solução
│   ├── architecture.md               # Arquitetura da solução
│   └── tech_stack.md                 # Descrição da stack AWS
├── spark/
│   └── gmv_etl.py                    # ETL PySpark — GMV diário por subsidiária
├── sql/
│   ├── exercise1_ddl.sql             # DDL das tabelas (Exercício 1)
│   ├── exercise1_queries.sql         # Queries top 50 produtores e top 2 produtos
│   ├── exercise2_ddl.sql             # DDL da tabela gmv_daily_snapshot
│   └── exercise2_gmv_current.sql     # Query GMV corrente por subsidiária
└── tests/
    └── test_gmv_etl.py               # Testes unitários do ETL
```

## Exercício 1 — SQL

Queries para responder:
- Top 50 produtores em faturamento em 2021
- Top 2 produtos por faturamento de cada produtor

**Entregáveis:** `sql/exercise1_ddl.sql` e `sql/exercise1_queries.sql`

## Exercício 2 — Modelagem e Desenvolvimento

Pipeline ETL que processa eventos CDC de `purchase`, `product_item` e `purchase_extra_info`
e materializa um snapshot histórico e imutável do GMV diário por subsidiária.

**Definição de GMV:** soma do `purchase_value` de transações com `release_date` preenchida
e `purchase_status = 'APROVADA'`.

### Stack

| Camada | Tecnologia |
|---|---|
| Processamento | PySpark |
| Execução | Amazon EMR |
| Armazenamento | AWS S3 (Parquet / SNAPPY) |
| Catálogo | AWS Glue Data Catalog |
| Consulta | AWS Athena |
| Orquestração | Amazon MWAA (Airflow) |

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
| `sql/exercise2_ddl.sql` | DDL da tabela final |
| `data/exercise2_sample_data.sql` | Exemplo do dataset final populado |
| `sql/exercise2_gmv_current.sql` | Query GMV corrente |
| `docs/tech_stack.md` | Descrição da tech stack |
| `diagrams/gmv_flow.md` | Fluxograma do pipeline |
| `tests/test_gmv_etl.py` | Testes unitários |