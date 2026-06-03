# Hotmart Analytics Engineer Challenge

Solução técnica para o desafio de Analytics Engineer da Hotmart, composta por dois exercícios.

## Estrutura do Repositório

```
hotmart-analytics-engineer-challenge/
├── data/
│   └── exercicio_2_amostra_dados_teste.sql     # Dataset final populado com dados do desafio
├── diagrams/
│   └── gmv_flow.md                   # Fluxograma do pipeline ETL (Mermaid)
├── docs/
│   ├── assumptions.md                # Premissas e requisitos da solução
│   ├── architecture.md               # Arquitetura da solução
│   ├── idempotency.md                # Estratégia adotada para idempotência do projeto
│   └── tech_stack.md                 # Descrição da stack AWS
├── spark/
│   └── gmv_etl.py                    # ETL PySpark — GMV diário por subsidiária
├── sql/
│   ├── tables_ddl.sql                # DDL de todas as tabelas 
│   ├── execicio_1_queries.sql        # DDL das tabelas (Exercício 1)
│   ├── exercicio_2_ddl.sql           # DDL da tabela gmv_daily_snapshot
│   └── exercicio_2_gmv.sql           # Query GMV corrente por subsidiária
└── tests/
    └── test_gmv_etl.py               # Testes unitários do ETL
```

## Exercício 1 — SQL

Queries para responder:
- Top 50 produtores em faturamento em 2021
- Top 2 produtos por faturamento de cada produtor

**Entregáveis:** `sql/execicio_1_queries.sql`

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
| `sql/exercicio_2_ddl.sql` | DDL da tabela final |
| `data/exercicio_2_amostra_dados_teste.sql` | Exemplo do dataset final populado |
| `sql/exercicio_2_gmv.sql` | Query GMV corrente |
| `docs/tech_stack.md` | Descrição da tech stack |
| `diagrams/gmv_flow.md` | Fluxograma do pipeline |
| `tests/test_gmv_etl.py` | Testes unitários |
