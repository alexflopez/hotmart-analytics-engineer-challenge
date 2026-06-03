# Tech Stack — GMV Daily Snapshot

## Arquitetura AWS

```
Eventos CDC
    └── S3 (raw/eventos — Parquet/SNAPPY)
            └── Amazon EMR (cluster Spark — executa gmv_etl.py)
                    └── S3 (gmv_daily_snapshot/ — Parquet/SNAPPY)
                            └── Glue Data Catalog (schema + partições)
                                    └── Athena (consulta SQL serverless)

Orquestração: Amazon MWAA (Airflow gerenciado) — agendamento D-1
Monitoramento: CloudWatch — logs e alertas do job EMR
Segurança: IAM — roles e permissões entre os serviços
```

## Componentes

| Recurso | Papel | Justificativa |
|---|---|---|
| **S3** | Armazena eventos CDC e tabela final em Parquet | Durável, baixo custo, integração nativa com EMR e Athena |
| **Amazon EMR** | Executa o PySpark — processamento distribuído | Controle total do cluster; ideal para jobs recorrentes e pesados |
| **Glue Data Catalog** | Registra schema e partições da `gmv_daily_snapshot` | Athena precisa do catálogo para enxergar a tabela e suas partições no S3 |
| **Athena** | Consulta SQL serverless sobre o Parquet no S3 | Sem infraestrutura; paga por query; acessível ao time de negócio |
| **Amazon MWAA** | Orquestra o job D-1 no EMR via Airflow gerenciado | Sem servidor para manter; suporte nativo a DAGs e reprocessamento |
| **CloudWatch** | Monitora logs e alertas do job EMR | Integrado nativamente ao EMR |
| **IAM** | Controla permissões entre os serviços | S3, EMR, Glue e Athena requerem roles específicas |

## Fluxo de execução

```
MWAA (Airflow D-1)
    └── Submete job para EMR
            ├── Lê eventos CDC do S3
            ├── Qualidade: remove nulos e valores inválidos
            ├── Deduplicação: MAX(transaction_datetime) por chave
            ├── JOIN: purchase + product_item + purchase_extra_info
            ├── Filtro GMV: release_date IS NOT NULL + status APROVADA
            ├── Agregação: SUM(purchase_value) por transaction_date × subsidiary
            ├── Grava Parquet no S3 (mode=append)
            └── MSCK REPAIR TABLE → atualiza partições no Glue Data Catalog
                    └── Athena enxerga as novas partições imediatamente
```

## Decisões de modelagem

**Imutabilidade via append puro**
A tabela nunca sofre UPDATE. Cada execução insere novas linhas com o
`snapshot_date` do dia. Snapshots anteriores permanecem intactos mesmo
com reprocessamento full.

**Idempotência via deduplicação CDC**
Antes do join, cada tabela é deduplicada pelo `transaction_datetime` mais
recente por chave. Rodar o pipeline uma ou dez vezes produz o mesmo resultado.

**Pipeline full — não incremental**
O pipeline relê todos os eventos a cada execução. Isso garante que reenvios
de eventos históricos sejam sempre considerados no snapshot do dia, sem
lógica adicional de detecção de mudanças.

**Qualidade de dados**
Registros com colunas críticas nulas ou com `purchase_value` inválido são
descartados antes do processamento, evitando contaminação do GMV.

**Definição de GMV aplicada**
Apenas transações com `release_date` preenchida e `purchase_status = APROVADA`
entram no cálculo. Transações com status `CANCELADA` ou `REEMBOLSADA` são
explicitamente excluídas.

**Particionamento por transaction_date**
Permite partition pruning no Athena — lê apenas as partições necessárias,
reduzindo custo e latência das consultas.

**Navegação temporal**
```sql
-- GMV de Jan/2023 como estava em 31/03/2023:
WHERE snapshot_date = '2023-03-31'

-- GMV corrente (estado mais recente):
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM gmv_daily_snapshot)

