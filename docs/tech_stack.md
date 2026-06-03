# Tech Stack — GMV Daily Snapshot

## Arquitetura AWS

```
Eventos CDC
    └── S3 (raw/eventos — Parquet/SNAPPY)
            └── Amazon EMR (cluster Spark — executa gmv_etl.py)
                    └── S3 (gmv_purchase_snapshot/ — Parquet/SNAPPY)
                            └── Glue Data Catalog (schema + partições)
                                    └── Athena (consulta SQL serverless)
Orquestração: Amazon MWAA (Airflow gerenciado) — agendamento diário
Alertas:      Amazon SNS + E-mail (via EmailOperator na DAG)
Monitoramento: CloudWatch — logs e alertas do job EMR
Segurança:    IAM — roles e permissões entre os serviços
```

## Componentes

| Recurso | Papel | Justificativa |
|---|---|---|
| **S3** | Armazena eventos CDC e tabela final em Parquet | Durável, baixo custo, integração nativa com EMR e Athena |
| **Amazon EMR** | Executa o PySpark — processamento distribuído | Controle total do cluster; ideal para jobs recorrentes e pesados |
| **Glue Data Catalog** | Registra schema e partições da `gmv_purchase_snapshot` | Athena precisa do catálogo para enxergar a tabela e suas partições no S3 |
| **Athena** | Consulta SQL serverless sobre o Parquet no S3 | Sem infraestrutura; paga por query; acessível ao time de negócio |
| **Amazon MWAA** | Orquestra o job diário no EMR via Airflow gerenciado | Sem servidor para manter; suporte nativo a DAGs e reprocessamento |
| **Amazon SNS** | Notifica sistemas externos em caso de falha na DAG | Integração com PagerDuty, Slack e outros canais de alerta |
| **E-mail (SMTP)** | Notifica destinatários diretamente em caso de falha na DAG | Configurado via `EmailOperator` na DAG; destinatários em `gmv_alert_emails` |
| **CloudWatch** | Monitora logs e alertas do job EMR | Integrado nativamente ao EMR |
| **IAM** | Controla permissões entre os serviços | S3, EMR, Glue e Athena requerem roles específicas |

## Fluxo de execução

```
MWAA (Airflow — agendamento diário)
    └── Submete job para EMR
            ├── Lê eventos CDC do S3 (purchase, product_item, purchase_extra_info)
            ├── Qualidade: remove nulos e valores inválidos
            ├── Deduplicação: MAX(transaction_datetime) por chave
            ├── LEFT JOIN: purchase + product_item + purchase_extra_info
            ├── Filtro GMV: release_date IS NOT NULL + status APROVADA + subsidiary IS NOT NULL
            ├── Snapshot: 1 linha por purchase_id — sem agregação
            ├── Grava Parquet no S3 (mode=append, partitionBy transaction_date)
            └── MSCK REPAIR TABLE → atualiza partições no Glue Data Catalog
                    └── Athena enxerga as novas partições imediatamente
    └── Em caso de falha:
            ├── SNS publica alerta no tópico gmv_alerts
            └── EmailOperator envia e-mail para gmv_alert_emails
```

## Decisões de modelagem

**Granularidade por compra — sem agregação no pipeline**
A tabela armazena uma linha por `purchase_id` por `snapshot_date`, não o GMV já
somado. A agregação ocorre na query, após identificar o estado mais recente de cada
`purchase_id` via `ROW_NUMBER`. Isso é necessário para suportar navegação temporal
e evitar multiplicidade ao consultar a tabela histórica.

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
Apenas transações com `release_date` preenchida, `purchase_status = APROVADA`
e `subsidiary` identificada entram no cálculo. Transações com status `CANCELADA`
ou `REEMBOLSADA` são explicitamente excluídas.

**Particionamento por transaction_date**
Permite partition pruning no Athena — lê apenas as partições necessárias,
reduzindo custo e latência das consultas.

**Navegação temporal**
```sql
-- GMV de Jan/2023 como estava em 31/01/2023:
WITH snapshot_at AS (
    SELECT purchase_id, order_date, subsidiary, purchase_value,
           ROW_NUMBER() OVER (PARTITION BY purchase_id ORDER BY snapshot_date DESC) AS rn
    FROM gmv_purchase_snapshot
    WHERE snapshot_date <= '2023-01-31'
      AND transaction_date BETWEEN '2023-01-01' AND '2023-01-31'
)
SELECT order_date, subsidiary, ROUND(SUM(purchase_value), 2) AS gmv
FROM snapshot_at WHERE rn = 1
GROUP BY order_date, subsidiary;

-- GMV corrente (estado mais recente de cada compra):
WITH latest AS (
    SELECT purchase_id, order_date, subsidiary, purchase_value,
           ROW_NUMBER() OVER (PARTITION BY purchase_id ORDER BY snapshot_date DESC) AS rn
    FROM gmv_purchase_snapshot
)
SELECT order_date, subsidiary, ROUND(SUM(purchase_value), 2) AS gmv
FROM latest WHERE rn = 1
GROUP BY order_date, subsidiary;
```