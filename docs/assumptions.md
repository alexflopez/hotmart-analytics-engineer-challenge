# Premissas e Requisitos do Desafio

## Objetivo

Disponibilizar o GMV diário por subsidiária de forma histórica, rastreável e auditável,
processando eventos CDC provenientes das tabelas:

- `purchase`
- `product_item`
- `purchase_extra_info`

O resultado final deve permitir que qualquer pessoa — inclusive sem conhecimento
sólido em SQL — consulte o GMV de um período específico de forma simples e confiável.

---

## Definição de GMV

GMV (Gross Merchandise Value) é a soma do `purchase_value` de transações que
atendam **simultaneamente** os três critérios abaixo:

| Critério | Campo | Condição |
|---|---|---|
| Pagamento confirmado | `release_date` | `IS NOT NULL` |
| Compra não cancelada | `purchase_status` | `= 'APROVADA'` |
| Subsidiária identificada | `subsidiary` | `IS NOT NULL` |

> Fonte: transcrição do vídeo — *"só faturamos alguma coisa se o comprador pagar"*;
> PDF do desafio — *"apenas transações com Data Liberação preenchida"*.

---

## Modelo de Dados de Origem

As três tabelas de origem operam no **modelo de eventos (CDC)**:

- Cada alteração em um registro gera uma **nova linha** na tabela — não há UPDATE.
- O campo `transaction_datetime` registra quando aquele evento foi salvo no banco.
- A mesma `purchase_id` pode ter múltiplos eventos ao longo do tempo.

### Assincronismo entre tabelas

Uma compra **sempre terá** registros correspondentes nas três tabelas, mas:

- Os registros das três tabelas **não chegam necessariamente no mesmo dia**.
- Não há garantia de qual tabela recebe o evento primeiro.
- O intervalo entre a chegada de eventos de tabelas diferentes pode ser de **dias**.

> Exemplo da transcrição: `purchase_id 55` foi registrada em `purchase` e `product_item`
> no dia 20/01, mas o registro em `purchase_extra_info` só chegou no dia 23/01.

### Reenvio de eventos históricos

- Em caso de falha de envio, eventos podem ser **reenviados** para corrigir dados
  presentes **ou passados**.
- A solução deve absorver reenvios sem duplicar nem alterar snapshots já gerados.

---

## Requisitos da Modelagem

### Imutabilidade

- O pipeline usa **`mode=append`** — nenhum snapshot anterior é sobrescrito.
- Reprocessamento full do pipeline produz o **mesmo resultado** (idempotência).

### Rastreabilidade diária

- A tabela final armazena **uma linha por `purchase_id` por `snapshot_date`**.
- `snapshot_date` = D-1 (data de processamento).
- É possível reconstruir o estado de qualquer compra em qualquer data passada.

### Navegação temporal

- Requisito explícito do desafio: consultar o GMV de Jan/2023 em 31/01/2023
  **e** em qualquer data futura deve retornar valores **consistentes e distintos**
  conforme o momento de referência.
- Implementado via filtro `snapshot_date <= <data_referencia>` + `ROW_NUMBER`
  para selecionar o estado mais recente de cada `purchase_id` dentro do período.

### Repetição de dados ativos

- Se apenas uma tabela sofreu atualização num dia, os dados das demais são
  **repetidos** no snapshot daquele dia (não ficam nulos).
- Implementado via `LEFT JOIN` — campos ausentes recebem o último valor conhecido.

### Registros correntes

- Deve ser simples recuperar o estado atual de cada compra.
- Implementado via `ROW_NUMBER OVER (PARTITION BY purchase_id ORDER BY snapshot_date DESC) = 1`.

### Particionamento

- Tabela particionada por `transaction_date` para permitir **partition pruning**
  no Athena e reduzir custo e latência das consultas.

---

## Critérios de Qualidade

| Critério | Implementação |
|---|---|
| **Idempotência** | Deduplicação por `MAX(transaction_datetime)` antes do join; `mode=append` |
| **Qualidade de dados** | Filtros de campos críticos nulos ou inválidos antes do processamento |
| **Escalabilidade** | PySpark no EMR; Parquet/SNAPPY; partition pruning no Athena |
| **Auditabilidade** | Snapshots imutáveis; `snapshot_date` em toda linha; navegação temporal |

---

## Decisões de Modelagem

### Por que não agregar na tabela final?

Agregar o GMV diretamente no pipeline (um `SUM` por `transaction_date × subsidiary`)
tornaria impossível a navegação temporal — uma correção retroativa de valor alteraria
o passado sem rastro. A tabela armazena **granularidade por compra**; a agregação
ocorre **na query**, após identificar o estado mais recente de cada `purchase_id`.

### Por que LEFT JOIN?

O assincronismo entre tabelas significa que, no momento do snapshot, uma compra
pode ainda não ter registro em `product_item` ou `purchase_extra_info`. O `LEFT JOIN`
preserva a compra com campos nulos até os dados chegarem nos snapshots seguintes.

### Por que `subsidiary IS NOT NULL` no filtro de GMV?

A subsidiária indica se a venda foi nacional ou internacional. Compras sem essa
informação ainda não têm os dados completos para compor o GMV — entrarão no
cálculo no snapshot do dia em que `purchase_extra_info` chegar.