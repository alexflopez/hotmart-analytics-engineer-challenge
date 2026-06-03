# Estratégia de Monitoramento e Alertas

## Objetivo

Garantir a confiabilidade operacional do pipeline de cálculo do GMV, permitindo rápida identificação de falhas, degradações de desempenho e anomalias nos dados processados.

## Monitoramento Operacional

Durante a execução do pipeline serão monitorados indicadores relacionados à saúde do processamento:

### Execução do Pipeline

* Início e término da execução.
* Tempo total de processamento.
* Status da execução (Sucesso ou Falha).
* Quantidade de registros processados.
* Quantidade de registros descartados por regras de qualidade.

### Infraestrutura

* Consumo de CPU dos nós de processamento.
* Utilização de memória.
* Espaço em armazenamento temporário.
* Falhas de leitura ou escrita no Amazon S3.

## Monitoramento de Qualidade dos Dados

Além da execução técnica, a solução monitora indicadores relacionados à qualidade dos dados.

### Validações

* Volume de registros recebidos por tabela.
* Percentual de registros inválidos.
* Registros com campos obrigatórios nulos.
* Quantidade de eventos deduplicados.
* Distribuição de transações por subsidiária.

### Indicadores de Negócio

* Valor total do GMV calculado.
* Quantidade de compras aprovadas.
* Quantidade de compras excluídas do cálculo.
* Variações significativas em relação às execuções anteriores.

Anomalias nesses indicadores podem indicar falhas de ingestão ou problemas nas fontes de dados.

## Estratégia de Alertas

A solução utiliza dois canais de notificação complementares, ambos disparados automaticamente pela DAG (`dag_gmv.py`) em caso de falha em qualquer etapa do pipeline:

### E-mail

A DAG dispara um `EmailOperator` com `trigger_rule=ONE_FAILED` assim que qualquer task falha. O e-mail contém:

* Nome da DAG e da task que falhou.
* Data de execução.
* Link direto para o log no Airflow.

Os destinatários são configurados na Airflow Variable `gmv_alert_emails` (separados por vírgula). O envio utiliza o servidor SMTP configurado no ambiente MWAA.

### Amazon SNS

Em paralelo ao e-mail, a DAG publica uma mensagem no tópico SNS configurado em `gmv_sns_topic_arn`, permitindo integração com outros sistemas de alerta (PagerDuty, Slack, etc.).

### Demais condições de alerta

Recomenda-se configurar alertas adicionais no Amazon CloudWatch quando ocorrer:

* Tempo de processamento acima do esperado.
* Falha de leitura ou escrita no S3.
* Queda abrupta no volume de registros processados.
* Crescimento anormal de registros rejeitados.
* Divergência significativa no GMV calculado.

## Rastreabilidade

Todos os eventos relevantes da execução devem ser registrados em logs estruturados contendo:

* Data e hora da execução.
* Quantidade de registros processados.
* Quantidade de registros descartados.
* Tempo de execução.
* Mensagens de erro e exceções.

Esses registros permitem auditoria, troubleshooting e análise de incidentes.

## Benefícios

* Maior confiabilidade operacional.
* Detecção antecipada de falhas.
* Redução do tempo de resposta a incidentes.
* Melhor governança dos dados.
* Maior confiança nos indicadores de negócio disponibilizados para consumo analítico.