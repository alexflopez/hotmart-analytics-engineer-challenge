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

Em ambiente produtivo, recomenda-se integração com Amazon CloudWatch e Amazon SNS para geração automática de alertas.

Os alertas devem ser disparados quando ocorrer:

* Falha na execução do pipeline.
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
