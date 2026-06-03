# Estratégia de Qualidade de Dados

## Objetivo

Garantir que o cálculo do GMV diário por subsidiária seja realizado utilizando dados íntegros, consistentes e confiáveis, reduzindo riscos de distorções nos indicadores de negócio e aumentando a confiança das áreas consumidoras dos dados.

## Princípios Adotados

A estratégia de qualidade de dados foi construída considerando os seguintes pilares:

* Completude dos dados.
* Consistência entre entidades.
* Integridade das informações processadas.
* Confiabilidade dos resultados gerados.
* Rastreabilidade das transformações.

## Validações de Entrada

Antes do processamento, são aplicadas validações nas tabelas de origem para garantir que os registros possuam os atributos mínimos necessários para participação no cálculo do GMV.

### purchase

Validações:

* `purchase_id` não pode ser nulo.
* `order_date` não pode ser nula.
* `purchase_status` não pode ser nulo.

### product_item

Validações:

* `prod_item_id` não pode ser nulo.
* `purchase_value` não pode ser nulo.
* `purchase_value` deve ser maior que zero.

### purchase_extra_info

Validações:

* `purchase_id` não pode ser nulo.
* `subsidiary` não pode ser nula.

## Consistência dos Eventos CDC

Como as tabelas são alimentadas por eventos CDC, um mesmo registro pode ser reenviado ou corrigido ao longo do tempo.

Para garantir consistência, a solução mantém apenas a versão mais recente de cada entidade utilizando:

* Chave de negócio.
* Maior `transaction_datetime`.

Essa abordagem evita:

* Registros duplicados.
* Contagem incorreta de transações.
* Divergências causadas por reenvio de eventos.

## Regras de Negócio

Uma transação somente será considerada no cálculo do GMV quando atender simultaneamente aos seguintes critérios:

* `release_date` preenchida.
* `purchase_status = 'APROVADA'`.
* `subsidiary` preenchida.

Registros que não atendam a essas condições são excluídos do cálculo.

## Validações Pós-Processamento

Após a geração do snapshot, recomenda-se validar:

* Quantidade de registros processados.
* Quantidade de registros descartados.
* Quantidade de transações por subsidiária.
* Valor total do GMV calculado.
* Comparação com execuções anteriores para identificação de anomalias.

## Auditoria e Rastreabilidade

Cada execução gera um novo snapshot identificado por `snapshot_date`.

Essa abordagem permite:

* Auditoria dos resultados.
* Reprocessamento seguro.
* Recuperação de estados históricos.
* Investigação de divergências de negócio.
