# Estratégia de Idempotência

## Objetivo

Garantir que reprocessamentos completos, reexecuções do pipeline ou reenvio de eventos CDC produzam resultados consistentes, sem duplicidade de registros ou impacto indevido no cálculo do GMV.

## Contexto

As tabelas de origem são alimentadas por eventos CDC, permitindo que informações sejam corrigidas ou reenviadas ao longo do tempo.

Dessa forma, uma mesma entidade pode possuir múltiplas versões armazenadas no Data Lake.

Exemplos:

* Correção do valor da compra.
* Alteração da subsidiária associada à transação.
* Atualização da data de liberação do pagamento.
* Reenvio de eventos já processados anteriormente.

## Estratégia Adotada

Para cada entidade, os registros são deduplicados utilizando sua chave de negócio e o campo `transaction_datetime`.

Apenas o evento mais recente é considerado durante o processamento.

### Chaves utilizadas

| Tabela              | Chave        |
| ------------------- | ------------ |
| purchase            | purchase_id  |
| product_item        | prod_item_id |
| purchase_extra_info | purchase_id  |

Em caso de múltiplos registros para a mesma chave, será selecionado o registro com maior `transaction_datetime`.

## Reprocessamentos

O pipeline realiza leitura completa dos eventos disponíveis nas tabelas de origem.

Essa abordagem garante que correções históricas sejam consideradas automaticamente durante a geração de novos snapshots.

Como a deduplicação é determinística, a execução repetida do pipeline sobre o mesmo conjunto de dados produz sempre o mesmo resultado lógico.

## Preservação Histórica

A camada analítica segue o princípio de imutabilidade.

Cada execução gera um novo snapshot identificado por `snapshot_date`.

Os registros históricos nunca são atualizados ou removidos.

Benefícios:

* Auditoria dos dados.
* Rastreabilidade temporal.
* Recuperação de estados anteriores.
* Reprocessamento seguro.

## Benefícios da Abordagem

* Proteção contra reenvio de eventos CDC.
* Consistência dos resultados.
* Eliminação de duplicidades.
* Reprocessamento confiável.
* Preservação do histórico analítico.
