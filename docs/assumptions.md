# Premissas e Requisitos do Desafio

## Objetivo

Disponibilizar o GMV diário por subsidiária considerando eventos CDC provenientes das tabelas:

- purchase
- product_item
- purchase_extra_info

## Requisitos Identificados

- Processamento baseado em CDC
- Atualização assíncrona entre entidades
- Reenvio de eventos históricos
- Imutabilidade histórica
- Rastreabilidade diária
- Recuperação simples dos registros correntes
- Particionamento por transaction_date
- Atualização D-1

## Critérios de Qualidade

- Idempotência
- Qualidade de dados
- Escalabilidade
- Auditabilidade