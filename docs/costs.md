# Considerações de Custos

## Objetivo

A arquitetura proposta foi desenhada buscando equilíbrio entre escalabilidade, simplicidade operacional e eficiência financeira, considerando o processamento de grandes volumes de dados históricos.

## Amazon S3

O Amazon S3 foi escolhido como camada de armazenamento devido ao baixo custo por volume armazenado, alta durabilidade e integração nativa com os serviços analíticos da AWS.

Benefícios:

* Baixo custo de armazenamento.
* Escalabilidade praticamente ilimitada.
* Integração nativa com EMR, Glue e Athena.
* Separação entre armazenamento e processamento.

## Formato Parquet

Os dados são armazenados em formato Parquet com compressão Snappy.

Motivações:

* Redução do espaço ocupado em disco.
* Menor volume de leitura durante consultas.
* Melhor performance para workloads analíticos.
* Compatibilidade nativa com Spark e Athena.

Como o Athena cobra pela quantidade de dados lidos, a utilização de Parquet reduz diretamente o custo operacional da solução.

## Particionamento

A tabela de snapshots é particionada por `transaction_date`.

Benefícios:

* Redução do volume de dados escaneados.
* Menor tempo de execução das consultas.
* Redução de custos no Athena através de partition pruning.

## Amazon Athena

O Athena foi escolhido para consumo analítico por ser um serviço serverless.

Benefícios:

* Não exige provisionamento de infraestrutura.
* Cobrança baseada em utilização.
* Integração direta com S3 e Glue Data Catalog.
* Facilidade de consulta utilizando SQL padrão.

## Amazon EMR

O processamento distribuído é realizado através do Amazon EMR utilizando Spark.

Considerações:

* Adequado para grandes volumes de dados.
* Escalabilidade horizontal conforme demanda.
* Permite processamento batch eficiente.

Em ambiente produtivo, recomenda-se utilização de clusters temporários (ephemeral clusters), iniciados apenas durante a execução do pipeline, reduzindo custos de computação ociosa.
