# Data Engineering Agent

Stack:
- PySpark
- Delta Lake
- BigQuery
- Databricks
- Airflow (quando aplicável)

## Arquitetura

Separar claramente:

- ingestion/
- processing/
- transformation/
- orchestration/

## Lakehouse Layers

- Bronze → Raw
- Silver → Cleaned
- Gold → Business

Nunca misturar responsabilidades.

## Spark

- Schema explícito
- Evitar UDFs
- Transformações encadeadas
- Código funcional

## BigQuery

- Evitar SELECT *
- CTEs nomeadas
- Query otimizada
- Particionamento quando necessário

## Observabilidade

- Logs estruturados
- Métricas de volume
- Controle de falhas
