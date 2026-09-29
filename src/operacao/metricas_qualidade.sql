-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 05 - Métricas de qualidade e observabilidade do pipeline
-- MAGIC
-- MAGIC A CLI de bundles usada neste workspace ainda não suporta configurar `event_log` (tabela
-- MAGIC publicada) no `databricks.yml`, então consultamos o event log diretamente com a função
-- MAGIC de tabela `event_log(TABLE(...))`, apontando para qualquer tabela produzida pelo pipeline
-- MAGIC (o Databricks resolve o pipeline dono da tabela automaticamente). Criamos uma view de
-- MAGIC apoio `monitoramento.vw_event_log` para não repetir essa chamada em toda consulta.

-- COMMAND ----------

CREATE WIDGET TEXT catalogo DEFAULT 'ecommerce_lakehouse';

-- COMMAND ----------

USE CATALOG ${catalogo};

-- COMMAND ----------

CREATE OR REPLACE VIEW monitoramento.vw_event_log AS
SELECT * FROM event_log(TABLE(${catalogo}.silver.pedidos));

-- COMMAND ----------

-- MAGIC %md ## View de qualidade por expectation (aprovados/reprovados)

-- COMMAND ----------

CREATE OR REPLACE VIEW monitoramento.vw_qualidade_expectations AS
SELECT
  expectation.dataset::STRING              AS dataset,
  expectation.name::STRING                 AS regra,
  expectation.passed_records::BIGINT       AS aprovados,
  expectation.failed_records::BIGINT       AS reprovados,
  timestamp
FROM monitoramento.vw_event_log
LATERAL VIEW explode(from_json(
  details:flow_progress.data_quality.expectations::STRING,
  'array<struct<name:string,dataset:string,passed_records:bigint,failed_records:bigint>>'
)) AS expectation
WHERE event_type = 'flow_progress'
  AND details:flow_progress.data_quality.expectations IS NOT NULL;

-- COMMAND ----------

SELECT dataset, regra, sum(aprovados) AS total_aprovados, sum(reprovados) AS total_reprovados
FROM monitoramento.vw_qualidade_expectations
GROUP BY ALL
ORDER BY dataset, regra;

-- COMMAND ----------

-- MAGIC %md ## Quarentena por regra violada

-- COMMAND ----------

SELECT regra, count(*) AS qtd
FROM silver.pedidos_quarentena
LATERAL VIEW explode(regras_violadas) AS regra
GROUP BY regra
ORDER BY qtd DESC;

-- COMMAND ----------

-- MAGIC %md ## Linhas processadas e descartadas por flow

-- COMMAND ----------

SELECT
  origin.flow_name AS flow_name,
  sum(details:flow_progress.metrics.num_output_rows::BIGINT) AS linhas_processadas,
  sum(coalesce(details:flow_progress.data_quality.dropped_records::BIGINT, 0)) AS linhas_descartadas
FROM monitoramento.vw_event_log
WHERE event_type = 'flow_progress'
GROUP BY ALL
ORDER BY flow_name;

-- COMMAND ----------

-- MAGIC %md ## Duração de cada atualização do pipeline

-- COMMAND ----------

SELECT
  origin.update_id         AS update_id,
  min(timestamp)           AS inicio,
  max(timestamp)           AS fim,
  cast(max(timestamp) AS DOUBLE) - cast(min(timestamp) AS DOUBLE) AS duracao_segundos
FROM monitoramento.vw_event_log
WHERE event_type = 'update_progress'
GROUP BY origin.update_id
ORDER BY inicio DESC;

-- COMMAND ----------

-- MAGIC %md ## Planning information das materialized views

-- COMMAND ----------

SELECT
  details:planning_information AS planejamento,
  timestamp,
  message
FROM monitoramento.vw_event_log
WHERE event_type = 'planning_information'
ORDER BY timestamp DESC;

-- COMMAND ----------

-- MAGIC %md ## Uso de recursos do cluster serverless

-- COMMAND ----------

SELECT timestamp, details:cluster_resources AS cluster_resources
FROM monitoramento.vw_event_log
WHERE event_type = 'cluster_resources'
ORDER BY timestamp DESC;

-- COMMAND ----------

-- MAGIC %md ## Linhagem de tabelas (system.access.table_lineage)

-- COMMAND ----------

SELECT
  source_table_catalog, source_table_schema, source_table_name,
  target_table_catalog, target_table_schema, target_table_name,
  event_time
FROM system.access.table_lineage
WHERE source_table_catalog = '${catalogo}' OR target_table_catalog = '${catalogo}'
ORDER BY event_time DESC
LIMIT 200;
