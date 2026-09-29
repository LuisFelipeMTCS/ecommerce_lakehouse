-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 04 - Manutenção Delta: histórico, auditoria e otimização
-- MAGIC
-- MAGIC Demonstra time travel, `table_changes` (CDF), um snapshot de auditoria com
-- MAGIC UPDATE/DELETE/MERGE manuais, um DELETE "acidental" seguido de `RESTORE`, e as rotinas
-- MAGIC de manutenção (`OPTIMIZE ... ZORDER BY`, `REORG TABLE ... APPLY (PURGE)`, `VACUUM`).

-- COMMAND ----------

CREATE WIDGET TEXT catalogo DEFAULT 'ecommerce_lakehouse';

-- COMMAND ----------

USE CATALOG ${catalogo};

-- COMMAND ----------

-- MAGIC %md ## Histórico e time travel nas tabelas silver

-- COMMAND ----------

DESCRIBE HISTORY silver.pedidos;

-- COMMAND ----------

DESCRIBE HISTORY silver.clientes;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC `table_changes` expõe o Change Data Feed habilitado nas tabelas CDC — aqui olhamos as
-- MAGIC mudanças na versão mais recente da tabela de pedidos.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # table_changes exige uma versao literal (nao aceita subquery), entao calculamos aqui
-- MAGIC versao_inicial = (
-- MAGIC     spark.sql("DESCRIBE HISTORY silver.pedidos")
-- MAGIC     .selectExpr("max(version) - 1 AS v")
-- MAGIC     .collect()[0]["v"]
-- MAGIC )
-- MAGIC versao_inicial = max(int(versao_inicial or 0), 0)
-- MAGIC display(spark.sql(f"""
-- MAGIC     SELECT _change_type, count(*) AS qtd
-- MAGIC     FROM table_changes('silver.pedidos', {versao_inicial})
-- MAGIC     GROUP BY ALL
-- MAGIC """))

-- COMMAND ----------

-- MAGIC %md ## Snapshot de auditoria (`auditoria.pedidos_snapshot`)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS auditoria.pedidos_snapshot
COMMENT 'Snapshot de auditoria de pedidos, usado para demonstrar UPDATE/DELETE/MERGE e RESTORE'
TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')
AS SELECT * FROM silver.pedidos;

-- COMMAND ----------

DESCRIBE DETAIL auditoria.pedidos_snapshot;

-- COMMAND ----------

UPDATE auditoria.pedidos_snapshot
SET status = 'ENTREGUE'
WHERE status = 'ENVIADO' AND pedido_ts < current_timestamp() - INTERVAL 30 DAYS;

-- COMMAND ----------

DELETE FROM auditoria.pedidos_snapshot
WHERE status = 'CANCELADO' AND pedido_ts < current_timestamp() - INTERVAL 365 DAYS;

-- COMMAND ----------

MERGE INTO auditoria.pedidos_snapshot AS destino
USING silver.pedidos AS origem
ON destino.pedido_id = origem.pedido_id
WHEN MATCHED AND destino.status != origem.status THEN UPDATE SET destino.status = origem.status
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

-- MAGIC %md ### Métricas extraídas do histórico

-- COMMAND ----------

SELECT
  version,
  timestamp,
  operation,
  operationMetrics['numTargetRowsUpdated']  AS linhas_atualizadas,
  operationMetrics['numTargetRowsDeleted']  AS linhas_deletadas,
  operationMetrics['numTargetRowsInserted'] AS linhas_inseridas
FROM (DESCRIBE HISTORY auditoria.pedidos_snapshot)
ORDER BY version DESC;

-- COMMAND ----------

-- MAGIC %md ### DELETE "acidental" + RESTORE

-- COMMAND ----------

DELETE FROM auditoria.pedidos_snapshot;

-- COMMAND ----------

SELECT count(*) AS linhas_apos_delete_acidental FROM auditoria.pedidos_snapshot;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # RESTORE TO VERSION AS OF tambem exige uma versao literal, nao subquery
-- MAGIC versao_antes_delete = (
-- MAGIC     spark.sql("DESCRIBE HISTORY auditoria.pedidos_snapshot")
-- MAGIC     .filter("operation != 'DELETE'")
-- MAGIC     .selectExpr("max(version) AS v")
-- MAGIC     .collect()[0]["v"]
-- MAGIC )
-- MAGIC spark.sql(f"RESTORE TABLE auditoria.pedidos_snapshot TO VERSION AS OF {versao_antes_delete}")

-- COMMAND ----------

SELECT count(*) AS linhas_apos_restore FROM auditoria.pedidos_snapshot;

-- COMMAND ----------

-- MAGIC %md ## Otimização: OPTIMIZE / REORG / VACUUM

-- COMMAND ----------

DESCRIBE DETAIL auditoria.pedidos_snapshot;

-- COMMAND ----------

OPTIMIZE auditoria.pedidos_snapshot ZORDER BY (cliente_id, pedido_ts);

-- COMMAND ----------

REORG TABLE auditoria.pedidos_snapshot APPLY (PURGE);

-- COMMAND ----------

DESCRIBE DETAIL auditoria.pedidos_snapshot;

-- COMMAND ----------

VACUUM auditoria.pedidos_snapshot RETAIN 168 HOURS DRY RUN;

-- COMMAND ----------

VACUUM auditoria.pedidos_snapshot RETAIN 168 HOURS;
