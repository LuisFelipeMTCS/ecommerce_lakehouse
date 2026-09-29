-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 07 - Validação de governança
-- MAGIC
-- MAGIC Confere que as máscaras e o row filter estão de fato aplicados, lista os metadados de
-- MAGIC governança do Unity Catalog (`information_schema.column_masks` / `row_filters`),
-- MAGIC os GRANTs concedidos e as tags de PII. Também cria a view dinâmica
-- MAGIC `gold.vw_clientes_regional`, usada como plano B caso `MASK`/`WITH ROW FILTER` não sejam
-- MAGIC suportados diretamente na materialized view `gold.dim_clientes` no ambiente de execução.

-- COMMAND ----------

CREATE WIDGET TEXT catalogo DEFAULT 'ecommerce_lakehouse';

-- COMMAND ----------

USE CATALOG ${catalogo};

-- COMMAND ----------

-- MAGIC %md ## Consultas de negócio sobre a dimensão mascarada

-- COMMAND ----------

SELECT * FROM gold.dim_clientes LIMIT 20;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## View dinâmica alternativa (`gold.vw_clientes_regional`)
-- MAGIC
-- MAGIC Reaplica a mesma máscara/row filter por fora do pipeline, útil como plano B e também
-- MAGIC como segunda camada de defesa para consumidores que acessam via view comum.

-- COMMAND ----------

CREATE OR REPLACE VIEW gold.vw_clientes_regional AS
SELECT
  cliente_id,
  nome,
  governanca.mascara_email(email)       AS email,
  governanca.mascara_cpf(cpf)           AS cpf,
  governanca.mascara_telefone(telefone) AS telefone,
  cidade,
  uf,
  data_nascimento,
  vigente_desde_evento_seq
FROM gold.dim_clientes
WHERE governanca.filtro_uf(uf);

-- COMMAND ----------

SELECT * FROM gold.vw_clientes_regional LIMIT 20;

-- COMMAND ----------

-- MAGIC %md ## Metadados de governança do Unity Catalog

-- COMMAND ----------

SELECT * FROM information_schema.column_masks
WHERE table_catalog = '${catalogo}' AND table_schema = 'gold';

-- COMMAND ----------

SELECT * FROM information_schema.row_filters
WHERE table_catalog = '${catalogo}' AND table_schema = 'gold';

-- COMMAND ----------

SHOW GRANTS ON SCHEMA silver;

-- COMMAND ----------

SHOW GRANTS ON SCHEMA gold;

-- COMMAND ----------

SELECT * FROM information_schema.schemata
WHERE catalog_name = '${catalogo}';
