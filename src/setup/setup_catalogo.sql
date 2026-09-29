-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 01 - Setup do catálogo
-- MAGIC
-- MAGIC Cria a estrutura base do Unity Catalog para o projeto de e-commerce:
-- MAGIC catálogo, schemas por camada (bronze/silver/gold + apoio) e os volumes usados
-- MAGIC pelo Auto Loader (landing) e pelos checkpoints de streaming.
-- MAGIC
-- MAGIC Também cria a tabela `referencia.categorias`, que é a **dependência externa**
-- MAGIC do pipeline DLT (carregada fora do pipeline, via MERGE, e apenas lida pelo Lakeflow).

-- COMMAND ----------

CREATE WIDGET TEXT catalogo DEFAULT 'ecommerce_lakehouse';

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS ${catalogo}
COMMENT 'Catálogo do projeto acadêmico de Data Lakehouse de e-commerce';

-- COMMAND ----------

USE CATALOG ${catalogo};

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS bronze
COMMENT 'Camada bronze: dados brutos ingeridos via Auto Loader, sem transformação de negócio';

CREATE SCHEMA IF NOT EXISTS silver
COMMENT 'Camada silver: dados limpos, deduplicados e com CDC aplicado (SCD1/SCD2)';

CREATE SCHEMA IF NOT EXISTS gold
COMMENT 'Camada gold: agregações e visões de negócio prontas para consumo analítico';

CREATE SCHEMA IF NOT EXISTS referencia
COMMENT 'Dados de referência mantidos fora do pipeline DLT (dependência externa)';

CREATE SCHEMA IF NOT EXISTS governanca
COMMENT 'Funções de máscara, filtros de linha e tabelas de apoio para Unity Catalog';

CREATE SCHEMA IF NOT EXISTS auditoria
COMMENT 'Tabelas e volumes de apoio para auditoria, snapshots e checkpoints';

CREATE SCHEMA IF NOT EXISTS monitoramento
COMMENT 'Event log do pipeline e views de métricas de qualidade/observabilidade';

-- COMMAND ----------

CREATE VOLUME IF NOT EXISTS bronze.landing
COMMENT 'Volume de pouso (landing zone) dos arquivos gerados pelo gerador de dados, lidos via Auto Loader';

CREATE VOLUME IF NOT EXISTS auditoria.checkpoints
COMMENT 'Checkpoints e schema location de jobs de streaming fora do pipeline DLT (Auto Loader standalone)';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Dependência externa do pipeline: `referencia.categorias`
-- MAGIC
-- MAGIC Tabela mantida fora do Lakeflow (carga manual/agendada via MERGE), lida pela camada
-- MAGIC gold para enriquecer os pedidos. Usa deletion vectors e uma PRIMARY KEY informativa
-- MAGIC (não aplicada, mas documenta o contrato) mais um CHECK constraint de negócio.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS referencia.categorias (
  categoria_id   INT NOT NULL,
  nome_categoria STRING NOT NULL,
  margem_alvo    DECIMAL(5,2),
  CONSTRAINT pk_categorias PRIMARY KEY (categoria_id)
)
COMMENT 'Dependência externa do pipeline: categorias de produto e margem alvo, carregada fora do DLT'
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true'
);

-- COMMAND ----------

-- CHECK constraints não são aceitos inline no CREATE TABLE, apenas via ALTER TABLE
ALTER TABLE referencia.categorias DROP CONSTRAINT IF EXISTS chk_margem_alvo;
ALTER TABLE referencia.categorias ADD CONSTRAINT chk_margem_alvo
  CHECK (margem_alvo IS NULL OR (margem_alvo >= 0 AND margem_alvo <= 1));

-- COMMAND ----------

MERGE INTO referencia.categorias AS destino
USING (
  SELECT * FROM (VALUES
    (1, 'Eletrônicos',        0.18),
    (2, 'Moda e Vestuário',   0.35),
    (3, 'Casa e Decoração',   0.28),
    (4, 'Esporte e Lazer',    0.30),
    (5, 'Livros e Papelaria', 0.22),
    (6, 'Beleza e Cuidados',  0.40),
    (7, 'Alimentos e Bebidas',0.15),
    (8, 'Brinquedos',         0.32)
  ) AS t(categoria_id, nome_categoria, margem_alvo)
) AS origem
ON destino.categoria_id = origem.categoria_id
WHEN MATCHED THEN UPDATE SET
  destino.nome_categoria = origem.nome_categoria,
  destino.margem_alvo    = origem.margem_alvo
WHEN NOT MATCHED THEN INSERT (categoria_id, nome_categoria, margem_alvo)
  VALUES (origem.categoria_id, origem.nome_categoria, origem.margem_alvo);

-- COMMAND ----------

SELECT * FROM referencia.categorias ORDER BY categoria_id;
