-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 02 - Governança e Unity Catalog
-- MAGIC
-- MAGIC Cria as funções de máscara (e-mail, CPF, telefone), a estrutura de row filter por UF,
-- MAGIC os GRANTs de isolamento entre camadas/grupos e as tags de PII. Os grupos
-- MAGIC `ecom_engenharia`, `ecom_analistas` e `ecom_pii_leitores` são provisionados via CLI
-- MAGIC (fora deste notebook, ver README.md) antes deste notebook rodar.

-- COMMAND ----------

CREATE WIDGET TEXT catalogo DEFAULT 'ecommerce_lakehouse';

-- COMMAND ----------

USE CATALOG ${catalogo};

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Funções de máscara
-- MAGIC
-- MAGIC Cada função libera o valor real para membros de `ecom_engenharia` (dono do dado) ou
-- MAGIC `ecom_pii_leitores` (leitura autorizada de PII); qualquer outro leitor recebe o valor mascarado.

-- COMMAND ----------

CREATE OR REPLACE FUNCTION governanca.mascara_email(email STRING)
RETURNS STRING
COMMENT 'Mascara e-mail mostrando apenas o domínio, exceto para engenharia e leitores de PII'
RETURN CASE
  WHEN is_account_group_member('ecom_engenharia') OR is_account_group_member('ecom_pii_leitores')
    THEN email
  WHEN email IS NULL OR NOT contains(email, '@') THEN '***@desconhecido'
  ELSE concat('***@', split(email, '@')[1])
END;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION governanca.mascara_cpf(cpf STRING)
RETURNS STRING
COMMENT 'Mascara CPF mantendo apenas os dois últimos dígitos, exceto para engenharia e leitores de PII'
RETURN CASE
  WHEN is_account_group_member('ecom_engenharia') OR is_account_group_member('ecom_pii_leitores')
    THEN cpf
  WHEN cpf IS NULL OR length(cpf) < 2 THEN '***.***.*-**'
  ELSE concat('***.***.*', right(cpf, 2))
END;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION governanca.mascara_telefone(telefone STRING)
RETURNS STRING
COMMENT 'Mascara telefone mantendo apenas os 4 últimos dígitos, exceto para engenharia e leitores de PII'
RETURN CASE
  WHEN is_account_group_member('ecom_engenharia') OR is_account_group_member('ecom_pii_leitores')
    THEN telefone
  WHEN telefone IS NULL OR length(telefone) < 4 THEN '****'
  ELSE concat('****', right(telefone, 4))
END;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Row filter por UF
-- MAGIC
-- MAGIC `ecom_engenharia`, `ecom_pii_leitores` e `admins` enxergam todas as UFs; `ecom_analistas`
-- MAGIC só enxerga a região Sudeste + GO/DF conforme `governanca.acesso_uf`.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS governanca.acesso_uf (
  grupo STRING NOT NULL,
  uf    STRING NOT NULL
)
COMMENT 'Mapa de quais UFs cada grupo de analistas pode enxergar via row filter';

-- COMMAND ----------

DELETE FROM governanca.acesso_uf WHERE grupo = 'ecom_analistas';

INSERT INTO governanca.acesso_uf (grupo, uf)
VALUES ('ecom_analistas', 'SP'),
       ('ecom_analistas', 'RJ'),
       ('ecom_analistas', 'MG'),
       ('ecom_analistas', 'GO'),
       ('ecom_analistas', 'DF');

-- COMMAND ----------

CREATE OR REPLACE FUNCTION governanca.filtro_uf(uf STRING)
RETURNS BOOLEAN
COMMENT 'Row filter: engenharia/pii/admins veem tudo; analistas só veem as UFs liberadas em governanca.acesso_uf'
RETURN
  is_account_group_member('ecom_engenharia')
  OR is_account_group_member('ecom_pii_leitores')
  OR is_account_group_member('admins')
  OR EXISTS (
    SELECT 1 FROM governanca.acesso_uf a
    WHERE a.grupo = 'ecom_analistas' AND a.uf = uf AND is_account_group_member('ecom_analistas')
  );

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Tags de PII nos schemas

-- COMMAND ----------

ALTER SCHEMA silver SET TAGS ('contem_pii' = 'true');
ALTER SCHEMA gold SET TAGS ('contem_pii' = 'parcial');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## GRANTs de isolamento entre camadas e grupos
-- MAGIC
-- MAGIC - `ecom_engenharia`: lê/escreve bronze, silver e gold, e usa o volume de landing.
-- MAGIC - `ecom_analistas` e `ecom_pii_leitores`: **apenas SELECT na gold** — nenhum acesso a
-- MAGIC   silver/bronze, garantindo isolamento da camada com PII bruta.

-- COMMAND ----------

GRANT USE CATALOG ON CATALOG ${catalogo} TO `ecom_engenharia`;
GRANT USE CATALOG ON CATALOG ${catalogo} TO `ecom_analistas`;
GRANT USE CATALOG ON CATALOG ${catalogo} TO `ecom_pii_leitores`;

-- COMMAND ----------

GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA bronze TO `ecom_engenharia`;
GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA silver TO `ecom_engenharia`;
GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA gold   TO `ecom_engenharia`;
GRANT READ VOLUME, WRITE VOLUME ON VOLUME bronze.landing TO `ecom_engenharia`;

-- COMMAND ----------

GRANT USE SCHEMA ON SCHEMA gold TO `ecom_analistas`;
GRANT SELECT ON SCHEMA gold TO `ecom_analistas`;

GRANT USE SCHEMA ON SCHEMA gold TO `ecom_pii_leitores`;
GRANT SELECT ON SCHEMA gold TO `ecom_pii_leitores`;

-- COMMAND ----------

SELECT 'governanca configurada' AS status;
