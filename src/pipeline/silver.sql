-- # Silver — limpeza, expectations e CDC
-- MAGIC
-- Para cada entidade: uma streaming table intermediária de "eventos válidos" (com
-- expectations que descartam ou sinalizam linhas ruins) e um `FLOW ... AUTO CDC INTO`
-- que aplica os eventos na tabela final com SCD1 ou SCD2, conforme a regra de negócio.
-- Pedidos inválidos vão para uma tabela de quarentena com o motivo da rejeição.

-- ## Clientes — SCD Type 2 (histórico completo)

CREATE OR REFRESH STREAMING TABLE silver.clientes_eventos_validos (
  CONSTRAINT id_valido        EXPECT (cliente_id IS NOT NULL)                      ON VIOLATION DROP ROW,
  CONSTRAINT sequencia_valida EXPECT (evento_seq IS NOT NULL)                      ON VIOLATION DROP ROW,
  CONSTRAINT operacao_valida  EXPECT (operacao IN ('INSERT', 'UPDATE', 'DELETE'))  ON VIOLATION DROP ROW,
  CONSTRAINT email_valido     EXPECT (operacao = 'DELETE' OR (email IS NOT NULL AND email LIKE '%@%')) ON VIOLATION DROP ROW,
  CONSTRAINT cpf_valido       EXPECT (operacao = 'DELETE' OR (cpf IS NOT NULL AND length(cpf) = 11))   ON VIOLATION DROP ROW,
  CONSTRAINT uf_valida        EXPECT (uf IS NOT NULL)
)
COMMENT 'Eventos de cliente padronizados e validados antes do CDC'
AS SELECT
  cliente_id,
  trim(nome)                              AS nome,
  lower(trim(email))                      AS email,
  cpf,
  telefone,
  cidade,
  upper(uf)                               AS uf,
  try_cast(data_nascimento AS DATE)       AS data_nascimento,
  operacao,
  evento_seq,
  try_cast(evento_ts AS TIMESTAMP)        AS evento_ts
FROM STREAM(bronze.clientes_raw);

CREATE OR REFRESH STREAMING TABLE silver.clientes (
  CONSTRAINT id_final_valido EXPECT (cliente_id IS NOT NULL)
)
COMMENT 'Dimensão de clientes com histórico completo (SCD Type 2), mantida via CDC'
TBLPROPERTIES (
  'delta.enableDeletionVectors'  = 'true',
  'delta.enableChangeDataFeed'   = 'true',
  'pipelines.autoOptimize.zOrderCols' = 'cliente_id'
);

CREATE FLOW clientes_cdc_flow AS AUTO CDC INTO silver.clientes
FROM STREAM(silver.clientes_eventos_validos)
KEYS (cliente_id)
APPLY AS DELETE WHEN operacao = 'DELETE'
SEQUENCE BY evento_seq
COLUMNS * EXCEPT (operacao, evento_seq)
STORED AS SCD TYPE 2;

-- ## Pedidos — SCD Type 1 (estado atual) + quarentena

CREATE OR REFRESH STREAMING TABLE silver.pedidos_eventos_validos (
  CONSTRAINT pedido_id_valido   EXPECT (pedido_id IS NOT NULL)                                       ON VIOLATION DROP ROW,
  CONSTRAINT cliente_id_valido  EXPECT (cliente_id IS NOT NULL)                                       ON VIOLATION DROP ROW,
  CONSTRAINT quantidade_valida  EXPECT (coalesce(quantidade, 0) > 0)                                  ON VIOLATION DROP ROW,
  CONSTRAINT preco_valido       EXPECT (coalesce(preco_unitario, -1) > 0)                              ON VIOLATION DROP ROW,
  CONSTRAINT status_valido      EXPECT (status IN ('CRIADO', 'PAGO', 'ENVIADO', 'ENTREGUE', 'CANCELADO')) ON VIOLATION DROP ROW,
  CONSTRAINT data_coerente      EXPECT (coalesce(pedido_ts, TIMESTAMP '1900-01-01') <= coalesce(evento_ts, TIMESTAMP '9999-12-31')) ON VIOLATION DROP ROW,
  CONSTRAINT canal_valido       EXPECT (canal IS NOT NULL)
)
COMMENT 'Eventos de pedido validados; linhas ruins vão para silver.pedidos_quarentena'
AS SELECT
  pedido_id,
  cliente_id,
  produto_id,
  quantidade,
  preco_unitario,
  status,
  canal,
  try_cast(pedido_ts AS TIMESTAMP) AS pedido_ts,
  try_cast(evento_ts AS TIMESTAMP) AS evento_ts
FROM STREAM(bronze.pedidos_raw);

CREATE OR REFRESH STREAMING TABLE silver.pedidos_quarentena
COMMENT 'Eventos de pedido que violaram alguma regra de qualidade, com o motivo da rejeição'
AS SELECT
  pedido_id,
  cliente_id,
  produto_id,
  quantidade,
  preco_unitario,
  status,
  canal,
  try_cast(pedido_ts AS TIMESTAMP) AS pedido_ts,
  try_cast(evento_ts AS TIMESTAMP) AS evento_ts,
  filter(array(
    CASE WHEN pedido_id IS NULL THEN 'pedido_id_nulo' END,
    CASE WHEN cliente_id IS NULL THEN 'cliente_id_nulo' END,
    CASE WHEN coalesce(quantidade, 0) <= 0 THEN 'quantidade_invalida' END,
    CASE WHEN coalesce(preco_unitario, -1) <= 0 THEN 'preco_invalido' END,
    CASE WHEN status NOT IN ('CRIADO', 'PAGO', 'ENVIADO', 'ENTREGUE', 'CANCELADO') OR status IS NULL THEN 'status_invalido' END,
    CASE WHEN coalesce(try_cast(pedido_ts AS TIMESTAMP), TIMESTAMP '1900-01-01') > coalesce(try_cast(evento_ts AS TIMESTAMP), TIMESTAMP '9999-12-31') THEN 'data_futura' END
  ), x -> x IS NOT NULL) AS regras_violadas
FROM STREAM(bronze.pedidos_raw)
WHERE NOT (
  pedido_id IS NOT NULL
  AND cliente_id IS NOT NULL
  AND coalesce(quantidade, 0) > 0
  AND coalesce(preco_unitario, -1) > 0
  AND status IN ('CRIADO', 'PAGO', 'ENVIADO', 'ENTREGUE', 'CANCELADO')
  AND coalesce(try_cast(pedido_ts AS TIMESTAMP), TIMESTAMP '1900-01-01') <= coalesce(try_cast(evento_ts AS TIMESTAMP), TIMESTAMP '9999-12-31')
);

CREATE OR REFRESH STREAMING TABLE silver.pedidos (
  CONSTRAINT id_final_valido EXPECT (pedido_id IS NOT NULL)
)
COMMENT 'Estado atual dos pedidos (SCD Type 1), mantida via CDC'
TBLPROPERTIES (
  'delta.enableDeletionVectors'  = 'true',
  'delta.enableChangeDataFeed'   = 'true',
  'pipelines.autoOptimize.zOrderCols' = 'cliente_id,pedido_ts'
);

CREATE FLOW pedidos_cdc_flow AS AUTO CDC INTO silver.pedidos
FROM STREAM(silver.pedidos_eventos_validos)
KEYS (pedido_id)
SEQUENCE BY evento_ts
COLUMNS * EXCEPT (evento_ts)
STORED AS SCD TYPE 1;

-- ## Produtos — SCD Type 1 com DELETE físico

CREATE OR REFRESH STREAMING TABLE silver.produtos_eventos_validos (
  CONSTRAINT produto_id_obrigatorio EXPECT (produto_id IS NOT NULL) ON VIOLATION FAIL UPDATE
)
COMMENT 'Eventos de produto validados antes do CDC; produto_id ausente derruba a atualização'
AS SELECT
  produto_id,
  nome_produto,
  categoria_id,
  preco_lista,
  ativo,
  operacao,
  try_cast(atualizado_em AS TIMESTAMP) AS atualizado_em
FROM STREAM(bronze.produtos_raw);

CREATE OR REFRESH STREAMING TABLE silver.produtos
COMMENT 'Dimensão de produtos com estado atual (SCD Type 1), mantida via CDC'
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.enableChangeDataFeed'  = 'true'
);

CREATE FLOW produtos_cdc_flow AS AUTO CDC INTO silver.produtos
FROM STREAM(silver.produtos_eventos_validos)
KEYS (produto_id)
APPLY AS DELETE WHEN operacao = 'DELETE'
SEQUENCE BY atualizado_em
COLUMNS * EXCEPT (operacao)
STORED AS SCD TYPE 1;
