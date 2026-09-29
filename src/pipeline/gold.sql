-- # Gold — modelagem de negócio
-- MAGIC
-- View temporária de enriquecimento (junta pedidos + produtos + a dependência externa
-- `referencia.categorias`), materialized views com liquid clustering (`CLUSTER BY`) para
-- as métricas de negócio, a dimensão de clientes com máscara/row filter, e uma streaming
-- table com watermark para demonstrar agregação em tempo quase real.

CREATE TEMPORARY VIEW pedidos_enriquecidos AS
SELECT
  p.pedido_id,
  p.cliente_id,
  p.produto_id,
  p.quantidade,
  p.preco_unitario,
  p.quantidade * p.preco_unitario AS receita_bruta,
  p.status,
  p.canal,
  p.pedido_ts,
  pr.nome_produto,
  pr.categoria_id,
  c.nome_categoria,
  c.margem_alvo
FROM silver.pedidos AS p
LEFT JOIN silver.produtos AS pr ON p.produto_id = pr.produto_id
LEFT JOIN referencia.categorias AS c ON pr.categoria_id = c.categoria_id;

-- ## Vendas diárias por categoria

CREATE MATERIALIZED VIEW gold.vendas_diarias_categoria (
  CONSTRAINT receita_nao_negativa EXPECT (receita_dia >= 0) ON VIOLATION FAIL UPDATE
)
COMMENT 'Receita e pedidos por dia e categoria, para acompanhamento comercial'
CLUSTER BY (data_pedido, categoria_id)
AS SELECT
  cast(pedido_ts AS DATE)      AS data_pedido,
  categoria_id,
  nome_categoria,
  count(DISTINCT pedido_id)    AS qtd_pedidos,
  sum(quantidade)              AS qtd_itens,
  sum(receita_bruta)           AS receita_dia,
  avg(margem_alvo)             AS margem_alvo_media
FROM pedidos_enriquecidos
WHERE status != 'CANCELADO'
GROUP BY ALL;

-- ## Funil de status dos pedidos

CREATE MATERIALIZED VIEW gold.funil_status_pedidos
COMMENT 'Contagem de pedidos por status atual, para acompanhar o funil de conversão'
CLUSTER BY (status)
AS SELECT
  status,
  count(*)              AS qtd_pedidos,
  sum(receita_bruta)     AS receita_total
FROM pedidos_enriquecidos
GROUP BY ALL;

-- ## Valor de clientes (sem PII)

CREATE MATERIALIZED VIEW gold.clientes_valor
COMMENT 'Métricas de valor por cliente, sem nenhuma coluna de PII'
CLUSTER BY (cliente_id)
AS SELECT
  cli.cliente_id,
  cli.cidade,
  cli.uf,
  count(DISTINCT ped.pedido_id)                                 AS qtd_pedidos,
  sum(ped.receita_bruta)                                        AS receita_total,
  max(ped.pedido_ts)                                            AS ultima_compra
FROM silver.clientes AS cli
LEFT JOIN pedidos_enriquecidos AS ped ON cli.cliente_id = ped.cliente_id
WHERE cli.__END_AT IS NULL
GROUP BY ALL;

-- ## Dimensão de clientes mascarada por UF
-- MAGIC
-- Somente a versão vigente do SCD Type 2 (`__END_AT IS NULL`). As colunas de PII usam
-- `MASK` com as funções de `governanca`. A cláusula `ROW FILTER` não é aceita pelo motor
-- do Lakeflow em MATERIALIZED VIEW nesta versão (erro de sintaxe testado em produção:
-- `PARSE_SYNTAX_ERROR ... near 'FILTER'`), então o filtro por UF é aplicado por fora,
-- na view dinâmica `gold.vw_clientes_regional` (ver src/operacao/governanca_validacao.sql).

CREATE MATERIALIZED VIEW gold.dim_clientes (
  cliente_id      BIGINT,
  nome            STRING,
  email           STRING MASK governanca.mascara_email,
  cpf             STRING MASK governanca.mascara_cpf,
  telefone        STRING MASK governanca.mascara_telefone,
  cidade          STRING,
  uf              STRING,
  data_nascimento DATE,
  vigente_desde   TIMESTAMP
)
COMMENT 'Dimensão vigente de clientes, com máscara de PII (row filter aplicado via gold.vw_clientes_regional)'
CLUSTER BY (uf)
AS SELECT
  cliente_id,
  nome,
  email,
  cpf,
  telefone,
  cidade,
  uf,
  data_nascimento,
  __START_AT AS vigente_desde
FROM silver.clientes
WHERE __END_AT IS NULL;

-- ## Pedidos por hora (streaming, com watermark)

CREATE OR REFRESH STREAMING TABLE gold.pedidos_por_hora
COMMENT 'Contagem de pedidos CRIADO por janela de 1 hora e canal, com watermark de 2 horas'
AS SELECT
  window(pedido_ts, '1 hour') AS janela,
  canal,
  count(*)                    AS qtd_pedidos,
  sum(quantidade * preco_unitario) AS receita_estimada
FROM STREAM(silver.pedidos)
WHERE status = 'CRIADO'
WATERMARK pedido_ts DELAY OF INTERVAL 2 HOURS
GROUP BY window(pedido_ts, '1 hour'), canal;
