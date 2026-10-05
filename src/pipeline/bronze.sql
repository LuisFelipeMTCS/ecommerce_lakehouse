-- # Bronze — ingestão via Auto Loader
-- MAGIC
-- Três streaming tables, uma por entidade, lendo do volume de landing com
-- `STREAM read_files` (Auto Loader declarativo do Lakeflow). Cada uma tem `schemaHints`
-- para fixar os tipos, guarda o caminho do arquivo de origem e o timestamp de ingestão,
-- e usa uma expectation WARN para observar (sem descartar) linhas com `_rescued_data`
-- preenchido — sinal de drift de schema no arquivo de origem.

CREATE OR REFRESH STREAMING TABLE bronze.clientes_raw (
  CONSTRAINT rescued_data_vazio EXPECT (_rescued_data IS NULL)
)
COMMENT 'Eventos brutos de clientes ingeridos via Auto Loader (JSON), camada bronze'
TBLPROPERTIES (
  'pipelines.reset.allowed' = 'false',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
)
AS SELECT
  *,
  _metadata.file_path   AS arquivo_origem,
  current_timestamp()   AS data_ingestao
FROM STREAM read_files(
  '${landing_path}/clientes',
  format => 'json',
  schemaHints => 'cliente_id INT, nome STRING, email STRING, cpf STRING, telefone STRING, cidade STRING, uf STRING, data_nascimento STRING, operacao STRING, evento_seq BIGINT, evento_ts STRING',
  rescuedDataColumn => '_rescued_data'
);

CREATE OR REFRESH STREAMING TABLE bronze.pedidos_raw (
  CONSTRAINT rescued_data_vazio EXPECT (_rescued_data IS NULL)
)
COMMENT 'Eventos brutos de pedidos ingeridos via Auto Loader (JSON), camada bronze'
TBLPROPERTIES (
  'pipelines.reset.allowed' = 'false',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
)
AS SELECT
  *,
  _metadata.file_path   AS arquivo_origem,
  current_timestamp()   AS data_ingestao
FROM STREAM read_files(
  '${landing_path}/pedidos',
  format => 'json',
  schemaHints => 'pedido_id BIGINT, cliente_id INT, produto_id INT, quantidade INT, preco_unitario DOUBLE, status STRING, canal STRING, pedido_ts STRING, evento_ts STRING',
  rescuedDataColumn => '_rescued_data'
);

CREATE OR REFRESH STREAMING TABLE bronze.produtos_raw (
  CONSTRAINT rescued_data_vazio EXPECT (_rescued_data IS NULL)
)
COMMENT 'Snapshot bruto de produtos ingerido via Auto Loader (CSV), camada bronze'
TBLPROPERTIES (
  'pipelines.reset.allowed' = 'false',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
)
AS SELECT
  *,
  _metadata.file_path   AS arquivo_origem,
  current_timestamp()   AS data_ingestao
FROM STREAM read_files(
  '${landing_path}/produtos',
  format => 'csv',
  header => true,
  schemaHints => 'produto_id INT, nome_produto STRING, categoria_id INT, preco_lista DOUBLE, ativo BOOLEAN, operacao STRING, atualizado_em STRING',
  rescuedDataColumn => '_rescued_data'
);
