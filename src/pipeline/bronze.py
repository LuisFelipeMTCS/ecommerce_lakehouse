# Bronze — ingestão via Auto Loader
#
# Três streaming tables, uma por entidade, lendo do volume de landing com Auto Loader
# (`cloudFiles`). As três seguem o mesmo molde, então em vez de copiar o código três vezes
# uma função `criar_tabela_bronze` recebe só o que muda (nome, pasta, formato, schema).
# Cada tabela fixa os tipos com `schemaHints`, guarda o arquivo de origem e o timestamp de
# ingestão, e usa uma expectation WARN para observar (sem descartar) linhas com
# `_rescued_data` preenchido — sinal de drift de schema no arquivo de origem.

from pyspark import pipelines as dp
from pyspark.sql import functions as F

LANDING_PATH = spark.conf.get("landing_path")

ENTIDADES = {
    "clientes_raw": {
        "pasta": "clientes",
        "formato": "json",
        "descricao": "Eventos brutos de clientes ingeridos via Auto Loader (JSON), camada bronze",
        "schema_hints": (
            "cliente_id INT, nome STRING, email STRING, cpf STRING, telefone STRING, "
            "cidade STRING, uf STRING, data_nascimento STRING, operacao STRING, "
            "evento_seq BIGINT, evento_ts STRING"
        ),
        "opcoes": {},
    },
    "pedidos_raw": {
        "pasta": "pedidos",
        "formato": "json",
        "descricao": "Eventos brutos de pedidos ingeridos via Auto Loader (JSON), camada bronze",
        "schema_hints": (
            "pedido_id BIGINT, cliente_id INT, produto_id INT, quantidade INT, "
            "preco_unitario DOUBLE, status STRING, canal STRING, pedido_ts STRING, evento_ts STRING"
        ),
        "opcoes": {},
    },
    "produtos_raw": {
        "pasta": "produtos",
        "formato": "csv",
        "descricao": "Snapshot bruto de produtos ingerido via Auto Loader (CSV), camada bronze",
        "schema_hints": (
            "produto_id INT, nome_produto STRING, categoria_id INT, preco_lista DOUBLE, "
            "ativo BOOLEAN, operacao STRING, atualizado_em STRING"
        ),
        "opcoes": {"header": "true"},
    },
}


def criar_tabela_bronze(nome, cfg):
    @dp.table(
        name=f"bronze.{nome}",
        comment=cfg["descricao"],
        table_properties={
            "pipelines.reset.allowed": "false",
            "delta.autoOptimize.optimizeWrite": "true",
            "delta.autoOptimize.autoCompact": "true",
        },
    )
    @dp.expect_all({"rescued_data_vazio": "_rescued_data IS NULL"})
    def _tabela():
        leitor = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", cfg["formato"])
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaHints", cfg["schema_hints"])
            .option("rescuedDataColumn", "_rescued_data")
        )
        for chave, valor in cfg["opcoes"].items():
            leitor = leitor.option(chave, valor)

        return (
            leitor.load(f"{LANDING_PATH}/{cfg['pasta']}")
            .withColumn("arquivo_origem", F.col("_metadata.file_path"))
            .withColumn("data_ingestao", F.current_timestamp())
        )


for _nome, _cfg in ENTIDADES.items():
    criar_tabela_bronze(_nome, _cfg)
