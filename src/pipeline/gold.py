# Gold — modelagem de negócio
#
# Enriquecimento (junta pedidos + produtos + a dependência externa `referencia.categorias`),
# materialized views com liquid clustering (`cluster_by`) para as métricas de negócio, a
# dimensão de clientes com máscara de PII, e uma streaming table com watermark para
# demonstrar agregação em tempo quase real.

from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOGO = spark.conf.get("catalogo")


def pedidos_enriquecidos():
    """Pedidos + produto + categoria. Função comum (não é dataset do pipeline): cada
    materialized view refaz o join, igual à view temporária que existia na versão SQL."""
    pedidos = spark.read.table("silver.pedidos").alias("p")
    produtos = spark.read.table("silver.produtos").alias("pr")
    categorias = spark.read.table("referencia.categorias").alias("c")

    return (
        pedidos.join(produtos, F.col("p.produto_id") == F.col("pr.produto_id"), "left")
        .join(categorias, F.col("pr.categoria_id") == F.col("c.categoria_id"), "left")
        .select(
            "p.pedido_id",
            "p.cliente_id",
            "p.produto_id",
            "p.quantidade",
            "p.preco_unitario",
            (F.col("p.quantidade") * F.col("p.preco_unitario")).alias("receita_bruta"),
            "p.status",
            "p.canal",
            "p.pedido_ts",
            "pr.nome_produto",
            "pr.categoria_id",
            "c.nome_categoria",
            "c.margem_alvo",
        )
    )


# ## Vendas diárias por categoria


@dp.materialized_view(
    name="gold.vendas_diarias_categoria",
    comment="Receita e pedidos por dia e categoria, para acompanhamento comercial",
    cluster_by=["data_pedido", "categoria_id"],
)
@dp.expect_or_fail("receita_nao_negativa", "receita_dia >= 0")
def vendas_diarias_categoria():
    return (
        pedidos_enriquecidos()
        .where(F.col("status") != "CANCELADO")
        .groupBy(
            F.col("pedido_ts").cast("date").alias("data_pedido"),
            "categoria_id",
            "nome_categoria",
        )
        .agg(
            F.countDistinct("pedido_id").alias("qtd_pedidos"),
            F.sum("quantidade").alias("qtd_itens"),
            F.sum("receita_bruta").alias("receita_dia"),
            F.avg("margem_alvo").alias("margem_alvo_media"),
        )
    )


# ## Funil de status dos pedidos


@dp.materialized_view(
    name="gold.funil_status_pedidos",
    comment="Contagem de pedidos por status atual, para acompanhar o funil de conversão",
    cluster_by=["status"],
)
def funil_status_pedidos():
    return pedidos_enriquecidos().groupBy("status").agg(
        F.count("*").alias("qtd_pedidos"),
        F.sum("receita_bruta").alias("receita_total"),
    )


# ## Valor de clientes (sem PII)


@dp.materialized_view(
    name="gold.clientes_valor",
    comment="Métricas de valor por cliente, sem nenhuma coluna de PII",
    cluster_by=["cliente_id"],
)
def clientes_valor():
    clientes_vigentes = spark.read.table("silver.clientes").where(F.col("__END_AT").isNull())
    pedidos = pedidos_enriquecidos().alias("ped")

    return (
        clientes_vigentes.alias("cli")
        .join(pedidos, F.col("cli.cliente_id") == F.col("ped.cliente_id"), "left")
        .groupBy("cli.cliente_id", "cli.cidade", "cli.uf")
        .agg(
            F.countDistinct("ped.pedido_id").alias("qtd_pedidos"),
            F.sum("ped.receita_bruta").alias("receita_total"),
            F.max("ped.pedido_ts").alias("ultima_compra"),
        )
    )


# ## Dimensão de clientes mascarada
#
# Somente a versão vigente do SCD Type 2 (`__END_AT IS NULL`). As colunas de PII usam
# `MASK` com as funções de `governanca` (nome totalmente qualificado). O filtro por UF
# continua sendo aplicado por fora, na view `gold.vw_clientes_regional`
# (ver src/operacao/governanca_validacao.sql).


@dp.materialized_view(
    name="gold.dim_clientes",
    comment="Dimensão vigente de clientes, com máscara de PII (row filter aplicado via gold.vw_clientes_regional)",
    cluster_by=["uf"],
    schema=f"""
        cliente_id INT,
        nome STRING,
        email STRING MASK {CATALOGO}.governanca.mascara_email,
        cpf STRING MASK {CATALOGO}.governanca.mascara_cpf,
        telefone STRING MASK {CATALOGO}.governanca.mascara_telefone,
        cidade STRING,
        uf STRING,
        data_nascimento DATE,
        vigente_desde_evento_seq BIGINT
    """,
)
def dim_clientes():
    return (
        spark.read.table("silver.clientes")
        .where(F.col("__END_AT").isNull())
        .select(
            "cliente_id",
            "nome",
            "email",
            "cpf",
            "telefone",
            "cidade",
            "uf",
            "data_nascimento",
            F.col("__START_AT").alias("vigente_desde_evento_seq"),
        )
    )


# ## Pedidos por hora (streaming, com watermark)
#
# Lê de `silver.pedidos_eventos_validos` (streaming table só de append, um evento por
# linha) em vez de `silver.pedidos` (alvo de MERGE via AUTO CDC): streaming não suporta
# ler updates de uma fonte que sofre MERGE (erro `DELTA_SOURCE_TABLE_IGNORE_CHANGES`).
# Como só nos interessa o evento de criação (status = 'CRIADO'), a tabela de eventos
# validados é a fonte correta e mantém a leitura 100% append-only.


@dp.table(
    name="gold.pedidos_por_hora",
    comment="Contagem de pedidos CRIADO por janela de 1 hora e canal, com watermark de 2 horas",
)
def pedidos_por_hora():
    return (
        spark.readStream.table("silver.pedidos_eventos_validos")
        .withWatermark("pedido_ts", "2 hours")
        .where(F.col("status") == "CRIADO")
        .groupBy(F.window("pedido_ts", "1 hour").alias("janela"), "canal")
        .agg(
            F.count("*").alias("qtd_pedidos"),
            F.sum(F.col("quantidade") * F.col("preco_unitario")).alias("receita_estimada"),
        )
    )
