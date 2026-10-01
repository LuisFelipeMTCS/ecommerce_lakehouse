# Silver — limpeza, expectations e CDC
#
# Para cada entidade: uma streaming table intermediária de "eventos válidos" (com
# expectations que descartam ou sinalizam linhas ruins) e um flow de AUTO CDC que aplica
# os eventos na tabela final com SCD1 ou SCD2, conforme a regra de negócio.
# Pedidos inválidos vão para uma tabela de quarentena com o motivo da rejeição.
#
# As regras de qualidade de pedidos ficam num único dicionário e são reaproveitadas pela
# expectation (que descarta) e pela quarentena (que guarda), então as duas nunca divergem.

from functools import reduce

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Clientes — SCD Type 2 (histórico completo)
# ---------------------------------------------------------------------------

CLIENTES_DESCARTAR = {
    "id_valido": "cliente_id IS NOT NULL",
    "sequencia_valida": "evento_seq IS NOT NULL",
    "operacao_valida": "operacao IN ('INSERT', 'UPDATE', 'DELETE')",
    "email_valido": "operacao = 'DELETE' OR (email IS NOT NULL AND email LIKE '%@%')",
    "cpf_valido": "operacao = 'DELETE' OR (cpf IS NOT NULL AND length(cpf) = 11)",
}
CLIENTES_AVISAR = {"uf_valida": "uf IS NOT NULL"}


@dp.table(
    name="silver.clientes_eventos_validos",
    comment="Eventos de cliente padronizados e validados antes do CDC",
)
@dp.expect_all_or_drop(CLIENTES_DESCARTAR)
@dp.expect_all(CLIENTES_AVISAR)
def clientes_eventos_validos():
    return spark.readStream.table("bronze.clientes_raw").select(
        "cliente_id",
        F.trim("nome").alias("nome"),
        F.lower(F.trim("email")).alias("email"),
        "cpf",
        "telefone",
        "cidade",
        F.upper("uf").alias("uf"),
        F.expr("try_cast(data_nascimento AS DATE)").alias("data_nascimento"),
        "operacao",
        "evento_seq",
        F.expr("try_cast(evento_ts AS TIMESTAMP)").alias("evento_ts"),
    )


dp.create_streaming_table(
    name="silver.clientes",
    comment="Dimensão de clientes com histórico completo (SCD Type 2), mantida via CDC",
    table_properties={
        "delta.enableDeletionVectors": "true",
        "delta.enableChangeDataFeed": "true",
        "pipelines.autoOptimize.zOrderCols": "cliente_id",
    },
    expect_all={"id_final_valido": "cliente_id IS NOT NULL"},
)

dp.create_auto_cdc_flow(
    name="clientes_cdc_flow",
    target="silver.clientes",
    source="silver.clientes_eventos_validos",
    keys=["cliente_id"],
    sequence_by=F.col("evento_seq"),
    apply_as_deletes=F.expr("operacao = 'DELETE'"),
    except_column_list=["operacao", "evento_seq"],
    stored_as_scd_type="2",
)

# ---------------------------------------------------------------------------
# Pedidos — SCD Type 1 (estado atual) + quarentena
# ---------------------------------------------------------------------------

STATUS_VALIDOS = "('CRIADO', 'PAGO', 'ENVIADO', 'ENTREGUE', 'CANCELADO')"

# nome da regra -> (condição que o pedido PRECISA cumprir, motivo gravado na quarentena)
REGRAS_PEDIDOS = {
    "pedido_id_valido": ("pedido_id IS NOT NULL", "pedido_id_nulo"),
    "cliente_id_valido": ("cliente_id IS NOT NULL", "cliente_id_nulo"),
    "quantidade_valida": ("coalesce(quantidade, 0) > 0", "quantidade_invalida"),
    "preco_valido": ("coalesce(preco_unitario, -1) > 0", "preco_invalido"),
    "status_valido": (f"status IN {STATUS_VALIDOS}", "status_invalido"),
    "data_coerente": (
        "coalesce(pedido_ts, TIMESTAMP '1900-01-01') <= coalesce(evento_ts, TIMESTAMP '9999-12-31')",
        "data_futura",
    ),
}
PEDIDOS_DESCARTAR = {nome: cond for nome, (cond, _) in REGRAS_PEDIDOS.items()}
PEDIDOS_AVISAR = {"canal_valido": "canal IS NOT NULL"}


def pedidos_tipados():
    """Eventos de pedido da bronze com timestamps convertidos (try_cast não derruba a linha)."""
    return spark.readStream.table("bronze.pedidos_raw").select(
        "pedido_id",
        "cliente_id",
        "produto_id",
        "quantidade",
        "preco_unitario",
        "status",
        "canal",
        F.expr("try_cast(pedido_ts AS TIMESTAMP)").alias("pedido_ts"),
        F.expr("try_cast(evento_ts AS TIMESTAMP)").alias("evento_ts"),
    )


def cumpre(condicao):
    # Na expectation, resultado NULL conta como violação; coalesce reproduz isso aqui.
    return F.expr(f"coalesce({condicao}, false)")


@dp.table(
    name="silver.pedidos_eventos_validos",
    comment="Eventos de pedido validados; linhas ruins vão para silver.pedidos_quarentena",
)
@dp.expect_all_or_drop(PEDIDOS_DESCARTAR)
@dp.expect_all(PEDIDOS_AVISAR)
def pedidos_eventos_validos():
    return pedidos_tipados()


@dp.table(
    name="silver.pedidos_quarentena",
    comment="Eventos de pedido que violaram alguma regra de qualidade, com o motivo da rejeição",
)
def pedidos_quarentena():
    motivos = F.filter(
        F.array(*[
            F.when(~cumpre(cond), F.lit(motivo))
            for cond, motivo in REGRAS_PEDIDOS.values()
        ]),
        lambda x: x.isNotNull(),
    )
    todas_cumpridas = reduce(lambda a, b: a & b, [cumpre(c) for c, _ in REGRAS_PEDIDOS.values()])
    return pedidos_tipados().where(~todas_cumpridas).withColumn("regras_violadas", motivos)


dp.create_streaming_table(
    name="silver.pedidos",
    comment="Estado atual dos pedidos (SCD Type 1), mantida via CDC",
    table_properties={
        "delta.enableDeletionVectors": "true",
        "delta.enableChangeDataFeed": "true",
        "pipelines.autoOptimize.zOrderCols": "cliente_id,pedido_ts",
    },
    expect_all={"id_final_valido": "pedido_id IS NOT NULL"},
)

dp.create_auto_cdc_flow(
    name="pedidos_cdc_flow",
    target="silver.pedidos",
    source="silver.pedidos_eventos_validos",
    keys=["pedido_id"],
    sequence_by=F.col("evento_ts"),
    except_column_list=["evento_ts"],
    stored_as_scd_type="1",
)

# ---------------------------------------------------------------------------
# Produtos — SCD Type 1 com DELETE físico
# ---------------------------------------------------------------------------


@dp.table(
    name="silver.produtos_eventos_validos",
    comment="Eventos de produto validados antes do CDC; produto_id ausente derruba a atualização",
)
@dp.expect_all_or_fail({"produto_id_obrigatorio": "produto_id IS NOT NULL"})
def produtos_eventos_validos():
    return spark.readStream.table("bronze.produtos_raw").select(
        "produto_id",
        "nome_produto",
        "categoria_id",
        "preco_lista",
        "ativo",
        "operacao",
        F.expr("try_cast(atualizado_em AS TIMESTAMP)").alias("atualizado_em"),
    )


dp.create_streaming_table(
    name="silver.produtos",
    comment="Dimensão de produtos com estado atual (SCD Type 1), mantida via CDC",
    table_properties={
        "delta.enableDeletionVectors": "true",
        "delta.enableChangeDataFeed": "true",
    },
)

dp.create_auto_cdc_flow(
    name="produtos_cdc_flow",
    target="silver.produtos",
    source="silver.produtos_eventos_validos",
    keys=["produto_id"],
    sequence_by=F.col("atualizado_em"),
    apply_as_deletes=F.expr("operacao = 'DELETE'"),
    except_column_list=["operacao"],
    stored_as_scd_type="1",
)
