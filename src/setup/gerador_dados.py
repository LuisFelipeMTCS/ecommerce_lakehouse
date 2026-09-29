# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Gerador de dados sintéticos
# MAGIC
# MAGIC Gera eventos CDC de clientes, eventos de pedidos e um snapshot de produtos, gravando-os
# MAGIC como arquivos no volume de landing (`bronze.landing`), para serem lidos pelo Auto Loader
# MAGIC no pipeline DLT. Cada lote é determinístico (semente fixa derivada do número do lote) e os
# MAGIC atributos de pedidos existentes são recalculados a partir do `pedido_id` (RNG semeado pelo
# MAGIC próprio id), então mudanças de status em lotes posteriores não exigem estado externo.

# COMMAND ----------

dbutils.widgets.text("landing_path", "/Volumes/ecommerce_lakehouse/bronze/landing")
dbutils.widgets.text("lote", "1")

landing_path = dbutils.widgets.get("landing_path")
lote = int(dbutils.widgets.get("lote"))

print(f"landing_path={landing_path} lote={lote}")

# COMMAND ----------

import json
import os
import random
from datetime import datetime, timedelta, timezone

TOTAL_CLIENTES_LOTE1 = 500
TOTAL_PRODUTOS_LOTE1 = 40
TOTAL_PEDIDOS_LOTE1 = 3000

CLIENTES_UPDATE_LOTE_N = 60
CLIENTES_DELETE_LOTE_N = 10
PEDIDOS_STATUS_CHANGE_LOTE_N = 800
PEDIDOS_NOVOS_LOTE_N = 1000
PRODUTOS_REAJUSTE_LOTE_N = 5
PRODUTOS_DELETE_LOTE_N = 1

PCT_PEDIDOS_INVALIDOS = 0.04
PCT_CLIENTES_INVALIDOS = 0.03

NOMES = ["Ana", "Bruno", "Carla", "Diego", "Elisa", "Fábio", "Gabriela", "Heitor", "Isabela", "João",
         "Karina", "Lucas", "Mariana", "Nicolas", "Olívia", "Pedro", "Quésia", "Rafael", "Sofia", "Thiago"]
SOBRENOMES = ["Silva", "Souza", "Oliveira", "Santos", "Pereira", "Costa", "Rodrigues", "Almeida",
              "Nascimento", "Lima", "Araújo", "Fernandes", "Carvalho", "Gomes", "Ribeiro"]
CIDADES_UF = [
    ("São Paulo", "SP"), ("Campinas", "SP"), ("Rio de Janeiro", "RJ"), ("Niterói", "RJ"),
    ("Belo Horizonte", "MG"), ("Uberlândia", "MG"), ("Goiânia", "GO"), ("Brasília", "DF"),
    ("Curitiba", "PR"), ("Porto Alegre", "RS"), ("Salvador", "BA"), ("Recife", "PE"),
    ("Fortaleza", "CE"), ("Manaus", "AM"), ("Belém", "PA"),
]
CANAIS = ["site", "app", "marketplace", "loja_fisica"]
STATUS_VALIDOS = ["CRIADO", "PAGO", "ENVIADO", "ENTREGUE", "CANCELADO"]

AGORA = datetime.now(timezone.utc)


def evento_ts_str(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def novo_cpf(rng: random.Random) -> str:
    return "".join(str(rng.randint(0, 9)) for _ in range(11))


def novo_telefone(rng: random.Random) -> str:
    return "11" + "".join(str(rng.randint(0, 9)) for _ in range(9))


def gerar_cliente_base(cliente_id: int):
    rng = random.Random(1_000_000 + cliente_id)
    nome = f"{rng.choice(NOMES)} {rng.choice(SOBRENOMES)}"
    email = f"{nome.lower().replace(' ', '.')}{cliente_id}@example.com"
    cpf = novo_cpf(rng)
    telefone = novo_telefone(rng)
    cidade, uf = rng.choice(CIDADES_UF)
    ano = rng.randint(1955, 2005)
    mes = rng.randint(1, 12)
    dia = rng.randint(1, 28)
    data_nascimento = f"{ano:04d}-{mes:02d}-{dia:02d}"
    return {
        "cliente_id": cliente_id, "nome": nome, "email": email, "cpf": cpf,
        "telefone": telefone, "cidade": cidade, "uf": uf, "data_nascimento": data_nascimento,
    }


def gerar_pedido_base(pedido_id: int):
    rng = random.Random(2_000_000 + pedido_id)
    cliente_id = rng.randint(1, TOTAL_CLIENTES_LOTE1)
    produto_id = rng.randint(1, TOTAL_PRODUTOS_LOTE1)
    quantidade = rng.randint(1, 5)
    preco_unitario = round(rng.uniform(15, 800), 2)
    canal = rng.choice(CANAIS)
    dias_atras = rng.randint(0, 400)
    pedido_ts = AGORA - timedelta(days=dias_atras, minutes=rng.randint(0, 1440))
    return {
        "pedido_id": pedido_id, "cliente_id": cliente_id, "produto_id": produto_id,
        "quantidade": quantidade, "preco_unitario": preco_unitario, "canal": canal,
        "pedido_ts": pedido_ts,
    }


def escrever_json_lines(caminho: str, registros: list):
    os.makedirs(os.path.dirname(caminho), exist_ok=True)
    with open(caminho, "w", encoding="utf-8") as f:
        for r in registros:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def escrever_csv(caminho: str, registros: list, colunas: list):
    os.makedirs(os.path.dirname(caminho), exist_ok=True)
    with open(caminho, "w", encoding="utf-8") as f:
        f.write(",".join(colunas) + "\n")
        for r in registros:
            f.write(",".join(str(r[c]) for c in colunas) + "\n")


# COMMAND ----------

# MAGIC %md ## Geração de clientes (eventos CDC)

# COMMAND ----------

rng_global = random.Random(10_000 + lote)
eventos_clientes = []
qtd_clientes_invalidos = 0

if lote == 1:
    for cliente_id in range(1, TOTAL_CLIENTES_LOTE1 + 1):
        c = gerar_cliente_base(cliente_id)
        invalido = rng_global.random() < PCT_CLIENTES_INVALIDOS
        if invalido:
            qtd_clientes_invalidos += 1
            if rng_global.random() < 0.5:
                c["email"] = c["email"].replace("@", "")
            else:
                c["cpf"] = c["cpf"][:5]
        eventos_clientes.append({
            **c, "operacao": "INSERT", "evento_seq": cliente_id,
            "evento_ts": evento_ts_str(AGORA - timedelta(days=400 - (cliente_id % 400))),
        })
else:
    ids_update = rng_global.sample(range(1, TOTAL_CLIENTES_LOTE1 + 1), CLIENTES_UPDATE_LOTE_N)
    seq_base = 100_000 + lote * 1_000
    for i, cliente_id in enumerate(ids_update):
        c = gerar_cliente_base(cliente_id)
        rng = random.Random(3_000_000 + lote * 10_000 + cliente_id)
        c["email"] = f"novo.{rng.randint(1000,9999)}.{c['email']}"
        c["telefone"] = novo_telefone(rng)
        invalido = rng_global.random() < PCT_CLIENTES_INVALIDOS
        if invalido:
            qtd_clientes_invalidos += 1
            c["email"] = c["email"].replace("@", "")
        eventos_clientes.append({
            **c, "operacao": "UPDATE", "evento_seq": seq_base + i,
            "evento_ts": evento_ts_str(AGORA),
        })

    ids_restantes = [i for i in range(1, TOTAL_CLIENTES_LOTE1 + 1) if i not in ids_update]
    ids_delete = rng_global.sample(ids_restantes, CLIENTES_DELETE_LOTE_N)
    for i, cliente_id in enumerate(ids_delete):
        c = gerar_cliente_base(cliente_id)
        eventos_clientes.append({
            **c, "operacao": "DELETE", "evento_seq": seq_base + CLIENTES_UPDATE_LOTE_N + i,
            "evento_ts": evento_ts_str(AGORA),
        })

    # Update fora de ordem: evento_seq mais antigo que o INSERT original (deve ser ignorado pelo CDC)
    cliente_fora_ordem = ids_update[0]
    c = gerar_cliente_base(cliente_fora_ordem)
    c["email"] = "evento.atrasado.ignorar@example.com"
    eventos_clientes.append({
        **c, "operacao": "UPDATE", "evento_seq": 1,
        "evento_ts": evento_ts_str(AGORA - timedelta(days=1000)),
    })

nome_arquivo_clientes = f"clientes_lote{lote}.json"
escrever_json_lines(f"{landing_path}/clientes/{nome_arquivo_clientes}", eventos_clientes)
print(f"clientes: {len(eventos_clientes)} eventos ({qtd_clientes_invalidos} inválidos)")

# COMMAND ----------

# MAGIC %md ## Geração de produtos (snapshot CSV)

# COMMAND ----------

eventos_produtos = []

if lote == 1:
    for produto_id in range(1, TOTAL_PRODUTOS_LOTE1 + 1):
        rng = random.Random(4_000_000 + produto_id)
        categoria_id = rng.randint(1, 8)
        preco_lista = round(rng.uniform(10, 1200), 2)
        eventos_produtos.append({
            "produto_id": produto_id, "nome_produto": f"Produto {produto_id:03d}",
            "categoria_id": categoria_id, "preco_lista": preco_lista, "ativo": True,
            "operacao": "INSERT", "atualizado_em": evento_ts_str(AGORA - timedelta(days=400)),
        })
else:
    rng = random.Random(5_000_000 + lote)
    ids_reajuste = rng.sample(range(1, TOTAL_PRODUTOS_LOTE1 + 1), PRODUTOS_REAJUSTE_LOTE_N)
    for produto_id in ids_reajuste:
        rng_p = random.Random(4_000_000 + produto_id)
        categoria_id = rng_p.randint(1, 8)
        preco_lista = round(rng_p.uniform(10, 1200) * 1.1, 2)
        eventos_produtos.append({
            "produto_id": produto_id, "nome_produto": f"Produto {produto_id:03d}",
            "categoria_id": categoria_id, "preco_lista": preco_lista, "ativo": True,
            "operacao": "UPDATE", "atualizado_em": evento_ts_str(AGORA),
        })
    restantes = [i for i in range(1, TOTAL_PRODUTOS_LOTE1 + 1) if i not in ids_reajuste]
    produto_delete = rng.choice(restantes)
    rng_p = random.Random(4_000_000 + produto_delete)
    eventos_produtos.append({
        "produto_id": produto_delete, "nome_produto": f"Produto {produto_delete:03d}",
        "categoria_id": rng_p.randint(1, 8), "preco_lista": round(rng_p.uniform(10, 1200), 2),
        "ativo": False, "operacao": "DELETE", "atualizado_em": evento_ts_str(AGORA),
    })

colunas_produtos = ["produto_id", "nome_produto", "categoria_id", "preco_lista", "ativo", "operacao", "atualizado_em"]
nome_arquivo_produtos = f"produtos_lote{lote}.csv"
escrever_csv(f"{landing_path}/produtos/{nome_arquivo_produtos}", eventos_produtos, colunas_produtos)
print(f"produtos: {len(eventos_produtos)} eventos")

# COMMAND ----------

# MAGIC %md ## Geração de pedidos (eventos CDC)

# COMMAND ----------

eventos_pedidos = []
qtd_pedidos_invalidos = 0
rng_pedidos = random.Random(20_000 + lote)

if lote == 1:
    for pedido_id in range(1, TOTAL_PEDIDOS_LOTE1 + 1):
        p = gerar_pedido_base(pedido_id)
        invalido = rng_pedidos.random() < PCT_PEDIDOS_INVALIDOS
        pedido_id_evento = p["pedido_id"]
        cliente_id_evento = p["cliente_id"]
        quantidade_evento = p["quantidade"]
        preco_evento = p["preco_unitario"]
        status_evento = "CRIADO"
        pedido_ts_evento = p["pedido_ts"]
        if invalido:
            qtd_pedidos_invalidos += 1
            tipo = rng_pedidos.choice(["pedido_id_nulo", "cliente_id_nulo", "quantidade_invalida",
                                        "preco_invalido", "status_invalido", "data_futura"])
            if tipo == "pedido_id_nulo":
                pedido_id_evento = None
            elif tipo == "cliente_id_nulo":
                cliente_id_evento = None
            elif tipo == "quantidade_invalida":
                quantidade_evento = -rng_pedidos.randint(1, 5)
            elif tipo == "preco_invalido":
                preco_evento = -abs(preco_evento)
            elif tipo == "status_invalido":
                status_evento = "PERDIDO"
            elif tipo == "data_futura":
                pedido_ts_evento = AGORA + timedelta(days=rng_pedidos.randint(1, 30))
        eventos_pedidos.append({
            "pedido_id": pedido_id_evento, "cliente_id": cliente_id_evento,
            "produto_id": p["produto_id"], "quantidade": quantidade_evento,
            "preco_unitario": preco_evento, "status": status_evento, "canal": p["canal"],
            "pedido_ts": evento_ts_str(pedido_ts_evento),
            "evento_ts": evento_ts_str(pedido_ts_evento),
        })
else:
    ids_existentes = rng_pedidos.sample(range(1, TOTAL_PEDIDOS_LOTE1 + 1), PEDIDOS_STATUS_CHANGE_LOTE_N)
    novos_status = ["PAGO", "ENVIADO", "ENTREGUE", "CANCELADO"]
    for pedido_id in ids_existentes:
        p = gerar_pedido_base(pedido_id)
        novo_status = rng_pedidos.choice(novos_status)
        eventos_pedidos.append({
            "pedido_id": p["pedido_id"], "cliente_id": p["cliente_id"], "produto_id": p["produto_id"],
            "quantidade": p["quantidade"], "preco_unitario": p["preco_unitario"], "status": novo_status,
            "canal": p["canal"], "pedido_ts": evento_ts_str(p["pedido_ts"]),
            "evento_ts": evento_ts_str(AGORA),
        })

    proximo_id_base = TOTAL_PEDIDOS_LOTE1 + (lote - 2) * PEDIDOS_NOVOS_LOTE_N
    for offset in range(1, PEDIDOS_NOVOS_LOTE_N + 1):
        pedido_id = proximo_id_base + offset
        p = gerar_pedido_base(pedido_id)
        invalido = rng_pedidos.random() < PCT_PEDIDOS_INVALIDOS
        pedido_id_evento = p["pedido_id"]
        cliente_id_evento = p["cliente_id"]
        quantidade_evento = p["quantidade"]
        preco_evento = p["preco_unitario"]
        status_evento = "CRIADO"
        pedido_ts_evento = AGORA
        if invalido:
            qtd_pedidos_invalidos += 1
            tipo = rng_pedidos.choice(["pedido_id_nulo", "cliente_id_nulo", "quantidade_invalida",
                                        "preco_invalido", "status_invalido", "data_futura"])
            if tipo == "pedido_id_nulo":
                pedido_id_evento = None
            elif tipo == "cliente_id_nulo":
                cliente_id_evento = None
            elif tipo == "quantidade_invalida":
                quantidade_evento = -rng_pedidos.randint(1, 5)
            elif tipo == "preco_invalido":
                preco_evento = -abs(preco_evento)
            elif tipo == "status_invalido":
                status_evento = "PERDIDO"
            elif tipo == "data_futura":
                pedido_ts_evento = AGORA + timedelta(days=rng_pedidos.randint(1, 30))
        eventos_pedidos.append({
            "pedido_id": pedido_id_evento, "cliente_id": cliente_id_evento,
            "produto_id": p["produto_id"], "quantidade": quantidade_evento,
            "preco_unitario": preco_evento, "status": status_evento, "canal": p["canal"],
            "pedido_ts": evento_ts_str(pedido_ts_evento),
            "evento_ts": evento_ts_str(pedido_ts_evento),
        })

nome_arquivo_pedidos = f"pedidos_lote{lote}.json"
escrever_json_lines(f"{landing_path}/pedidos/{nome_arquivo_pedidos}", eventos_pedidos)
print(f"pedidos: {len(eventos_pedidos)} eventos ({qtd_pedidos_invalidos} inválidos)")

# COMMAND ----------

print("=== RESUMO DO LOTE", lote, "===")
print(f"clientes: {len(eventos_clientes)} (inválidos: {qtd_clientes_invalidos})")
print(f"produtos: {len(eventos_produtos)}")
print(f"pedidos: {len(eventos_pedidos)} (inválidos: {qtd_pedidos_invalidos})")
