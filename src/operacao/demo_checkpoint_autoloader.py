# Databricks notebook source
# MAGIC %md
# MAGIC # 06 - Demonstração de Auto Loader standalone (fora do DLT)
# MAGIC
# MAGIC Mostra o uso "clássico" do Auto Loader via Structured Streaming direto em notebook
# MAGIC Python (fora do Lakeflow), com `cloudFiles.schemaLocation`, `checkpointLocation` no
# MAGIC volume de auditoria, watermark + `dropDuplicatesWithinWatermark` e
# MAGIC `trigger(availableNow=True)` para uma carga incremental "batch-like".

# COMMAND ----------

dbutils.widgets.text("catalogo", "ecommerce_lakehouse")
dbutils.widgets.text("landing_path", "/Volumes/ecommerce_lakehouse/bronze/landing")

catalogo = dbutils.widgets.get("catalogo")
landing_path = dbutils.widgets.get("landing_path")

spark.sql(f"USE CATALOG {catalogo}")

# COMMAND ----------

checkpoint_base = f"/Volumes/{catalogo}/auditoria/checkpoints/demo_autoloader_pedidos"
schema_location = f"{checkpoint_base}/_schema"
checkpoint_location = f"{checkpoint_base}/_checkpoint"

# COMMAND ----------

from pyspark.sql import functions as F

df_raw = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", schema_location)
    .option("cloudFiles.schemaHints",
            "pedido_id BIGINT, cliente_id INT, produto_id INT, quantidade INT, "
            "preco_unitario DOUBLE, status STRING, canal STRING, pedido_ts STRING, evento_ts STRING")
    .load(f"{landing_path}/pedidos")
)

df_tratado = (
    df_raw
    .withColumn("pedido_ts", F.to_timestamp("pedido_ts"))
    .withColumn("evento_ts", F.to_timestamp("evento_ts"))
    .withWatermark("evento_ts", "2 hours")
    .dropDuplicatesWithinWatermark(["pedido_id", "evento_ts"])
)

# COMMAND ----------

query = (
    df_tratado.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_location)
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable(f"{catalogo}.auditoria.demo_autoloader_pedidos")
)

query.awaitTermination()

# COMMAND ----------

display(spark.table(f"{catalogo}.auditoria.demo_autoloader_pedidos").limit(20))

# COMMAND ----------

print("linhas carregadas:", spark.table(f"{catalogo}.auditoria.demo_autoloader_pedidos").count())
