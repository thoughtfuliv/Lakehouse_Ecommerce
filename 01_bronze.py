# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze
# MAGIC Bruto preservado (tudo string) + `_source_file` + `_ingest_ts`. Incremental via checkpoint do Auto Loader.
# MAGIC `multiLine` só em `reviews` (comentários com quebra de linha); nas demais deixa o Spark paralelizar a leitura.

# Configuração (catálogo, caminhos, helpers)
from pyspark.sql import functions as F, Window
from functools import reduce

C = "meu_catalog"  # <- AJUSTE para o seu catálogo (SHOW CATALOGS)
RAW = f"/Volumes/{C}/default/olist_raw"
CHK = f"/Volumes/{C}/default/olist_chk"

# pasta (= nome da tabela) -> arquivo CSV original
ARQUIVOS = {
    "customers":   "olist_customers_dataset.csv",
    "orders":      "olist_orders_dataset.csv",
    "items":       "olist_order_items_dataset.csv",
    "payments":    "olist_order_payments_dataset.csv",
    "reviews":     "olist_order_reviews_dataset.csv",
    "products":    "olist_products_dataset.csv",
    "sellers":     "olist_sellers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "categories":  "product_category_name_translation.csv",
}

def tbl(camada, nome):
    return f"{C}.{camada}.{nome}"

def comentar(tabela, texto):
    texto = texto.replace("'", "''")
    spark.sql(f"COMMENT ON TABLE {tabela} IS '{texto}'")

def bronze(nome):
    return spark.table(tbl("bronze", nome))

# COMMAND ----------
import time

def run_bronze():
    for pasta in ARQUIVOS:
        t0 = time.time()
        print(f"[{pasta}] iniciando...")
        multi = "true" if pasta == "reviews" else "false"
        (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("multiLine", multi)
            .option("quote", '"').option("escape", '"')
            .option("cloudFiles.inferColumnTypes", "false")
            .option("cloudFiles.schemaLocation", f"{CHK}/{pasta}/schema")
            .load(f"{RAW}/{pasta}/")
            .withColumn("_source_file", F.col("_metadata.file_path"))
            .withColumn("_ingest_ts", F.current_timestamp())
            .writeStream
            .option("checkpointLocation", f"{CHK}/{pasta}/checkpoint")
            .trigger(availableNow=True)
            .toTable(tbl("bronze", pasta))
            .awaitTermination())
        comentar(tbl("bronze", pasta),
                 f"Bruto Olist: {pasta}. Tudo string, com origem (_source_file) e data de ingestão (_ingest_ts).")
        print(f"[{pasta}] ok em {time.time() - t0:.0f}s")

run_bronze()

for pasta in ARQUIVOS:
    print(pasta, bronze(pasta).count())
