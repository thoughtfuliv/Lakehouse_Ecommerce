# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup (rodar UMA vez, fora do Job)
# MAGIC Cria schemas/volumes, baixa o Olist do Kaggle direto no RAW e separa uma subpasta por tabela.

# COMMAND ----------
# MAGIC %pip install kagglehub -q

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
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
for camada in ("bronze", "silver", "gold"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {C}.{camada}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {C}.default.olist_raw")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {C}.default.olist_chk")

# COMMAND ----------
import kagglehub, shutil, os
cache_path = kagglehub.dataset_download("olistbr/brazilian-ecommerce")
for nome in os.listdir(cache_path):
    origem = os.path.join(cache_path, nome)
    if os.path.isfile(origem):
        shutil.copy(origem, RAW)

# COMMAND ----------
soltos = {f.name for f in dbutils.fs.ls(RAW)}
for pasta, arquivo in ARQUIVOS.items():
    if arquivo in soltos:
        dbutils.fs.mkdirs(f"{RAW}/{pasta}")
        dbutils.fs.mv(f"{RAW}/{arquivo}", f"{RAW}/{pasta}/{arquivo}")
display(dbutils.fs.ls(RAW))
