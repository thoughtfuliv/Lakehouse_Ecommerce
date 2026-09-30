# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Configuração compartilhada
# MAGIC Referência: este código já está embutido na primeira célula de cada notebook (não é preciso `%run`).

# COMMAND ----------
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
