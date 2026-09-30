# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver (incremental)
# MAGIC Processa em **lote** só o que é novo na Bronze (watermark de `_ingest_ts` na tabela `silver.controle`), tipa, valida, deduplica e faz **MERGE**. Sem streaming, sem checkpoint.
# MAGIC Violações vão para `silver.quarentena`. Reexecutar sem dados novos não altera nada.

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
# (Opcional) Rode SÓ para recomeçar do zero. Não há mais checkpoints da Silver (só a tabela de controle).
# for n in ARQUIVOS:
#     spark.sql(f"DROP TABLE IF EXISTS {tbl('silver', n)}")
# for t in ("quarentena", "metricas_qualidade", "controle"):
#     spark.sql(f"DROP TABLE IF EXISTS {tbl('silver', t)}")

# COMMAND ----------
import time
META = ["_source_file", "_ingest_ts"]
QUAR = tbl("silver", "quarentena")
CTRL = tbl("silver", "controle")

spark.sql(f"""CREATE TABLE IF NOT EXISTS {CTRL} (tabela STRING, wm_micros BIGINT)
COMMENT 'Watermark (_ingest_ts em microssegundos) já processado por tabela da Silver.'""")

def tc(c, t): return F.expr(f"try_cast(`{c}` as {t})").alias(c)
def tts(c):   return F.expr(f"try_to_timestamp(`{c}`)").alias(c)
def nn(c):    return F.col(c).isNotNull() & (F.trim(F.col(c)) != "")

def get_wm(nome):
    r = spark.sql(f"SELECT wm_micros FROM {CTRL} WHERE tabela = '{nome}'").first()
    return r[0] if r else 0

def set_wm(nome, wm):
    spark.sql(f"""MERGE INTO {CTRL} t USING (SELECT '{nome}' AS tabela, {wm}L AS wm_micros) s
                  ON t.tabela = s.tabela
                  WHEN MATCHED THEN UPDATE SET *
                  WHEN NOT MATCHED THEN INSERT *""")

def silver_incremental(nome, tipar, regras, chaves, desc, prep=None, dedup_simples=False):
    t0 = time.time()
    destino = tbl("silver", nome)

    # 1) só linhas novas da Bronze (acima do watermark)
    wm = get_wm(nome)
    b = bronze(nome).withColumn("_us", F.expr("unix_micros(_ingest_ts)"))
    novo_wm = b.agg(F.max("_us")).first()[0]
    if novo_wm is None or novo_wm <= wm:
        print(f"[{nome}] sem dados novos")
        return
    lote = b.filter((F.col("_us") > wm) & (F.col("_us") <= novo_wm)).drop("_us")

    df = tipar(lote)
    if prep:
        df = prep(df)

    # 2) regras -> lista de violações por linha
    falhas = F.filter(
        F.array(*[F.when(~F.coalesce(cond, F.lit(False)), F.lit(r)) for r, cond in regras.items()]),
        lambda x: x.isNotNull())
    d = df.withColumn("_falhas", falhas)

    # 3) quarentena
    (d.filter(F.size("_falhas") > 0)
      .select(F.lit(nome).alias("tabela"),
              F.col("_falhas").alias("regras_violadas"),
              F.to_json(F.struct(*df.columns)).alias("registro"),
              F.current_timestamp().alias("_quarentena_ts"))
      .write.mode("append").saveAsTable(QUAR))

    # 4) válidos -> dedup -> MERGE
    ok = d.filter(F.size("_falhas") == 0).drop("_falhas")
    ok = ok.drop(*[c for c in ok.columns if c.startswith("_") and c not in META])
    if dedup_simples:
        ok = ok.dropDuplicates(chaves)   # mais barato (tabelas grandes sem chave natural)
    else:
        w = Window.partitionBy(*chaves).orderBy(F.col("_ingest_ts").desc())
        ok = ok.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

    if not spark.catalog.tableExists(destino):
        ok.write.saveAsTable(destino)
    else:
        ok.createOrReplaceTempView(f"_novo_{nome}")
        cond = " AND ".join(f"t.`{k}` <=> s.`{k}`" for k in chaves)
        spark.sql(f"""MERGE INTO {destino} t USING _novo_{nome} s ON {cond}
                      WHEN MATCHED AND s._ingest_ts > t._ingest_ts THEN UPDATE SET *
                      WHEN NOT MATCHED THEN INSERT *""")

    set_wm(nome, novo_wm)
    comentar(destino, desc)
    print(f"[{nome}] ok em {time.time() - t0:.0f}s")

# COMMAND ----------
silver_incremental("categories",
    tipar=lambda b: b.select("product_category_name", "product_category_name_english", *META),
    regras={"chave_nao_nula": nn("product_category_name")},
    chaves=["product_category_name"],
    desc="Tradução de categorias (pt->en). Chave: product_category_name.")

silver_incremental("customers",
    tipar=lambda b: b.select(
        "customer_id", "customer_unique_id", "customer_zip_code_prefix",
        F.lower(F.trim("customer_city")).alias("customer_city"),
        F.upper(F.trim("customer_state")).alias("customer_state"), *META),
    regras={"customer_id_nao_nulo": nn("customer_id"),
            "uf_com_2_letras": F.length("customer_state") == 2},
    chaves=["customer_id"],
    desc="Clientes tipados e padronizados. Chave: customer_id.")

silver_incremental("sellers",
    tipar=lambda b: b.select(
        "seller_id", "seller_zip_code_prefix",
        F.lower(F.trim("seller_city")).alias("seller_city"),
        F.upper(F.trim("seller_state")).alias("seller_state"), *META),
    regras={"seller_id_nao_nulo": nn("seller_id"),
            "uf_com_2_letras": F.length("seller_state") == 2},
    chaves=["seller_id"],
    desc="Vendedores tipados e padronizados. Chave: seller_id.")

silver_incremental("products",
    tipar=lambda b: b.select(
        "product_id", F.trim("product_category_name").alias("product_category_name"),
        tc("product_name_lenght", "int"), tc("product_description_lenght", "int"),
        tc("product_photos_qty", "int"), tc("product_weight_g", "double"),
        tc("product_length_cm", "double"), tc("product_height_cm", "double"),
        tc("product_width_cm", "double"), *META),
    regras={"product_id_nao_nulo": nn("product_id"),
            "peso_nao_negativo": F.col("product_weight_g").isNull() | (F.col("product_weight_g") >= 0)},
    chaves=["product_id"],
    desc="Produtos tipados. Chave: product_id.")

silver_incremental("geolocation",
    tipar=lambda b: b.select(
        "geolocation_zip_code_prefix", tc("geolocation_lat", "double"), tc("geolocation_lng", "double"),
        F.lower(F.trim("geolocation_city")).alias("geolocation_city"),
        F.upper(F.trim("geolocation_state")).alias("geolocation_state"), *META),
    regras={"coordenadas_dentro_do_brasil":
                F.col("geolocation_lat").between(-34, 6) & F.col("geolocation_lng").between(-74, -34),
            "uf_com_2_letras": F.length("geolocation_state") == 2},
    chaves=["geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng",
            "geolocation_city", "geolocation_state"],
    desc="Geolocalização por CEP (prefixo), sem duplicatas exatas.",
    dedup_simples=True)

STATUS = ["created", "approved", "invoiced", "processing", "shipped", "delivered", "unavailable", "canceled"]
silver_incremental("orders",
    tipar=lambda b: b.select(
        "order_id", "customer_id", "order_status",
        tts("order_purchase_timestamp"), tts("order_approved_at"), tts("order_delivered_carrier_date"),
        tts("order_delivered_customer_date"), tts("order_estimated_delivery_date"), *META),
    regras={"order_id_nao_nulo": nn("order_id"),
            "customer_id_nao_nulo": nn("customer_id"),
            "status_conhecido": F.col("order_status").isin(STATUS),
            "compra_entre_2016_e_2018": F.col("order_purchase_timestamp").between("2016-01-01", "2018-12-31 23:59:59"),
            "entrega_nao_anterior_a_compra": F.col("order_delivered_customer_date").isNull()
                | (F.col("order_delivered_customer_date") >= F.col("order_purchase_timestamp"))},
    chaves=["order_id"],
    desc="Pedidos tipados e validados. Chave: order_id (única).")

def prep_items(df):
    # integridade: pedido e produto precisam existir na Silver (orders e products já foram processadas acima)
    o = spark.table(tbl("silver", "orders")).select("order_id").distinct().withColumn("_ped_ok", F.lit(True))
    p = spark.table(tbl("silver", "products")).select("product_id").distinct().withColumn("_prod_ok", F.lit(True))
    return df.join(o, "order_id", "left").join(p, "product_id", "left")

silver_incremental("items",
    tipar=lambda b: b.select(
        "order_id", tc("order_item_id", "int"), "product_id", "seller_id",
        tts("shipping_limit_date"), tc("price", "double"), tc("freight_value", "double"), *META),
    prep=prep_items,
    regras={"pedido_existe": F.col("_ped_ok"),
            "produto_existe": F.col("_prod_ok"),
            "price_maior_ou_igual_zero": F.col("price") >= 0,
            "freight_maior_ou_igual_zero": F.col("freight_value") >= 0},
    chaves=["order_id", "order_item_id"],
    desc="Itens de pedido validados (pedido e produto existentes). Chave: order_id + order_item_id.")

silver_incremental("payments",
    tipar=lambda b: b.select(
        "order_id", tc("payment_sequential", "int"), "payment_type",
        tc("payment_installments", "int"), tc("payment_value", "double"), *META),
    regras={"order_id_nao_nulo": nn("order_id"),
            "valor_nao_negativo": F.col("payment_value") >= 0},
    chaves=["order_id", "payment_sequential"],
    desc="Pagamentos tipados. Chave: order_id + payment_sequential.")

silver_incremental("reviews",
    tipar=lambda b: b.select(
        "review_id", "order_id", tc("review_score", "int"), "review_comment_title", "review_comment_message",
        tts("review_creation_date"), tts("review_answer_timestamp"), *META),
    regras={"review_id_nao_nulo": nn("review_id"),
            "nota_entre_1_e_5": F.col("review_score").between(1, 5)},
    chaves=["review_id"],
    desc="Avaliações tipadas, sem duplicatas por review_id.")

# COMMAND ----------
comentar(QUAR, "Registros que violaram regras de qualidade na Silver. registro = linha original em JSON.")

spark.sql(f"""
CREATE OR REPLACE TABLE {tbl('silver','metricas_qualidade')}
COMMENT 'Contagem de violações por tabela e regra (a partir da quarentena).'
AS SELECT tabela, regra, COUNT(*) AS violacoes
   FROM (SELECT tabela, explode(regras_violadas) AS regra FROM {QUAR})
   GROUP BY tabela, regra""")
display(spark.table(tbl("silver", "metricas_qualidade")).orderBy("tabela", "regra"))

# COMMAND ----------
# Conciliação: bronze = silver + quarentena + duplicatas removidas
rec = " UNION ALL ".join(
    f"""SELECT '{t}' AS tabela,
        (SELECT COUNT(*) FROM {tbl('bronze', t)}) AS bronze,
        (SELECT COUNT(*) FROM {tbl('silver', t)}) AS silver,
        (SELECT COUNT(*) FROM {QUAR} WHERE tabela = '{t}') AS quarentena""" for t in ARQUIVOS)
display(spark.sql(f"SELECT *, bronze - silver - quarentena AS duplicatas_removidas FROM ({rec})"))