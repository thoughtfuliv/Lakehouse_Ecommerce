# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold (incremental)
# MAGIC `fact_vendas` (grão: **1 linha por item de pedido**) atualizada por **MERGE** só com o que mudou na Silver
# MAGIC (watermark de `_ingest_ts` em `gold.controle`). KPIs são recalculados a partir da fato, e só quando ela mudou.
# MAGIC Receita = soma de `price` (sem frete), excluindo pedidos `canceled` e `unavailable`.
# MAGIC Pagamentos ficam fora da fato para não multiplicar linhas (um pedido pode ter vários pagamentos).

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
import time
t0 = time.time()

F_ = tbl("gold", "fact_vendas")
CTRL = tbl("gold", "controle")
US = "unix_micros({}._ingest_ts)"

spark.sql(f"""CREATE TABLE IF NOT EXISTS {CTRL} (tabela STRING, wm_micros BIGINT)
COMMENT 'Watermark (_ingest_ts da Silver em microssegundos) já processado pela Gold.'""")

def fato_sql(filtro):
    return f"""
SELECT
  i.order_id, i.order_item_id, i.product_id, i.seller_id, o.customer_id, c.customer_unique_id,
  o.order_status,
  o.order_purchase_timestamp AS compra_ts,
  CAST(o.order_purchase_timestamp AS DATE) AS data_compra,
  o.order_delivered_customer_date AS entrega_ts,
  o.order_estimated_delivery_date AS entrega_estimada_ts,
  i.price, i.freight_value, i.price + i.freight_value AS valor_total_item,
  COALESCE(cat.product_category_name_english, p.product_category_name, 'desconhecida') AS categoria,
  c.customer_state AS uf_cliente, s.seller_state AS uf_vendedor,
  DATEDIFF(o.order_delivered_customer_date, o.order_purchase_timestamp) AS dias_entrega,
  CASE WHEN o.order_delivered_customer_date IS NULL THEN NULL
       WHEN o.order_delivered_customer_date > o.order_estimated_delivery_date THEN 1 ELSE 0 END AS entregue_com_atraso,
  r.nota_media
FROM {tbl('silver','items')} i
JOIN {tbl('silver','orders')} o ON o.order_id = i.order_id
LEFT JOIN {tbl('silver','customers')} c ON c.customer_id = o.customer_id
LEFT JOIN {tbl('silver','sellers')} s ON s.seller_id = i.seller_id
LEFT JOIN {tbl('silver','products')} p ON p.product_id = i.product_id
LEFT JOIN {tbl('silver','categories')} cat ON cat.product_category_name = p.product_category_name
LEFT JOIN (SELECT order_id, AVG(review_score) AS nota_media
           FROM {tbl('silver','reviews')} GROUP BY order_id) r ON r.order_id = i.order_id
WHERE {filtro}"""

# COMMAND ----------
# 1) fato: cria vazia na 1ª vez e faz MERGE só do que é novo/alterado (itens ou pedidos)
spark.sql(f"""CREATE TABLE IF NOT EXISTS {F_}
COMMENT 'Fato de vendas. Grão: 1 linha por item de pedido (order_id + order_item_id).'
AS {fato_sql("1 = 0")}""")

r = spark.sql(f"SELECT wm_micros FROM {CTRL} WHERE tabela = 'fact_vendas'").first()
wm = r[0] if r else 0

def max_us(nome):
    return spark.table(tbl("silver", nome)).agg(F.max(F.expr("unix_micros(_ingest_ts)"))).first()[0]

candidatos = [x for x in (max_us("items"), max_us("orders")) if x is not None]
novo_wm = max(candidatos) if candidatos else None
houve_novo = novo_wm is not None and novo_wm > wm

if houve_novo:
    filtro = (f"({US.format('i')} > {wm} OR {US.format('o')} > {wm}) "
              f"AND {US.format('i')} <= {novo_wm} AND {US.format('o')} <= {novo_wm}")
    spark.sql(f"""MERGE INTO {F_} t USING ({fato_sql(filtro)}) s
    ON t.order_id = s.order_id AND t.order_item_id = s.order_item_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *""")
    spark.sql(f"""MERGE INTO {CTRL} t USING (SELECT 'fact_vendas' AS tabela, {novo_wm}L AS wm_micros) s
    ON t.tabela = s.tabela WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""")
    print(f"[fact_vendas] ok em {time.time() - t0:.0f}s")
else:
    print("[fact_vendas] sem dados novos na Silver")

# COMMAND ----------
# 2) KPIs: só recalcula se a fato mudou (ou se ainda não existem)
if houve_novo or not spark.catalog.tableExists(tbl("gold", "kpi_receita_mensal")):
    VALIDOS = "order_status NOT IN ('canceled','unavailable')"

    spark.sql(f"""
    CREATE OR REPLACE TABLE {tbl('gold','kpi_receita_mensal')}
    COMMENT 'Receita mensal, pedidos e ticket médio (por pedido). Exclui canceled/unavailable.'
    AS SELECT CAST(date_trunc('MONTH', data_compra) AS DATE) AS mes,
              ROUND(SUM(price), 2) AS receita,
              COUNT(DISTINCT order_id) AS pedidos,
              ROUND(SUM(price) / COUNT(DISTINCT order_id), 2) AS ticket_medio
       FROM {F_} WHERE {VALIDOS} GROUP BY 1 ORDER BY 1""")

    spark.sql(f"""
    CREATE OR REPLACE TABLE {tbl('gold','kpi_prazo_entrega')}
    COMMENT 'Prazo médio de entrega (dias) e % de atraso por UF do cliente. Só pedidos entregues.'
    AS SELECT uf_cliente,
              ROUND(AVG(dias_entrega), 1) AS dias_entrega_medio,
              ROUND(100 * AVG(entregue_com_atraso), 1) AS pct_atraso,
              COUNT(DISTINCT order_id) AS pedidos_entregues
       FROM {F_} WHERE entrega_ts IS NOT NULL GROUP BY uf_cliente ORDER BY dias_entrega_medio""")

    spark.sql(f"""
    CREATE OR REPLACE TABLE {tbl('gold','kpi_top_categorias')}
    COMMENT 'Top 10 categorias por receita. Exclui canceled/unavailable.'
    AS SELECT categoria, ROUND(SUM(price), 2) AS receita, COUNT(DISTINCT order_id) AS pedidos
       FROM {F_} WHERE {VALIDOS} GROUP BY categoria ORDER BY receita DESC LIMIT 10""")
    print(f"[kpis] ok em {time.time() - t0:.0f}s (total)")
else:
    print("[kpis] fato sem mudanças, mantidos")

# COMMAND ----------
# Checagem do grão: deve retornar 0 linhas
display(spark.sql(f"""SELECT order_id, order_item_id, COUNT(*) n FROM {F_}
                      GROUP BY 1, 2 HAVING COUNT(*) > 1"""))
display(spark.table(tbl("gold", "kpi_receita_mensal")))