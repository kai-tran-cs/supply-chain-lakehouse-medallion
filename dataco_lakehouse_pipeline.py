# Databricks notebook source
# MAGIC %md
# MAGIC # DataCo Lakehouse Pipeline — Bronze → Silver → Gold (Star Schema + SCD2)
# MAGIC
# MAGIC Pipeline end-to-end cho dataset **DataCo Smart Supply Chain** (180.519 dòng).
# MAGIC Chạy trên **Databricks Free Edition** (serverless). Mỗi cell là một bước —
# MAGIC chạy lần lượt từ trên xuống.
# MAGIC
# MAGIC **Trước khi chạy:**
# MAGIC 1. Upload `DataCoSupplyChainDataset.csv` vào một Volume (Catalog Explorer → tạo Volume → Upload).
# MAGIC 2. Sửa biến `INPUT_PATH` và `CATALOG` ở cell Config bên dưới cho khớp môi trường của bạn.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config — sửa 2 biến này cho khớp môi trường của bạn

# COMMAND ----------

# Catalog mặc định trên Free Edition thường là "workspace". Nếu khác, đổi tại đây.
CATALOG = "workspace"

# Đường dẫn file CSV bạn đã upload vào Volume.
# Ví dụ: /Volumes/workspace/default/raw/DataCoSupplyChainDataset.csv
INPUT_PATH = "/Volumes/workspace/default/raw/DataCoSupplyChainDataset.csv"

# Tên 3 schema (database) của kiến trúc medallion
BRONZE = f"{CATALOG}.bronze"
SILVER = f"{CATALOG}.silver"
GOLD   = f"{CATALOG}.gold"

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import re

# Tạo 3 schema nếu chưa có
for schema in [BRONZE, SILVER, GOLD]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

print("Config OK. Catalog =", CATALOG)

# COMMAND ----------

# MAGIC %md
# MAGIC ## PHASE 1 — BRONZE: đổ raw, không sửa giá trị
# MAGIC Chỉ đọc CSV (encoding latin-1), làm sạch *tên cột* để ghi được vào Delta,
# MAGIC thêm metadata ingest. Giá trị giữ nguyên.

# COMMAND ----------

# Đọc CSV — encoding ISO-8859-1 (= latin-1) để không lỗi ký tự đặc biệt
raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("encoding", "ISO-8859-1")
    .csv(INPUT_PATH)
)

# Hàm chuẩn hoá tên cột: "Days for shipping (real)" -> "days_for_shipping_real"
def clean_name(c):
    c = c.strip().lower()
    c = re.sub(r"[^0-9a-z]+", "_", c)
    return c.strip("_")

raw = raw.toDF(*[clean_name(c) for c in raw.columns])

# Thêm metadata ingest (đặc trưng của lớp Bronze)
bronze_df = (
    raw
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.lit(INPUT_PATH))
)

bronze_df.write.mode("overwrite").saveAsTable(f"{BRONZE}.dataco_orders")
print("Bronze rows:", spark.table(f"{BRONZE}.dataco_orders").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## PHASE 2 — Lập hồ sơ chất lượng (data profiling)
# MAGIC Xác nhận các cột rác để loại ở Silver. Tự nhìn để hiểu *vì sao* bỏ.

# COMMAND ----------

b = spark.table(f"{BRONZE}.dataco_orders")

# Đếm số giá trị distinct mỗi cột nghi ngờ là rác
junk_check = ["customer_email", "customer_password", "product_description",
              "product_status", "product_image"]
b.select([F.countDistinct(c).alias(c) for c in junk_check]).show()
# Kỳ vọng: email/password = 1, product_description = 0 (toàn null), product_status = 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## PHASE 3 — SILVER: làm sạch, ép kiểu, parse ngày
# MAGIC Bỏ cột rác, đổi 2 cột ngày sang timestamp, khử trùng lặp.

# COMMAND ----------

DROP_COLS = ["customer_email", "customer_password", "product_description",
             "product_status", "product_image", "_ingested_at", "_source_file"]

silver_df = (
    b.drop(*DROP_COLS)
    # parse ngày: định dạng gốc là M/d/yyyy H:mm
    .withColumn("order_ts",
                F.to_timestamp("order_date_dateorders", "M/d/yyyy H:mm"))
    .withColumn("ship_ts",
                F.to_timestamp("shipping_date_dateorders", "M/d/yyyy H:mm"))
    .withColumn("order_date", F.to_date("order_ts"))
    .withColumn("ship_date", F.to_date("ship_ts"))
    .drop("order_date_dateorders", "shipping_date_dateorders")
    # khử trùng lặp theo grain = 1 dòng / order_item_id
    .dropDuplicates(["order_item_id"])
)

silver_df.write.mode("overwrite").saveAsTable(f"{SILVER}.dataco_clean")
print("Silver rows:", spark.table(f"{SILVER}.dataco_clean").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## PHASE 5 — GOLD: xây các Dimension
# MAGIC Mỗi dim = lấy giá trị distinct + gán surrogate key (1..N) bằng row_number.
# MAGIC (Phase 4 là thiết kế trên giấy — xem sơ đồ star schema, không có code.)

# COMMAND ----------

s = spark.table(f"{SILVER}.dataco_clean")

# ---------- dim_product ----------
dim_product = (
    s.select("product_card_id", "product_name",
             "category_id", "category_name",
             "department_id", "department_name", "product_price")
    .dropDuplicates(["product_card_id"])
)
w = Window.orderBy("product_card_id")
dim_product = dim_product.withColumn("product_key", F.row_number().over(w))
dim_product.write.mode("overwrite").saveAsTable(f"{GOLD}.dim_product")

# ---------- dim_geo (địa điểm đích của đơn) ----------
dim_geo = (
    s.select("market", "order_region", "order_country", "order_state", "order_city")
    .dropDuplicates()
)
w = Window.orderBy("market", "order_region", "order_country", "order_state", "order_city")
dim_geo = dim_geo.withColumn("geo_key", F.row_number().over(w))
dim_geo.write.mode("overwrite").saveAsTable(f"{GOLD}.dim_geo")

# ---------- dim_shipping_mode ----------
dim_ship = s.select("shipping_mode").dropDuplicates()
w = Window.orderBy("shipping_mode")
dim_ship = dim_ship.withColumn("shipping_mode_key", F.row_number().over(w))
dim_ship.write.mode("overwrite").saveAsTable(f"{GOLD}.dim_shipping_mode")

# ---------- dim_date (sinh dải ngày từ min order tới max ship) ----------
bounds = s.select(
    F.min("order_date").alias("d_min"),
    F.greatest(F.max("order_date"), F.max("ship_date")).alias("d_max")
).first()

dim_date = (
    spark.sql(f"""
        SELECT explode(sequence(
            to_date('{bounds.d_min}'),
            to_date('{bounds.d_max}'),
            interval 1 day)) AS full_date
    """)
    .withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("int"))
    .withColumn("year", F.year("full_date"))
    .withColumn("quarter", F.quarter("full_date"))
    .withColumn("month", F.month("full_date"))
    .withColumn("day_of_week", F.date_format("full_date", "EEEE"))
    .withColumn("is_weekend",
                F.when(F.dayofweek("full_date").isin(1, 7), True).otherwise(False))
)
dim_date.write.mode("overwrite").saveAsTable(f"{GOLD}.dim_date")

print("Dimensions xong: product / geo / shipping_mode / date")

# COMMAND ----------

# MAGIC %md
# MAGIC ## PHASE 6 — SCD Type 2 cho dim_customer  ⭐
# MAGIC Dataset là ảnh tĩnh nên ta **mô phỏng** một thay đổi lịch sử:
# MAGIC một số khách (id chia hết 20, không phải Consumer) trước đây là 'Consumer',
# MAGIC rồi "nâng hạng" lên segment hiện tại kể từ 2017-06-01.
# MAGIC Mỗi khách đổi segment sẽ có **2 dòng**: bản cũ (is_current=false) + bản mới.

# COMMAND ----------

CUTOFF = "2017-06-01"   # mốc thời gian xảy ra thay đổi (mô phỏng)
FAR    = "9999-12-31"   # "vô cực" cho bản hiện hành

# Lấy thuộc tính khách theo đơn GẦN NHẤT (1 dòng / customer_id)
wlatest = Window.partitionBy("customer_id").orderBy(F.col("order_ts").desc())
cust_base = (
    s.withColumn("rn", F.row_number().over(wlatest))
    .filter("rn = 1")
    .select("customer_id", "customer_fname", "customer_lname",
            "customer_segment", "customer_city", "customer_state",
            "customer_country", "customer_zipcode")
)

# Đánh dấu khách "có thay đổi" (mô phỏng): id % 20 == 0 và segment hiện tại != Consumer
cust_base = cust_base.withColumn(
    "is_changed",
    ((F.col("customer_id") % 20 == 0) & (F.col("customer_segment") != "Consumer"))
)

# Bản HIỆN HÀNH (current) cho tất cả khách
current_rows = (
    cust_base
    .withColumn("start_date",
                F.when(F.col("is_changed"), F.to_date(F.lit(CUTOFF)))
                 .otherwise(F.to_date(F.lit("2000-01-01"))))
    .withColumn("end_date", F.to_date(F.lit(FAR)))
    .withColumn("is_current", F.lit(True))
)

# Bản LỊCH SỬ (cũ) chỉ cho khách có thay đổi — segment cũ = 'Consumer'
history_rows = (
    cust_base.filter("is_changed")
    .withColumn("customer_segment", F.lit("Consumer"))   # giá trị cũ
    .withColumn("start_date", F.to_date(F.lit("2000-01-01")))
    .withColumn("end_date", F.date_sub(F.to_date(F.lit(CUTOFF)), 1))
    .withColumn("is_current", F.lit(False))
)

# Gộp 2 phần → gán surrogate key cho TỪNG dòng version
dim_customer = current_rows.unionByName(history_rows).drop("is_changed")
w = Window.orderBy("customer_id", "start_date")
dim_customer = dim_customer.withColumn("customer_key", F.row_number().over(w))

dim_customer.write.mode("overwrite").saveAsTable(f"{GOLD}.dim_customer")

# Kiểm chứng: vài khách có 2 dòng lịch sử
print("Số dòng dim_customer:", spark.table(f"{GOLD}.dim_customer").count())
spark.table(f"{GOLD}.dim_customer").filter("is_current = false") \
    .select("customer_id", "customer_segment", "start_date", "end_date").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## PHASE 7 — GOLD: xây Fact + lớp KPI
# MAGIC Join Silver với các dim để thay khóa tự nhiên bằng surrogate key.
# MAGIC Khóa customer dùng **temporal join** (khớp order_date trong [start_date, end_date])
# MAGIC để lấy đúng version SCD2 tại thời điểm đặt hàng.

# COMMAND ----------

dim_customer = spark.table(f"{GOLD}.dim_customer")
dim_product  = spark.table(f"{GOLD}.dim_product")
dim_geo      = spark.table(f"{GOLD}.dim_geo")
dim_ship     = spark.table(f"{GOLD}.dim_shipping_mode")
dim_date     = spark.table(f"{GOLD}.dim_date")

fact = (
    s.alias("s")
    # customer_key: temporal join theo SCD2
    .join(dim_customer.alias("c"),
          (F.col("s.customer_id") == F.col("c.customer_id")) &
          (F.col("s.order_date") >= F.col("c.start_date")) &
          (F.col("s.order_date") <= F.col("c.end_date")),
          "left")
    # product_key
    .join(dim_product.alias("p"),
          F.col("s.product_card_id") == F.col("p.product_card_id"), "left")
    # geo_key
    .join(dim_geo.alias("g"),
          [F.col("s.market") == F.col("g.market"),
           F.col("s.order_region") == F.col("g.order_region"),
           F.col("s.order_country") == F.col("g.order_country"),
           F.col("s.order_state") == F.col("g.order_state"),
           F.col("s.order_city") == F.col("g.order_city")], "left")
    # shipping_mode_key
    .join(dim_ship.alias("sm"),
          F.col("s.shipping_mode") == F.col("sm.shipping_mode"), "left")
    # date keys (role-playing dimension dùng 2 lần)
    .join(dim_date.alias("od"), F.col("s.order_date") == F.col("od.full_date"), "left")
    .join(dim_date.alias("sd"), F.col("s.ship_date") == F.col("sd.full_date"), "left")
    .select(
        # khóa nghiệp vụ + degenerate dimension
        F.col("s.order_item_id"),
        F.col("s.order_id"),
        # foreign keys
        F.col("c.customer_key"),
        F.col("p.product_key"),
        F.col("g.geo_key"),
        F.col("sm.shipping_mode_key"),
        F.col("od.date_key").alias("order_date_key"),
        F.col("sd.date_key").alias("ship_date_key"),
        # measures
        F.col("s.sales"),
        F.col("s.order_item_quantity").alias("quantity"),
        F.col("s.order_item_discount"),
        F.col("s.order_item_discount_rate"),
        F.col("s.order_item_profit_ratio"),
        F.col("s.order_profit_per_order").alias("profit_per_order"),
        F.col("s.benefit_per_order"),
        F.col("s.order_item_total"),
        F.col("s.days_for_shipping_real").alias("days_ship_real"),
        F.col("s.days_for_shipment_scheduled").alias("days_ship_scheduled"),
        # degenerate dimensions (giữ trong fact)
        F.col("s.late_delivery_risk"),
        F.col("s.delivery_status"),
        F.col("s.order_status"),
    )
)

fact.write.mode("overwrite").saveAsTable(f"{GOLD}.fact_order_line")
print("Fact rows:", spark.table(f"{GOLD}.fact_order_line").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lớp KPI — bảng tổng hợp sẵn cho Power BI
# MAGIC Định nghĩa rõ: on-time rate = 1 − tỉ lệ late_delivery_risk.

# COMMAND ----------

f = spark.table(f"{GOLD}.fact_order_line")

# KPI giao hàng theo Market
kpi_delivery = (
    f.join(dim_geo, "geo_key")
    .groupBy("market")
    .agg(
        F.count("*").alias("total_orders"),
        F.round(1 - F.avg("late_delivery_risk"), 3).alias("on_time_rate"),
        F.round(F.avg(F.col("days_ship_real") - F.col("days_ship_scheduled")), 2)
         .alias("avg_delay_days"),
    )
    .orderBy(F.desc("total_orders"))
)
kpi_delivery.write.mode("overwrite").saveAsTable(f"{GOLD}.kpi_delivery_by_market")

# KPI doanh thu/lợi nhuận theo segment khách (dùng version SCD2 hiện hành)
kpi_segment = (
    f.join(dim_customer.filter("is_current = true"), "customer_key")
    .groupBy("customer_segment")
    .agg(
        F.round(F.sum("sales"), 2).alias("total_sales"),
        F.round(F.sum("profit_per_order"), 2).alias("total_profit"),
        F.round(F.avg("order_item_profit_ratio"), 3).alias("avg_profit_ratio"),
    )
    .orderBy(F.desc("total_sales"))
)
kpi_segment.write.mode("overwrite").saveAsTable(f"{GOLD}.kpi_sales_by_segment")

kpi_delivery.show()
kpi_segment.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## PHASE 8 — Export cho Power BI
# MAGIC Cách đơn giản nhất với Free Edition: ghi các bảng Gold ra CSV trong một Volume,
# MAGIC rồi tải về nạp vào Power BI Desktop. (Hoặc dùng connector Databricks nếu có.)

# COMMAND ----------

EXPORT_DIR = f"/Volumes/{CATALOG}/default/exports"  # tạo Volume 'exports' trước nếu chưa có
dbutils.fs.mkdirs(EXPORT_DIR)

gold_tables = ["fact_order_line", "dim_customer", "dim_product",
               "dim_geo", "dim_shipping_mode", "dim_date",
               "kpi_delivery_by_market", "kpi_sales_by_segment"]

for t in gold_tables:
    (spark.table(f"{GOLD}.{t}")
        .coalesce(1)                      # gộp về 1 file cho dễ tải
        .write.mode("overwrite").option("header", "true")
        .csv(f"{EXPORT_DIR}/{t}"))

print("Đã export Gold ra:", EXPORT_DIR)
print("Tải file CSV trong từng thư mục con về máy → import vào Power BI Desktop.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Kiểm tra cuối — star schema đã sẵn sàng
# MAGIC Query thử để chắc chắn join hoạt động.

# COMMAND ----------

spark.sql(f"""
    SELECT g.market,
           COUNT(*) AS orders,
           ROUND(1 - AVG(f.late_delivery_risk), 3) AS on_time_rate,
           ROUND(SUM(f.sales), 0) AS total_sales
    FROM {GOLD}.fact_order_line f
    JOIN {GOLD}.dim_geo g ON f.geo_key = g.geo_key
    GROUP BY g.market
    ORDER BY total_sales DESC
""").show()
