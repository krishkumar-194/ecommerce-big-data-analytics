# Databricks notebook source
print("====================================")
print(" E-COMMERCE BIG DATA ANALYTICS")
print("====================================")

print("Apache Spark Version:", spark.version)

print("Spark Application:", spark.sparkContext.appName)

print("Spark is running successfully!")

# COMMAND ----------


# ==========================================
# E-COMMERCE BIG DATA ANALYTICS
# STEP 1: LOAD DATASET USING APACHE SPARK
# ==========================================

data_path = "/Volumes/workspace/default/ecommerce_data/ecommerce_data.csv"

df = (spark.read
      .option("header", True)
      .option("inferSchema", True)
      .csv(data_path))

print("Dataset Loaded Successfully!")
print("Total Records:", df.count())
print("Total Columns:", len(df.columns))

print("\nDataset Schema:")
df.printSchema()

print("\nFirst 10 Records:")
display(df.limit(10))


# COMMAND ----------


from pyspark.sql.functions import (
    col, count, when, sum as spark_sum,
    round as spark_round
)

# STEP 1: Check missing values
print("========== DATA QUALITY CHECK ==========")

missing_values = df.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in df.columns
])

display(missing_values)

# STEP 2: Remove duplicate orders
df_clean = df.dropDuplicates(["order_id"])

# STEP 3: Remove rows with missing essential values
df_clean = df_clean.dropna(
    subset=[
        "order_id", "order_date", "category",
        "city", "quantity", "unit_price"
    ]
)

# STEP 4: Calculate revenue
df_clean = df_clean.withColumn(
    "revenue",
    col("quantity") * col("unit_price")
)

# STEP 5: Display results
print("========== CLEANED DATA ==========")
print("Original Records:", df.count())
print("Cleaned Records:", df_clean.count())
print("Total Columns:", len(df_clean.columns))

display(df_clean.limit(10))


# COMMAND ----------


from pyspark.sql.functions import (
    sum, avg, count, max, min, round
)

# REVENUE ANALYSIS

print("========== REVENUE ANALYSIS ==========")

revenue_summary = df_clean.agg(
    count("order_id").alias("Total Orders"),

    round(sum("revenue"), 2).alias("Total Revenue"),

    round(avg("revenue"), 2).alias("Average Order Value"),

    max("revenue").alias("Highest Order Value"),

    min("revenue").alias("Lowest Order Value")
)

display(revenue_summary)


# COMMAND ----------


from pyspark.sql.functions import sum as spark_sum, count, round as spark_round

category_sales = (
    df_clean
    .groupBy("category")
    .agg(
        count("order_id").alias("Total Orders"),
        spark_round(spark_sum("revenue"), 2).alias("Total Revenue")
    )
    .orderBy("Total Revenue", ascending=False)
)

print("========== CATEGORY WISE SALES ==========")

display(category_sales)


# COMMAND ----------


from pyspark.sql.functions import sum as spark_sum, count, round as spark_round

city_sales = (
    df_clean
    .groupBy("city")
    .agg(
        count("order_id").alias("Total Orders"),
        spark_round(spark_sum("revenue"), 2).alias("Total Revenue")
    )
    .orderBy("Total Revenue", ascending=False)
)

print("========== CITY WISE SALES ==========")

display(city_sales)


# COMMAND ----------


from pyspark.sql.functions import (
    month, sum as spark_sum,
    count, round as spark_round
)

monthly_sales = (
    df_clean
    .groupBy(month("order_date").alias("Month"))
    .agg(
        count("order_id").alias("Total Orders"),
        spark_round(spark_sum("revenue"), 2).alias("Total Revenue")
    )
    .orderBy("Month")
)

print("========== MONTHLY SALES ANALYSIS ==========")

display(monthly_sales)


# COMMAND ----------


status_analysis = (
    df_clean
    .groupBy("order_status")
    .agg(
        count("order_id").alias("Total Orders"),
        spark_round(
            spark_sum("revenue"), 2
        ).alias("Total Revenue")
    )
    .orderBy("Total Orders", ascending=False)
)

print("========== ORDER STATUS ANALYSIS ==========")

display(status_analysis)


# COMMAND ----------


from pyspark.sql.functions import (
    sum as spark_sum,
    count,
    round as spark_round
)

top_products = (
    df_clean
    .groupBy("product")
    .agg(
        count("order_id").alias("Total Orders"),
        spark_round(spark_sum("revenue"), 2).alias("Total Revenue")
    )
    .orderBy("Total Revenue", ascending=False)
    .limit(10)
)

print("========== TOP 10 PRODUCTS ==========")
display(top_products)


# COMMAND ----------


# ======================================
# SPARK SQL ANALYSIS
# ======================================

df_clean.createOrReplaceTempView("ecommerce_orders")

print("========== SPARK SQL ANALYSIS ==========")

sql_result = spark.sql("""
    SELECT
        category,
        COUNT(order_id) AS total_orders,
        ROUND(SUM(revenue), 2) AS total_revenue
    FROM ecommerce_orders
    GROUP BY category
    ORDER BY total_revenue DESC
""")

display(sql_result)


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     city,
# MAGIC     COUNT(*) AS total_orders,
# MAGIC     ROUND(AVG(revenue), 2) AS average_revenue
# MAGIC FROM ecommerce_orders
# MAGIC GROUP BY city
# MAGIC ORDER BY average_revenue DESC;
# MAGIC

# COMMAND ----------


import plotly.express as px

# 1. Category Revenue Chart
category_pd = category_sales.toPandas()

fig1 = px.bar(
    category_pd,
    x="category",
    y="Total Revenue",
    title="Category Wise Revenue Analysis",
    color="category"
)
fig1.show()

# 2. City Revenue Chart
city_pd = city_sales.toPandas()

fig2 = px.bar(
    city_pd,
    x="city",
    y="Total Revenue",
    title="City Wise Revenue Analysis",
    color="city"
)
fig2.show()

# 3. Monthly Revenue Trend
monthly_pd = monthly_sales.toPandas()

fig3 = px.line(
    monthly_pd,
    x="Month",
    y="Total Revenue",
    markers=True,
    title="Monthly Revenue Trend"
)
fig3.show()

print("All charts generated successfully!")


# COMMAND ----------


# CATEGORY WISE REVENUE CHART
display(category_sales)


# COMMAND ----------

display(city_sales)

# COMMAND ----------

display(monthly_sales)