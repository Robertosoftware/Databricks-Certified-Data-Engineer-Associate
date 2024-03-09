# Databricks notebook source
import dlt
from pyspark.sql.functions import lit, regexp_extract, to_timestamp, trim, expr
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze Layer

# COMMAND ----------

# Locations where table would be saved
@dlt.table(
  name="bronze_auctions",
  path="/mnt/lifeline-ukraine/dbronzeem/auc",
  comment="The raw bids ingested from sources container.",
  table_properties={
    "TbAucPipeline.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def auctions_bronze():
  return  spark.read.option("inferSchema", "false").option("header", "true").csv("/mnt/lifeline-ukraine/data/auctions/")

# COMMAND ----------

# Location where table would be saved
@dlt.table(
  name="bronze_bids",
  path="/mnt/lifeline-ukraine/dbronzeem/bids",
  comment="The raw bids ingested from sources container.",
  table_properties={
    "TbAucPipeline.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def bids_bronze():
  return  spark.read.option("inferSchema", "false").option("header", "true").csv("/mnt/lifeline-ukraine/data/bids/")

# COMMAND ----------

# Location where table would be saved
@dlt.table(
    name="bronze_buyers",
    path="/mnt/lifeline-ukraine/dbronzeem/buyers",
    comment="The raw bids ingested from sources container.",
    table_properties={
        "TbAucPipeline.quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
  }
)
def buyers_bronze():
  return  spark.read.option("inferSchema", "false").option("header", "true").csv("/mnt/lifeline-ukraine/data/buyers/")

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Layer

# COMMAND ----------

# Define rules for data quality checks
rules = {
    "valid_id":"id IS NOT NULL",
    "valid_name": "name IS NOT NULL",
    "valid_username": "username IS NOT NULL",
    "valid_email":"email IS NOT NULL AND email LIKE '%@%'"
}
quarantine_rules = "NOT({0})".format(" AND ".join(rules_expect.values()))

# COMMAND ----------

@dlt.table(
    name="buyers_quarantine",
    path="/mnt/lifeline-ukraine/dquarantined/buyers",
    partition_cols=["is_quarantined"],
    comment="Quarantined buyers records due to data quality issues.",
    table_properties={
        "TbAucPipeline.quality": "quarantine",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect_all(rules)
def buyers_quarantine():
    return (
        dlt.read("bronze_buyers")
        .withColumn("is_quarantined",expr(quarantine_rules))
    )

# COMMAND ----------

@dlt.table(
    name="silver_buyers",
    path="/mnt/lifeline-ukraine/dsilver/buyers",
    comment="The transformed data of buyers in the silver layer.",
    table_properties={
        "TbAucPipeline.quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
def buyers_silver():
    # Filter out the non-quarantined records to be stored in the silver layer
    return (
        dlt.read("buyers_quarantine")
        .filter("is_quarantined = false")
    )

# COMMAND ----------

# Define a function to transform the data to the silver layer
@dlt.table(
    name="silver_auctions",
    path="/mnt/lifeline-ukraine/dsilver/auctions",
    comment="The transformed data of auctions in the silver layer.",
    table_properties={
        "TbAucPipeline.quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)

#@dlt.expect_all({"valid_name":"name IS NOT NULL","valid_username":"username IS NOT NULL"})
#    df= dlt.read('bronze_auctions').selectExpr('auctionid as auction_id', 'auction_type')
@dlt.expect_all_or_fail({"valid_auctionid":"auctionid IS NOT NULL"})
def auctions_silver():
    df= dlt.read('bronze_auctions')
    df = df.withColumn("auction_days", regexp_extract("auction_type", "([0-9]+)",1)) \
            .withColumn("auctionid", df["auctionid"].cast("bigint")) \
            .withColumn("seller_id", df["seller_id"].cast("int")) \
            .withColumnRenamed("auctionid","auction_id")
    df = df.withColumn("auction_days", df["auction_days"].cast("int"))
    return df

# COMMAND ----------

# Define a function to transform the data to the silver layer
@dlt.table(
    name="silver_bids",
    path="/mnt/lifeline-ukraine/dsilver/bids",
    comment="The transformed data of bids in the silver layer.",
    table_properties={
        "TbAucPipeline.quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect_all_or_fail({"valid_auctionid":"auctionid IS NOT NULL"})
def bids_silver():
    df = dlt.read('bronze_bids')
    df = df.selectExpr('auctionid as auction_id','bid','bidder', 'openbid as open_bid', 'itemid as item_id','item', 'item_description','price', 'datetime')
    df = df.withColumn("datetime", to_timestamp(df["datetime"], "yyyy-MM-dd HH:mm:ss.SSSSSSSSS")) \
            .withColumn("auction_id", df["auction_id"].cast("bigint")) \
            .withColumn("open_bid", df["open_bid"].cast("double")) \
            .withColumn("price", df["price"].cast("double")) \
            .withColumn("price", trim(df["item_description"])) \
            .withColumn("item_id", df["item_id"].cast("bigint")) 
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Layer

# COMMAND ----------

# Define a function to transform the data to the silver layer
@dlt.table(
    name="gold_items",
    path="/mnt/lifeline-ukraine/dgold/items",
    comment="The transformed data of items in the gold layer.",
    table_properties={
        "TbAucPipeline.quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)

def items_gold():
    df= dlt.read('silver_bids').select("item_id", "item","item_description")
    return df

# COMMAND ----------

# Define a function to transform the data to the gold layer
@dlt.table(
    name="gold_auctions",
    path="/mnt/lifeline-ukraine/dgold/auctions",
    comment="The transformed data of auctions in the gold layer.",
    table_properties={
        "TbAucPipeline.quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def auctions_gold():
    return dlt.read('silver_auctions').select('auction_id','auction_type', 'auction_days')

# COMMAND ----------

# Define a function to transform the data to the gold layer
@dlt.table(
    name="gold_bids",
    path="/mnt/lifeline-ukraine/dgold/bids",
    comment="The transformed data of bids in the gold layer.",
    table_properties={
        "TbAucPipeline.quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def bids_gold():
    return dlt.read('silver_bids').select('auction_id','bid','bidder', 'open_bid', 'item_id','price', 'datetime')

# COMMAND ----------


