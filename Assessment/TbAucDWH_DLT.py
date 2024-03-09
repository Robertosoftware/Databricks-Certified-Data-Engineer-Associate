# Databricks notebook source
# MAGIC %md
# MAGIC * Author: Roberto Bonilla
# MAGIC * Date: 09/03/2024
# MAGIC * Version: v1.0
# MAGIC * Comments: Assessment TBauctions
# MAGIC
# MAGIC # Importing Libraries

# COMMAND ----------

import dlt
from pyspark.sql.functions import lit, regexp_extract, to_timestamp, trim, expr
from pyspark.sql.types import TimestampType

# COMMAND ----------

# MAGIC %md
# MAGIC # Importing Data Quality Checks

# COMMAND ----------

from Libraries.dlt_checks.quality_checks import ( rules_buyers, quarantine_rules_buyers, rules_auctions,rules_bids, quarantine_rules_bids )

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze Layer
# MAGIC
# MAGIC Approach: Bronze tables would be as little modified as possible, column names, data types, filtering and transformations will be done in silver and golden layer.

# COMMAND ----------

# DBTITLE 1,Bronze Auctions
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

# DBTITLE 1,Bronze Bids
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

# DBTITLE 1,Bronze Buyers
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
# MAGIC # Quarantine Layer

# COMMAND ----------

# DBTITLE 1,Quarantine Buyers
@dlt.table(
    name="quarantine_buyers",
    path="/mnt/lifeline-ukraine/dquarantined/buyers",
    comment="Quarantined buyers records due to data quality issues.",
    table_properties={
        "TbAucPipeline.quality": "quarantine",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect_or_drop("valid_quarantine",quarantine_rules_buyers)
def buyers_quarantine():
    df = dlt.read("bronze_buyers")
    return df

# COMMAND ----------

# DBTITLE 1,Quarantine Bids
# Define a function to transform the data to the silver layer
@dlt.table(
    name="quarantine_bids",
    path="/mnt/lifeline-ukraine/dquarantined/bids",
    comment="The transformed data of bids in quarantine.",
    table_properties={
        "TbAucPipeline.quality": "quarantine",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect_or_drop("valid_quarantine",quarantine_rules_bids)
def bids_quarantine():
    df = dlt.read("bronze_bids")
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Layer

# COMMAND ----------

# DBTITLE 1,Silver Buyers
@dlt.table(
    name="silver_buyers",
    path="/mnt/lifeline-ukraine/dsilver/buyers",
    comment="The transformed data of buyers in the silver layer.",
    table_properties={
        "TbAucPipeline.quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect_all_or_drop(rules_buyers)
def buyers_silver():
    # Filter out the non-quarantined records to be stored in the silver layer
    return (
        dlt.read("bronze_buyers")
    )

# COMMAND ----------

# DBTITLE 1,Silver Auctions
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

@dlt.expect_all_or_fail(rules_auctions)
def auctions_silver():
    df= dlt.read('bronze_auctions')
    df = df.select("auctionid", "auction_type").dropDuplicates()
    df = df.withColumn("auction_days", regexp_extract("auction_type", "([0-9]+)",1)) \
            .withColumn("auctionid", df["auctionid"].cast("bigint")) \
            .withColumnRenamed("auctionid","auction_id")
    df = df.withColumn("auction_days", df["auction_days"].cast("int"))
    return df

# COMMAND ----------

# DBTITLE 1,Silver Bids
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
@dlt.expect_all_or_drop(rules_bids)
def bids_silver():
    df = dlt.read('bronze_bids')
    df = df.selectExpr('auctionid as auction_id','bid','bidder', 'openbid as open_bid', 'itemid as item_id','item', 'item_description','price', 'datetime')
    df = df.withColumn("datetime", to_timestamp(df["datetime"], "yyyy-MM-dd HH:mm:ss.SSSSSSSSS")) \
            .withColumn("auction_id", df["auction_id"].cast("bigint")) \
            .withColumn("open_bid", df["open_bid"].cast("double")) \
            .withColumn("bid", df["bid"].cast("double")) \
            .withColumn("price", df["price"].cast("double")) \
            .withColumn("item_description", trim(df["item_description"])) \
            .withColumn("item_id", df["item_id"].cast("bigint"))
    df = df.withColumn("date_bid", df["datetime"].cast("date")) 
    return df

# COMMAND ----------

# DBTITLE 1,Silver Auction_Sellers
# Define a function to transform the data to the silver layer
@dlt.table(
    name="silver_auction_sellers",
    path="/mnt/lifeline-ukraine/dsilver/auction_sellers",
    comment="The transformed data of auction-sellers in the silver layer.",
    table_properties={
        "TbAucPipeline.quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
def auction_sellers_silver():
    df = dlt.read('bronze_auctions')
    df = df.withColumn("seller_id", df["seller_id"].cast("int")) \
            .withColumn("auctionid", df["auctionid"].cast("bigint")) \
            .withColumnRenamed("auctionid","auction_id") \
            .select("seller_id", "auction_id").dropDuplicates()
    return df

# COMMAND ----------

# DBTITLE 1,Silver Sellers
# Define a function to transform the data to the silver layer
@dlt.table(
    name="silver_sellers",
    path="/mnt/lifeline-ukraine/dsilver/sellers",
    comment="The transformed data of sellers in the silver layer.",
    table_properties={
        "TbAucPipeline.quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
def sellers_silver():
    df = dlt.read('bronze_auctions')
    df = df.withColumn("seller_id", df["seller_id"].cast("int")) \
                .select("seller_id", "name","email", "username").dropDuplicates()
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Layer

# COMMAND ----------

# DBTITLE 1,Gold Items
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
    df= dlt.read('silver_bids').select("item_id", "item","item_description").drop_duplicates()
    return df

# COMMAND ----------

# DBTITLE 1,Gold Auctions
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

# DBTITLE 1,Gold Bids
# Define a function to transform the data to the gold layer
@dlt.table(
    name="gold_bids",
    path="/mnt/lifeline-ukraine/dgold/bids",
    comment="The transformed data of bids (joining bids table and auction-sellers) in the gold layer.",
    table_properties={
        "TbAucPipeline.quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def bids_gold():
    df_bids =  dlt.read('silver_bids')
    df_auction_sellers =  dlt.read('silver_auction_sellers')
    df = df_bids.join(df_auction_sellers, 'auction_id', how="inner") \
            .select('bid','auction_id', 'item_id','seller_id','bidder','open_bid','price','datetime','date_bid')
    return df

# COMMAND ----------

# DBTITLE 1,Gold Buyers
# Define a function to transform the data to the gold layer
@dlt.table(
    name="gold_buyers",
    path="/mnt/lifeline-ukraine/dgold/buyers",
    comment="The transformed data of buyers in the gold layer.",
    table_properties={
        "TbAucPipeline.quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def buyers_gold():
    return dlt.read('silver_buyers')

# COMMAND ----------

# DBTITLE 1,Gold Sellers
# Define a function to transform the data to the gold layer
@dlt.table(
    name="gold_sellers",
    path="/mnt/lifeline-ukraine/dgold/sellers",
    comment="The transformed data of sellers in the gold layer.",
    table_properties={
        "TbAucPipeline.quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def sellers_gold():
    return dlt.read('silver_sellers')
