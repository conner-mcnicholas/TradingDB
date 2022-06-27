# Databricks notebook source
# MAGIC %md
# MAGIC # Step Four: Analytical ETL
# MAGIC 
# MAGIC Use SparkSQL and Python to build an ETL job that calculates the following results for a given day:<br>
# MAGIC &emsp;&emsp;-Latest trade price as of each quote. <br>
# MAGIC &emsp;&emsp;-Latest 3 hour moving average trade price, before the quote. <br>
# MAGIC &emsp;&emsp;-The bid/ask price movement from the previous day’s closing price.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.1 Read Parquet Files From Azure Blob Storage Partition

# COMMAND ----------

df =spark.read.parquet("/trade/trade_dt={}".format("2020-08-06"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2 Create Trade Staging Table
# MAGIC ### 4.2.1 Use Spark To Read The Trade Table With Date Partition “2020-07-29”

# COMMAND ----------

df = df.select("stock_symbol","stock_exchange","latest_trade", "event_seq_nb", "trade_pr").where(df.trade_dt == "2020-08-06")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2.2 Create A Spark Temporary View

# COMMAND ----------

df.createOrReplaceTempView("tmp_trade_moving_avg")

# COMMAND ----------

spark.sql("SELECT stock_symbol,stock_exchange,latest_trade,event_seq_nb,trade_pr FROM tmp_trade_moving_avg").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2.3 Calculate The 180*-min Moving Average Using The Spark Temp View 
# MAGIC Partition by symbol and order by time. The window should contain all records within 30-min of the corresponding row.<br>
# MAGIC *Increased from 30mins b/c the given data only has trades every ~ 90 mins, so 30min mov avg is identical to trade_pr.

# COMMAND ----------

mov_avg_df = spark.sql("SELECT stock_symbol,stock_exchange, latest_trade, event_seq_nb, trade_pr, \
    AVG(trade_pr) OVER (PARTITION BY stock_symbol ORDER BY latest_trade RANGE BETWEEN INTERVAL '180' MINUTES PRECEDING AND CURRENT ROW) AS mov_avg_pr \
    FROM tmp_trade_moving_avg")

# COMMAND ----------

mov_avg_df.show(3,truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2.4 Save The Temporary View Into Hive Table For Staging

# COMMAND ----------

mov_avg_df.write.mode('overwrite').saveAsTable("temp_trade_moving_avg")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.3 Create Staging Table For The Prior Day’s Last Trade
# MAGIC You’ll need another staging table in order to make the last trade price available to the quote
# MAGIC records of corresponding symbols. Note that this table has only 1 price per trade/exchange, so
# MAGIC the record size will be small enough.
# MAGIC 
# MAGIC ### 4.3.1 Get The Previous Date Value

# COMMAND ----------

import datetime
date = datetime.datetime.strptime('2020-08-06', '%Y-%m-%d')
prev_date_str = (date - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
print('Assuming current date of 2020-08-06, prev_date_str = ' + prev_date_str)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3.2 Use Spark To Read The Trade Table With Date Partition “2020-07-28”

# COMMAND ----------

df =spark.read.parquet("/trade/trade_dt={}".format(prev_date_str))
df = df.select("stock_symbol","stock_exchange","latest_trade", "event_seq_nb", "trade_pr").where(df.trade_dt == prev_date_str)
df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3.3 Create Spark Temporary View

# COMMAND ----------

df.createOrReplaceTempView("tmp_last_trade")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3.4 Calculate Last Trade Price Using The Spark Temp View

# COMMAND ----------

""" 
THIS QUERY TEMPLATE IN THE INSTRUCTIONS IS NOT CLEAR - WHY DID I JUST MAKE TMP_LAST_TRADE IF I'M RE-USING TMP_TRADE_MOVING_AVG FROM BEFORE? 

last_pr_df = spark.sql("select symbol, exchange, last_pr from (select
symbol, exchange, event_tm, event_seq_nb, trade_pr,
# [logic to derive last 30 min moving average price] AS last_pr
FROM tmp_trade_moving_avg) a
")

"""

# COMMAND ----------

# This logic follows the lead from the query above (from instructions)
last_pr_df = spark.sql("SELECT stock_symbol, stock_exchange, latest_trade, event_seq_nb,trade_pr, last_mov_avg_pr FROM \
(SELECT stock_symbol, stock_exchange, latest_trade,  event_seq_nb, trade_pr,LAST_VALUE(mov_avg_pr) \
 OVER (PARTITION BY stock_symbol ORDER BY latest_trade RANGE BETWEEN INTERVAL '180' MINUTES PRECEDING AND CURRENT ROW) \
 AS last_mov_avg_pr FROM temp_trade_moving_avg) as tmp2b")
last_pr_df.show(3,truncate=False)

# The following, MUCH simpler query also works because:
# THE MOVING AVG PRICE AS OF ANY GIVEN TIMESTAMP IS INHERENTLY THE LATEST MOVING AVG PRICE AS OF THAT TIMESTAMP.  THE PREMISE MAKES NO SENSE.
last_pr_df2 = spark.sql("SELECT stock_symbol, stock_exchange, latest_trade,  event_seq_nb, trade_pr, mov_avg_pr as last_mov_avg_pr FROM temp_trade_moving_avg")
last_pr_df2.show(3,truncate=False)

# COMMAND ----------

prev_last_pr_df = spark.sql("SELECT stock_symbol, stock_exchange, latest_trade,  event_seq_nb, trade_pr, \
LAST_VALUE(mov_avg_pr) OVER (PARTITION BY stock_symbol ORDER BY latest_trade RANGE BETWEEN INTERVAL '180' MINUTES PRECEDING AND CURRENT ROW) AS last_mov_avg_pr \
                            FROM (SELECT stock_symbol,stock_exchange, latest_trade, event_seq_nb, trade_pr, \
                                  AVG(trade_pr) OVER (PARTITION BY stock_symbol ORDER BY latest_trade RANGE BETWEEN INTERVAL '180' MINUTES PRECEDING AND CURRENT ROW) AS mov_avg_pr \
                                  FROM tmp_last_trade) as prev_tmp_trade_mov_avg")
prev_last_pr_df.show(3,truncate=False)
# Again, the following, MUCH simpler query also works because:
# THE MOVING AVG PRICE AS OF ANY GIVEN TIMESTAMP IS INHERENTLY THE LATEST MOVING AVG PRICE AS OF THAT TIMESTAMP.  THE PREMISE MAKES NO SENSE.
# Either the instructions are intentionally ambiguous, or I made a wrong turn awhile ago
prev_last_pr_df2 = spark.sql("SELECT stock_symbol, stock_exchange, latest_trade, event_seq_nb, trade_pr, mov_avg_pr AS last_mov_avg_pr \
                            FROM (SELECT stock_symbol,stock_exchange, latest_trade, event_seq_nb, trade_pr, \
                                  AVG(trade_pr) OVER (PARTITION BY stock_symbol ORDER BY latest_trade RANGE BETWEEN INTERVAL '180' MINUTES PRECEDING AND CURRENT ROW) AS mov_avg_pr \
                                  FROM tmp_last_trade) as prev_tmp_trade_mov_avg")
prev_last_pr_df.show(3,truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3.4 Save The Temporary View Into Hive Table For Staging

# COMMAND ----------

# Instructions show: 
# mov_avg_df.write.saveAsTable("temp_last_trade")
# ^ is this a typo? meant to be below? ugh.

# COMMAND ----------

last_pr_df.write.mode('overwrite').saveAsTable("temp_last_trade")

# COMMAND ----------

prev_last_pr_df.write.mode('overwrite').saveAsTable("prev_temp_last_trade")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.4 Populate The Latest Trade and Latest Moving Average Trade Price To The Quote Records
# MAGIC Now that you’ve produced both staging tables, join them with the main table “quotes” to populate trade related information.

# COMMAND ----------

# the tables are the exact same, with the exception of the latter having been created with additional column selected: event_seq_nb
# what was the point of this?
print('first rows of current day, in ascending order:')
spark.sql("select * from temp_last_trade where stock_symbol = 'SYMC'").show(3,truncate=False)
prev_last_pr_df_ind = prev_last_pr_df.withColumn("index", monotonically_increasing_id())
print('last rows of previous day, in descending order:')
prev_last_pr_df_ind.orderBy(desc("index")).drop("index").show(5,truncate=False)
spark.sql("select * from temp_last_trade").show(3,truncate=False)
print('now an identical table:')
spark.sql("select * from temp_trade_moving_avg").show(3,truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.4.1 Join With Table temp_trade_moving_avg
# MAGIC You need to join “quotes” and “temp_trade_moving_avg” to populate trade_pr and mov_avg_pr into quotes. However, you cannot use equality join in this case; trade events don’t happen at the same quote time. You want the latest in time sequence. This is a typical time sequence analytical use case. A good method for this problem is to merge both tables in a common time sequence.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4.4.1.1 Define A Common Schema Holding “quotes” and “temp_trade_moving_avg” Records
# MAGIC This is a necessary step before the union of two datasets which have a different schema (denormalization). The schema needs to include all the fields of quotes and temp_trade_mov_avg so that no information gets lost.<

# COMMAND ----------

""" 
Schema:
Column         |  Value
--------------------------------------------------
trade_dt       |  Value from corresponding records
rec_type       |  “Q” for quotes, “T” for trades
stock_symbol   |  Value from corresponding records
event_tm       |  Value from corresponding records
event_seq_nb   |  From quotes, null for trades
exchange       |  Value from corresponding records
bid_pr         |  From quotes, null for trades
bid_size       |  From quotes, null for trades
ask_pr         |  From quotes, null for trades
ask_size       |  From quotes, null for trades
trade_pr       |  From trades, null for quotes
mov_avg_pr     |  From trades, null for quotes
"""

# COMMAND ----------

common_event_schema = StructType([StructField('trade_dt', DateType(), True),
                           StructField('rec_type', StringType(), True),
                           StructField('stock_symbol', StringType(), True),
                           StructField('stock_exchange', StringType(), True),
                           StructField('event_tm', TimestampType(), True),
                           StructField('event_seq_nb', IntegerType(), True),
                           StructField('bid_pr', FloatType(), True),
                           StructField('bid_size', IntegerType(), True),
                           StructField('ask_pr', FloatType(), True),
                           StructField('ask_size', IntegerType(), True),
                           StructField('trade_pr', FloatType(), True),
                           StructField('mov_avg_pr', FloatType(), True)])

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4.4.1.2 Create Spark Temp View To Union Both Tables
# MAGIC Perform data normalization in the select statement from both table followed by union:

# COMMAND ----------

"""PROPOSED TEMPLATE QUOTE:

quote_union = spark.sql("
# [Creates a denormalized view union both quotes and
temp_trade_moving_avg, populate null for fields not applicable]
")
quote_union.createOrReplaceTempView("quote_union")
"""


# COMMAND ----------

df =spark.read.parquet("/quote/trade_dt={}".format("2020-08-06"))
df.createOrReplaceTempView("quotes")

# COMMAND ----------

quote_union = spark.sql("SELECT date(latest_trade) as trade_dt, 'T' as rec_type,stock_symbol,stock_exchange,latest_trade as event_tm, event_seq_nb, null as bid_pr,null as bid_size,null as ask_pr, null as ask_size, trade_pr,mov_avg_pr from temp_trade_moving_avg").union(spark.sql("SELECT trade_dt, 'Q' as rec_type,stock_symbol,stock_exchange,latest_quote as event_tm, event_seq_nb, bid_pr,bid_size,ask_pr, ask_size, null as trade_pr,null as mov_avg_pr from quotes"))
quote_union.createOrReplaceTempView("quote_union")

# COMMAND ----------

yesterday_very_last_trades = spark.sql("SELECT date(latest_trade) AS trade_dt, 'T' AS rec_type,stock_symbol, stock_exchange, latest_trade as event_tm, event_seq_nb,null as bid_pr,null as bid_size, null as ask_pr,null as ask_size,last_trade_pr AS trade_pr, last_mov_avg_pr AS mov_avg_pr FROM (SELECT stock_symbol, stock_exchange, latest_trade,  event_seq_nb, LAST_VALUE(trade_pr) OVER (PARTITION BY stock_symbol,stock_exchange ORDER BY latest_trade RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_trade_pr, LAST_VALUE(last_mov_avg_pr) OVER (PARTITION BY stock_symbol,stock_exchange ORDER BY latest_trade RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_mov_avg_pr FROM prev_temp_last_trade) AS prev_last_trades")

w = Window.partitionBy('stock_symbol','stock_exchange').orderBy(desc('event_tm'))
ydf = yesterday_very_last_trades.withColumn('Rank',dense_rank().over(w))
yesterday_very_last_trades = ydf.filter(ydf.Rank == 1).drop(ydf.Rank).orderBy('event_tm')


# COMMAND ----------

print("These are the very last trade prices and moving average trade prices of the previous day, per symbol and stock exchange: ")
yesterday_very_last_trades.show(truncate=False)

# COMMAND ----------

quote_union_all = yesterday_very_last_trades.union(spark.sql("SELECT * FROM quote_union")).orderBy('stock_exchange','stock_symbol','event_tm','rec_type')

# COMMAND ----------

print('Now including previous days last state: ')
quote_union_all.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4.4.1.3 Populate The Latest trade_pr and mov_avg_pr
# MAGIC Use a window analytical function to populate the values from the latest records with rec_type “T.” The window should be under the partition of the exchange symbol:

# COMMAND ----------

""" QUERY TEMPLATE FROM INSTRUCTIONS:
quote_union_update = spark.sql("
select *,
# [logic for the last not null trade price] AS last_trade_pr,
# [logic for the last not null mov_avg_pr price] AS last_mov_avg_pr
from quote_union
")
quote_union_update.createOrReplaceTempView("quote_union_update")
"""

# COMMAND ----------

window = Window.partitionBy('stock_symbol','stock_exchange').orderBy('event_tm').rowsBetween(-sys.maxsize,0)
fill_pr = last(quote_union_all['trade_pr'],ignorenulls=True).over(window)
fill_avg_pr = last(quote_union_all['mov_avg_pr'],ignorenulls=True).over(window)
quote_union_filled = quote_union_all.withColumn('last_trade_price', fill_pr)
quote_union_filled = quote_union_filled.withColumn('last_mov_avg_price', fill_avg_pr)
quote_update = quote_union_filled.drop('trade_pr','mov_avg_pr').filter(quote_union_filled.rec_type == 'Q')

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4.4.1.4 Filter For Quote Records
# MAGIC Since you ultimately only need quote records, filter out trade events after the calculation.

# COMMAND ----------

quote_update.createOrReplaceTempView("quote_update")
spark.sql("select * from quote_update").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.4.2 Join With Table temp_last_trade To Get The Prior Day Close Price
# MAGIC The prior day close price table has a single record per symbol and exchange. For this join, you can use equality join since the join fields are only “symbol” and “exchange”. However, note that this table has a very limited number of records (no more than the number of symbol and exchange combinations). This is an excellent opportunity to use broadcast join to achieve optimal join performance. Broadcast join is always recommended if one of the join tables is small enough so that the whole dataset fits in memory.

# COMMAND ----------

#Already done!

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.4.3 Write The Final Dataframe Into Azure Blob Storage At Corresponding Partition'
# MAGIC The final output needs to be stored as a new partition for the object on Azure Blob Storage.

# COMMAND ----------

quote_update.write.parquet("dbfs:/allrecords/date=2020-08-06")
