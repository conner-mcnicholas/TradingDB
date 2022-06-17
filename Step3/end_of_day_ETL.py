# Originally ran from databricks pyspark cluster and these weren't necessary
import pyspark
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# Filling config params with dummy data throughout
spark.conf.set("mycontainer@mystorage","somekey")

# Read Trade Partition Dataset From It’s Temporary Location
trade_common = spark.read.format('parquet').load("wasbs://mycontainer@mystorage.blob.core.windows.net/output_dir/partition=T")

# Select The Necessary Columns For Trade Records
trade = trade_common.select("trade_dt", "rec_type", "symbol", "exchange", "event_tm", "event_seq_nb", "arrival_tm", "trade_pr")

# Apply Data Correction
def applyLatest(df):
    #trades
    if df.first()["rec_type"] == "T":
        df_grouped = df.groupBy("trade_dt", "rec_type", "symbol", "arrival_tm", "event_seq_nb").agg(max("event_tm").alias("latest_trade"))
        df_joined = df_grouped.join(df.select("event_tm", "exchange", "trade_pr"), df.event_tm == df_grouped.latest_trade, "inner")
        df_final = df_joined.select("trade_dt", "rec_type", col("symbol").alias("stock_symbol"), col("exchange").alias("stock_exchange"), "latest_trade", "event_seq_nb", "arrival_tm", "trade_pr").orderBy("trade_dt", "symbol", "event_seq_nb")
        return df_final
    #quotes
    elif df.first()["rec_type"] == "Q":
        df_grouped = df.groupBy("trade_dt", "rec_type", "symbol", "arrival_tm", "event_seq_nb").agg(max("event_tm").alias("latest_quote"))
        df_joined = df_grouped.join(df.select("event_tm", "exchange", "bid_pr", "bid_size", "ask_pr", "ask_size"), df.event_tm == df_grouped.latest_quote, "inner")
        df_final = df_joined.select("trade_dt", "rec_type", col("symbol").alias("stock_symbol"), col("exchange").alias("stock_exchange"), "latest_quote", "event_seq_nb", "arrival_tm", "bid_pr", "bid_size", "ask_pr", "ask_size").orderBy("trade_dt", "symbol", "event_seq_nb")
        return df_final

trade_corrected = applyLatest(trade)
trades_080520 = trade_corrected.where(trade_corrected.trade_dt == "2020-08-05")
trades_080620 = trade_corrected.where(trade_corrected.trade_dt == "2020-08-06")

#  Write The Trade Dataset Back To Parquet On Azure Blob Storage
trades_080520.write.parquet("wasbs://mycontainer@mystorage.blob.core.windows.net/trade/trade_dt={}".format('2020-08-05'))
trades_080620.write.parquet("wasbs://mycontainer@mystorage.blob.core.windows.net/trade/trade_dt={}".format('2020-08-06'))

# ****************** REPEAT FOR QUOTES ******************
quote_common = spark.read.format('parquet').load("wasbs://mycontainer@mystorage.blob.core.windows.net/output_dir/partition=Q")

# Select The Necessary Columns For Quote Records
quote = quote_common.select("trade_dt", "rec_type", "symbol", "exchange", "event_tm", "event_seq_nb", "arrival_tm", "bid_pr", "bid_size", "ask_pr", "ask_size")

# Apply Data Correction
quote_corrected = applyLatest(quote)

# Separate dataframes by trade date
quotes_080520 = quote_corrected.where(quote_corrected.trade_dt == "2020-08-05")
quotes_080620 = quote_corrected.where(quote_corrected.trade_dt == "2020-08-06")

# Write The Quote Dataset Back To Parquet On Azure Blob Storage
quotes_080520.write.parquet("wasbs://mycontainer@mystorage.blob.core.windows.net/quote/trade_dt={}".format('2020-08-05'))
quotes_080620.write.parquet("wasbs://mycontainer@mystorage.blob.core.windows.net/quote/trade_dt={}".format('2020-08-06'))
