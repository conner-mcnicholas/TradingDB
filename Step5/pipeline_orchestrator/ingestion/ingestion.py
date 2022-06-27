import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import SparkConf, SparkContext
from datetime import datetime,timedelta,date
import json
import logging
from decimal import Decimal
import datetime

logging.basicConfig(format='%(asctime)s :: %(levelname)s :: %(funcName)s :: %(lineno)d \
:: %(message)s', level = logging.INFO)

class Batchloader:
    """
    Transforms and saves down data in improved format
    """
    def __init__(self, spark):
        self.spark = spark
        self._load_path = "dbfs:/preprocessed"
        self._save_path = "dbfs:/postprocessed"

    def applyLatest(df):
        """
        applies data correction to trade and quote records
        """
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

    def tradeload(self):
        # Read Trade Partition Dataset From It’s Temporary Location
        trade_common = self.spark.read.format('parquet').load({self._load_path}+"/partition=T")
        # Select The Necessary Columns For Trade Records
        trade = trade_common.select("trade_dt", "rec_type", "symbol", "exchange", "event_tm", "event_seq_nb", "arrival_tm", "trade_pr")

        trade_corrected = applyLatest(trade)
        trades_080520 = trade_corrected.where(trade_corrected.trade_dt == "2020-08-05")
        trades_080620 = trade_corrected.where(trade_corrected.trade_dt == "2020-08-06")

        #  Write The Trade Dataset Back To Parquet On Azure Blob Storage
        trades_080520.write.parquet({self._save_path}+"/trade/trade_dt={}".format('2020-08-05'))
        trades_080620.write.parquet({self._save_path}+"/trade/trade_dt={}".format('2020-08-06'))


    def quoteload(self):
        quote_common = self.spark.read.format('parquet').load({self._load_path}+"/partition=Q")
        # Select The Necessary Columns For Quote Records
        quote = quote_common.select("trade_dt", "rec_type", "symbol", "exchange", "event_tm", "event_seq_nb", "arrival_tm", "bid_pr", "bid_size", "ask_pr", "ask_size")

        # Apply Data Correction
        quote_corrected = applyLatest(quote)

        # Separate dataframes by trade date
        quotes_080520 = quote_corrected.where(quote_corrected.trade_dt == "2020-08-05")
        quotes_080620 = quote_corrected.where(quote_corrected.trade_dt == "2020-08-06")

        # Write The Quote Dataset Back To Parquet On Azure Blob Storage
        quotes_080520.write.parquet({self._save_path}+"/quote/trade_dt={}".format('2020-08-05'))
        quotes_080620.write.parquet({self._save_path}+"/quote/trade_dt={}".format('2020-08-06'))
