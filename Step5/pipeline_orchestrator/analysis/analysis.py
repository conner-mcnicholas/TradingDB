from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import pyspark
from pathlib import Path
from datetime import datetime, timedelta
import logging
logging.basicConfig(format='%(asctime)s :: %(levelname)s :: %(funcName)s :: %(lineno)d \
:: %(message)s', level = logging.INFO)

class Analyzer:
    """
    calculates the following for a given day:
        - Latest trade price before the quote.
        - Latest 30-minute moving average trade price, before the quote.
        - The bid/ask price movement from previous day’s closing price
    """
    def __init__(self,spark):
        self.spark=spark
        self._load_path = "dbfs:/postprocessed"
        self._save_path = "dbfs:/analyzed"

    def analyze(self):
        df = self.spark.read.parquet.load({self._load_path}+"/trade/trade_dt={}".format("2020-08-06"))
        df = df.select("stock_symbol","stock_exchange","latest_trade", "event_seq_nb", "trade_pr").where(df.trade_dt == "2020-08-06")
        df.createOrReplaceTempView("tmp_trade_moving_avg")

        self.spark.sql("SELECT stock_symbol,stock_exchange,latest_trade,event_seq_nb,trade_pr FROM tmp_trade_moving_avg").show(truncate=False)

        mov_avg_df = self.spark.sql("SELECT stock_symbol,stock_exchange, latest_trade, event_seq_nb, trade_pr, \
            AVG(trade_pr) OVER (PARTITION BY stock_symbol ORDER BY latest_trade RANGE BETWEEN INTERVAL '180' MINUTES PRECEDING AND CURRENT ROW) AS mov_avg_pr \
            FROM tmp_trade_moving_avg")
        mov_avg_df.show(3,truncate=False)
        mov_avg_df.write.mode('overwrite').saveAsTable("temp_trade_moving_avg")

        import datetime
        date = datetime.datetime.strptime('2020-08-06', '%Y-%m-%d')
        prev_date_str = (date - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        print('Assuming current date of 2020-08-06, prev_date_str = ' + prev_date_str)


        df =spark.read.parquet("/trade/trade_dt={}".format(prev_date_str))
        df = df.select("stock_symbol","stock_exchange","latest_trade", "event_seq_nb", "trade_pr").where(df.trade_dt == prev_date_str)
        df.show(truncate=False)
        df.createOrReplaceTempView("tmp_last_trade")

        last_pr_df = self.spark.sql("SELECT stock_symbol, stock_exchange, latest_trade, event_seq_nb,trade_pr, last_mov_avg_pr FROM \
        (SELECT stock_symbol, stock_exchange, latest_trade,  event_seq_nb, trade_pr,LAST_VALUE(mov_avg_pr) \
         OVER (PARTITION BY stock_symbol ORDER BY latest_trade RANGE BETWEEN INTERVAL '180' MINUTES PRECEDING AND CURRENT ROW) \
         AS last_mov_avg_pr FROM temp_trade_moving_avg) as tmp2b")
        last_pr_df.show(3,truncate=False)
        last_pr_df.write.mode('overwrite').saveAsTable("temp_last_trade")

        prev_last_pr_df = self.spark.sql("SELECT stock_symbol, stock_exchange, latest_trade,  event_seq_nb, trade_pr, \
        LAST_VALUE(mov_avg_pr) OVER (PARTITION BY stock_symbol ORDER BY latest_trade RANGE BETWEEN INTERVAL '180' MINUTES PRECEDING AND CURRENT ROW) AS last_mov_avg_pr \
                                    FROM (SELECT stock_symbol,stock_exchange, latest_trade, event_seq_nb, trade_pr, \
                                          AVG(trade_pr) OVER (PARTITION BY stock_symbol ORDER BY latest_trade RANGE BETWEEN INTERVAL '180' MINUTES PRECEDING AND CURRENT ROW) AS mov_avg_pr \
                                          FROM tmp_last_trade) as prev_tmp_trade_mov_avg")
        prev_last_pr_df.show(3,truncate=False)
        prev_last_pr_df.write.mode('overwrite').saveAsTable("prev_temp_last_trade")

        print('first rows of current day, in ascending order:')
        self.spark.sql("select * from temp_last_trade where stock_symbol = 'SYMC'").show(3,truncate=False)
        prev_last_pr_df_ind = prev_last_pr_df.withColumn("index", monotonically_increasing_id())
        print('last rows of previous day, in descending order:')
        prev_last_pr_df_ind.orderBy(desc("index")).drop("index").show(5,truncate=False)
        self.spark.sql("select * from temp_last_trade").show(3,truncate=False)
        print('now an identical table:')
        self.spark.sql("select * from temp_trade_moving_avg").show(3,truncate=False)

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

        df =spark.read.parquet("/quote/trade_dt={}".format("2020-08-06"))
        df.createOrReplaceTempView("quotes")

        quote_union = self.spark.sql("SELECT date(latest_trade) as trade_dt, 'T' as rec_type,stock_symbol,stock_exchange,latest_trade as event_tm, event_seq_nb, null as bid_pr,null as bid_size,null as ask_pr, null as ask_size, trade_pr,mov_avg_pr from temp_trade_moving_avg").union(spark.sql("SELECT trade_dt, 'Q' as rec_type,stock_symbol,stock_exchange,latest_quote as event_tm, event_seq_nb, bid_pr,bid_size,ask_pr, ask_size, null as trade_pr,null as mov_avg_pr from quotes"))
        quote_union.createOrReplaceTempView("quote_union")

        yesterday_very_last_trades = self.spark.sql("SELECT date(latest_trade) AS trade_dt, 'T' AS rec_type,stock_symbol, stock_exchange, latest_trade as event_tm, event_seq_nb,null as bid_pr,null as bid_size, null as ask_pr,null as ask_size,last_trade_pr AS trade_pr, last_mov_avg_pr AS mov_avg_pr FROM (SELECT stock_symbol, stock_exchange, latest_trade,  event_seq_nb, LAST_VALUE(trade_pr) OVER (PARTITION BY stock_symbol,stock_exchange ORDER BY latest_trade RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_trade_pr, LAST_VALUE(last_mov_avg_pr) OVER (PARTITION BY stock_symbol,stock_exchange ORDER BY latest_trade RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_mov_avg_pr FROM prev_temp_last_trade) AS prev_last_trades")

        w = Window.partitionBy('stock_symbol','stock_exchange').orderBy(desc('event_tm'))
        ydf = yesterday_very_last_trades.withColumn('Rank',dense_rank().over(w))
        yesterday_very_last_trades = ydf.filter(ydf.Rank == 1).drop(ydf.Rank).orderBy('event_tm')

        print("These are the very last trade prices and moving average trade prices of the previous day, per symbol and stock exchange: ")
        yesterday_very_last_trades.show(truncate=False)

        quote_union_all = yesterday_very_last_trades.union(spark.sql("SELECT * FROM quote_union")).orderBy('stock_exchange','stock_symbol','event_tm','rec_type')

        print('Now including previous days last state: ')
        quote_union_all.show(truncate=False)

        window = Window.partitionBy('stock_symbol','stock_exchange').orderBy('event_tm').rowsBetween(-sys.maxsize,0)
        fill_pr = last(quote_union_all['trade_pr'],ignorenulls=True).over(window)
        fill_avg_pr = last(quote_union_all['mov_avg_pr'],ignorenulls=True).over(window)
        quote_union_filled = quote_union_all.withColumn('last_trade_price', fill_pr)
        quote_union_filled = quote_union_filled.withColumn('last_mov_avg_price', fill_avg_pr)
        quote_update = quote_union_filled.drop('trade_pr','mov_avg_pr').filter(quote_union_filled.rec_type == 'Q')

        quote_update.createOrReplaceTempView("quote_update")
        self.spark.sql("select * from quote_update").show(truncate=False)

        quote_update.write.parquet(f"{self._save_path}/date=2020-08-06")
