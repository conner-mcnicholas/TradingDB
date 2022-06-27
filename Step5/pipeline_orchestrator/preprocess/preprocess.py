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

class Preprocessor:
    """
    Parses csvs and jsons to save down trade and quote records in parquet format
    """
    def __init__(self, spark, spark_context: pyspark.SparkContext):
        self.spark = spark
        self._spark_context = spark_context
        self._load_path = "dbfs:/mnt/output_dir"
        self._save_path = "dbfs:/preprocessed"

    @staticmethod
    def parse_csv(line):
        """
        parses csv files into trade and quote records
        """
        record_type_pos = 2
        record = line.split(",")
        try:
            if record[record_type_pos] == 'T':
                  return (datetime.strptime(record[0], '%Y-%m-%d').date(),
                          record[2],
                          record[3],
                          record[6],
                          datetime.strptime(record[4], '%Y-%m-%d %H:%M:%S.%f'),
                          int(record[5]),
                          datetime.strptime(record[1], '%Y-%m-%d %H:%M:%S.%f'),
                          float(record[7]),
                          None,
                          None,
                          None,
                          None,
                          record[2])
            elif record[record_type_pos] == 'Q':
                  return (datetime.strptime(record[0], '%Y-%m-%d').date(),
                          record[2],
                          record[3],
                          record[6],
                          datetime.strptime(record[4], '%Y-%m-%d %H:%M:%S.%f'),
                          int(record[5]),
                          datetime.strptime(record[1], '%Y-%m-%d %H:%M:%S.%f'),
                          None,
                          float(record[7]),
                          int(record[8]),
                          float(record[9]),
                          int(record[10]),
                          record[2])
        except:
            return (None, None, None, None, None, None, None, None, None, None, None, None, 'B')

    @staticmethod
    def parse_json(line):
        """
        parses json files into trade and quote records
        """
        line = json.loads(line)
        record_type = line['event_type']
        try:
            if record_type == 'T':
                  return (datetime.strptime(line['trade_dt'], '%Y-%m-%d').date(),
                          record_type,
                          line['symbol'],
                          line['exchange'],
                          datetime.strptime(line['event_tm'], '%Y-%m-%d %H:%M:%S.%f'),
                          line['event_seq_nb'],
                          datetime.strptime(line['file_tm'], '%Y-%m-%d %H:%M:%S.%f'),
                          line['price'],
                          None,
                          None,
                          None,
                          None,
                          record_type)
            elif record_type == 'Q':
                  return (datetime.strptime(line['trade_dt'], '%Y-%m-%d').date(),
                          record_type,
                          line['symbol'],
                          line['exchange'],
                          datetime.strptime(line['event_tm'], '%Y-%m-%d %H:%M:%S.%f'),
                          line['event_seq_nb'],
                          datetime.strptime(line['file_tm'], '%Y-%m-%d %H:%M:%S.%f'),
                          None,
                          line['bid_pr'],
                          line['bid_size'],
                          line['ask_pr'],
                          line['ask_size'],
                          record_type)
        except:
            return (None, None, None, None, None, None, None, None, None, None, None, None, 'B')

    def process_csvjson(self):
        """
        consolidates csv and json records and saves down records partitioned by trade or quote
        """
        common_event_schema = StructType([StructField('trade_dt', DateType(), True),
                                   StructField('rec_type', StringType(), True),
                                   StructField('symbol', StringType(), True),
                                   StructField('exchange', StringType(), True),
                                   StructField('event_tm', TimestampType(), True),
                                   StructField('event_seq_nb', IntegerType(), True),
                                   StructField('arrival_tm', TimestampType(), True),
                                   StructField('trade_pr', FloatType(), True),
                                   StructField('bid_pr', FloatType(), True),
                                   StructField('bid_size', IntegerType(), True),
                                   StructField('ask_pr', FloatType(), True),
                                   StructField('ask_size', IntegerType(), True),
                                   StructField('partition', StringType(), True)])

        dates = ['2020-08-05','2020-08-06']
        csvlist = []
        jsonlist = []

        for dt in dates:
            rawcsv = self.spark.sparkContext.textFile(f"{self._load_path}/data/csv/{dt}/NYSE/*.txt")
            rawjson = self.spark.sparkContext.textFile(f"{self._load_path}/data/json/{dt}/NASDAQ/*.txt")
            parsedcsv = rawcsv.map(lambda line: parse_csv(line))
            parsedjson = rawjson.map(lambda line: parse_json(line))
            datacsv = self.spark.createDataFrame(parsedcsv, common_event_schema)
            datajson = self.spark.createDataFrame(parsedjson, common_event_schema)
            csvlist.append(datacsv)
            jsonlist.append(datajson)

        csv_data = csvlist[0].union(csvlist[1])
        json_data= jsonlist[0].union(jsonlist[1])
        all_data = csv_data.union(json_data)

        print('all parsed and combined data: ')
        all_data.show(truncate=False)
        all_data.write.partitionBy("partition").mode("overwrite").parquet(self._save_path)
