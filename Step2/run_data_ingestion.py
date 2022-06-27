# Databricks notebook source
from datetime import datetime
import json
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

#if not using preconfigured databricks cluster as kernel
#spark.conf.set("fs.azure.account.key.pipelinestorageacctaus.blob.core.windows.net","f6fWRdrrX8qYB9a1y2Rlgu7qCuyeHuD59j3UIb0hi3ZanAn8DUmej+uofzFi7irJm954fTa5LtBb+AStzjJHYA==")
#spark.conf.set("spark.hadoop.fs.azure.account.key.your_storage_account.blob.core.windows.net","f6fWRdrrX8qYB9a1y2Rlgu7qCuyeHuD59j3UIb0hi3ZanAn8DUmej+uofzFi7irJm954fTa5LtBb+AStzjJHYA==")
#spark.conf.set("fs.azure.sas.pipelineauscontainer.pipelinestorageacctaus.blob.core.windows.net","si=databricks_access&spr=https&sv=2021-06-08&sr=c&sig=ugpsC3zLJtG3xCkPh48p%2BHZUf7IWZYvu%2BVOJ2837L08%3D")

#--begin azure blob mounting---
storageAccountName = 'pipelinestorageaccaus'
storageAccountAccessKey = '<hidden>'
blobContainerName = 'pipelineauscontainer'

if not any(mount.mountPoint == '/mnt/output_dir/' for mount in dbutils.fs.mounts()):
  try:
    dbutils.fs.mount(
    source = "wasbs://{}@{}.blob.core.windows.net".format(blobContainerName, storageAccountName),
    mount_point = "/mnt/output_dir/",
    extra_configs = {'fs.azure.account.key.' + storageAccountName + '.blob.core.windows.net': storageAccountAccessKey}
  )
  except Exception as e:
    print("already mounted. Try to unmount first")

display(dbutils.fs.ls("dbfs:/mnt/output_dir"))
#--end azure blob mounting---

def parse_csv(line):
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


def parse_json(line):
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
spark = SparkSession.builder.getOrCreate()
for dt in dates:
    rawcsv = spark.sparkContext.textFile(f"wasbs://pipelineauscontainer@pipelinestorageacctaus.blob.core.windows.net/data/csv/%s/NYSE/*.txt" %dt)
    rawjson = spark.sparkContext.textFile(f"wasbs://pipelineauscontainer@pipelinestorageacctaus.blob.core.windows.net/data/json/%s/NASDAQ/*.txt" %dt)
    parsedcsv = rawcsv.map(lambda line: parse_csv(line))
    parsedjson = rawjson.map(lambda line: parse_json(line))
    datacsv = spark.createDataFrame(parsedcsv, common_event_schema)
    datajson = spark.createDataFrame(parsedjson, common_event_schema)
    csvlist.append(datacsv)
    jsonlist.append(datajson)

csv_data = csvlist[0].union(csvlist[1])
json_data= jsonlist[0].union(jsonlist[1])
all_data = csv_data.union(json_data)

print('all parsed and combined data: ')
all_data.show()
all_data.write.partitionBy("partition").mode("overwrite").parquet("dbfs:/mnt/output_dir")
