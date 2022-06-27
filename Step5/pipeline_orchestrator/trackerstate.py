import pyspark
from pyspark.sql import SparkSession
from pathlib import Path
import logging
import logging.config
import configparser
import time
from pathlib import Path
import sys

path = str(Path(Path(__file__).parent.absolute()).parent.absolute())
logging.info(path)

try:
    sys.path.insert(1, path+"/pipeline_orchestrator/preprocess")
    sys.path.insert(2, path+"/pipeline_orchestrator/ingestion")
    sys.path.insert(4, path+"/pipeline_orchestrator/analysis")
    sys.path.insert(4, path+"/pipeline_orchestrator/tracker")

    from preprocess import Preprocessor
    from ingestion import Batchloader
    from analysis import Analyzer
    from Tracker import Tracker

except (ModuleNotFoundError, ImportError) as e:
    print("{} fileure".format(type(e)))
else:
    print("Import succeeded")

logging.basicConfig(format='%(asctime)s :: %(levelname)s :: %(funcName)s :: %(lineno)d \
:: %(message)s', level = logging.INFO)

def create_sparksession():
    """
    Initialize a spark session
    """
    return SparkSession.builder.appName("pipeline_orchestrator_app").getOrCreate()

def main(job: str = "",**kwargs):
    """
    Driver code to perform the following steps
    1. Preprocesses (extracts) parquet files, then ingests (transform+load) the EOD trade and quote records
    2. Executes analytical functions on consolidated data
    """
    logging.debug("\n\nSetting up Spark Session...")
    spark = create_sparksession()
    spark.conf.set("spark.databricks.service.address", "https://adb-3064050213402635.15.azuredatabricks.net")
    spark_context = spark.sparkContext
    preprocess= Preprocessor(spark,spark_context)
    batchload = Batchloader(spark)
    analyze = Analyzer(spark)

    # cleaning tables
    logging.debug("\n\nCleaning Hive Tables...")
    spark.sql("DROP TABLE IF EXISTS tmp_trade_moving_avg")
    spark.sql("DROP TABLE IF EXISTS temp_last_trade")
    spark.sql("DROP TABLE IF EXISTS tmp_last_trade")
    spark.sql("DROP TABLE IF EXISTS quotes")
    spark.sql("DROP TABLE IF EXISTS quote_union")
    spark.sql("DROP TABLE IF EXISTS quote_update")
    spark.sql("DROP TABLE IF EXISTS temp_trade_mov_avg")
    spark.sql("DROP TABLE IF EXISTS prev_temp_last_trade")

    # Modules in the project
    modules = {
         "rawfiles": preprocess.process_csvjson,
         "eodtrades": batchload.tradeload,
         "eodquotes": batchload.quoteload,
         "analyzed": analyze.analyze
    }

    for file in modules.keys():
        try:
           logging.info("Processing "+file)
           modules[file]()
        except Exception as e:
           print(e)
           logging.error(" Error encountered while processing "+file)
           return

#Pipeline Entrypoint
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--job", type=str)
    parser.add_argument("--date_range", default=",")

    args = parser.parse_args()
    args = vars(args)

    main(**args)

    trade_date = "2020-08-06"

    logging.info("Updating the job status in the POSTGRES Table")

    tracker = Tracker("analytical_etl", trade_date)

    try:
        logging.info("Updating the job status")
        tracker.update_job_status("success")
    except Exception as e:
        tracker.update_job_status("failed")
        logging.error("Error while performing operation in the table {}".format(e))

    logging.info("ALL DONE!\n")
