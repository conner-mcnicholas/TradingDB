#!/bin/sh
. ~/.profile
spark-submit \
--master local \
--py-files dist/pipeline_orchestration-0.0.1-py3.8.egg \
--conf spark.driver.extraClassPath=./jars/hadoop-azure-3.3.0.jar:./jars/azure-storage-8.6.5.jar:./jars/postgresql-42.4.0.jar \
pipeline_orchestrator/analysis/analysis.py
