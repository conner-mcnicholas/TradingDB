# Guided Capstone

Investment analysts want to better understand their raw quote data by referencing specific trade indicators which occur whenever their quote data is generated, including:
- Latest trade price
- Prior day closing price
- 30-minute moving average trade price

As a data engineer, you are asked to build a data pipeline that produces a dataset including the above indicators. The goal of this project is to build an end-to-end data pipeline to ingest and process daily stock market data from multiple stock exchanges. The pipeline should maintain the source data in a structured format, organized by date. It also needs to produce analytical results that support business analysis.

## Step 1: Database Table Design

![alt text](https://github.com/conner-mcnicholas/TradingDB/blob/main/arch_diagram.png?raw=true)

## Step 2: Data Ingestion

work deliverable captured in Step2/run.ipynb

## Step 3: End-of-Day (EOD) Data Load

work deliverable captured in Step3/end_of_day_ETL.py <br>
(originally ran in databricks pyspark cluster notebook)
