# Guided Capstone

Investment analysts want to better understand their raw quote data by referencing specific trade indicators which occur whenever their quote data is generated, including:
- Latest trade price
- Prior day closing price
- 30-minute moving average trade price

As a data engineer, you are asked to build a data pipeline that produces a dataset including the above indicators. The goal of this project is to build an end-to-end data pipeline to ingest and process daily stock market data from multiple stock exchanges. The pipeline should maintain the source data in a structured format, organized by date. It also needs to produce analytical results that support business analysis.

## Step 1: Database Table Design

![alt text](https://github.com/conner-mcnicholas/TradingDB/blob/main/imgs/arch_diagram.png?raw=true)

## Step 2: Data Ingestion

work deliverable captured in Step2/run.ipynb

![alt text](https://github.com/conner-mcnicholas/TradingDB/blob/main/imgs/finalsuccess.png?raw=true)

## Step 3: End-of-Day (EOD) Data Load

work deliverable captured in Step3/end_of_day_ETL.py <br>
(originally ran in databricks pyspark cluster notebook)

![alt text](https://github.com/conner-mcnicholas/TradingDB/blob/main/imgs/notebook_results.png?raw=true)

## Step 4: Analytical ETL

I did these exercises in a Databricks Notebook, found in the Step4 dir.  The fully reconciled
quotes and trade records appear as follows:<br>

![alt text](https://github.com/conner-mcnicholas/TradingDB/blob/main/imgs/complete_analysis.png?raw=true)<br>

There were several erroneous or miscommunicated instructions that frustrated this effort:<br>

![alt text](https://github.com/conner-mcnicholas/TradingDB/blob/main/imgs/3hr_mavg.png?raw=true)<br>
![alt text](https://github.com/conner-mcnicholas/TradingDB/blob/main/imgs/wrongtable.png?raw=true)<br>
![alt text](https://github.com/conner-mcnicholas/TradingDB/blob/main/imgs/which_tmp_table.png?raw=true)<br>
![alt text](https://github.com/conner-mcnicholas/TradingDB/blob/main/imgs/wrong_query.png?raw=true)<br>
![alt text](https://github.com/conner-mcnicholas/TradingDB/blob/main/imgs/simple_alternative.png?raw=true)
