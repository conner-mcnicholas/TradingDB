# Step 5: Pipeline Orchestration

In Microsoft Azure Cloud, my workflow runs in elastic Databricks spark clusters
with input and output data that are persistent on Azure Blob Storage containers.
The pipeline can be divided into two workflows:
- Preprocessing Workflow: data ingestion and batch load (spark_ingest.sh):
- Analytical Workflow: analytical ETL job (spark_analyticalETL.sh):<br>

Example of spark script:<br>
![alt text](https://github.com/conner-mcnicholas/TradingDB/blob/main/Step5/imgs/scr1.png?raw=true)<br>

After creating these shell scripts, I:
- Executed Spark jobs in Azure Elastic Databricks Clusters
- Designed job status tracker (src/tracker/Tracker.py , src/tracker_updater/updater.py)

Status tracker monitoring events in the workflow:<br>
![alt text](https://github.com/conner-mcnicholas/TradingDB/blob/main/Step5/imgs/statustracker.png?raw=true)<br>
