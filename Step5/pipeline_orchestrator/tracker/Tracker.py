import datetime
import psycopg2
import uuid
import random
import logging
import logging.config
from psycopg2 import sql
from pathlib import Path

class Tracker(object):
    """
    job_id, status, updated_time
    """
    def __init__(self, jobname, dbconfig):
        self.jobname = jobname
        self.dbconfig = dbconfig

    def assign_job_id(self):
        job_id = str(uuid.uuid4())
        return job_id

    def update_job_status(self, status):
        job_id = self.assign_job_id()
        print("Job ID Assigned: {}".format(job_id))
        table_name = "spark_job"
        conn = self.get_db_connection()
        cursor = conn.cursor()

        try:
            logging.info("Trying to create table {}".format(table_name))
            cursor.execute("DROP TABLE IF EXISTS {}".format(table_name))

            create_table_query = sql.SQL("""
                CREATE TABLE {} (job_id STRING NOT NULL, job_name VARCHAR(50),\
                                 status VARCHAR(50),trade_date DATE NOT NULL);
            """).format(sql.Identifier(table_name))

            cursor.execute(create_table_query)

            logging.info("Created Table {} for job_id: {}".format(table_name,job_id))

            logging.info(" Updating status of the job in the table")

            query = sql.SQL("""
                insert into {} (job_id, job_name, status, trade_date)
                values (%s, %s, %s,%s)
            """).format(sql.Identifier(table_name))

            cursor.execute(query, (job_id, self.jobname, status, self.trade_date))

            logging.info("Printing the output of spark job table {}".format(table_name))
            cursor.execute("SELECT * FROM {}".format(table_name))

            rows = cursor.fetchall()

            for row in rows:
                logging.info("Data row = (%s, %s, %s, %s)" %(str(row[0]), str(row[1]), str(row[2]), str(row[3])))

        except (Exception, psycopg2.Error) as error:
            logging.error("Error getting value from the Database {}".format(error))
        return

    def get_job_status(self, job_id):
    # connect db and send sql query
        table_name = config.get('POSTGRES', 'job_tracker_table_name')
        conn = self.get_db_connection()
        cursor = conn.cursor()
        try:
            logging.info(" Fetching status of the job from the table")
            cursor.execute("SELECT status FROM {} where job_id={}", table_name, job_id)
            record = cursor.fetchall()
            return record
        except (Exception, psycopg2.Error) as error:
            logging.error("Error getting value from the Database {}".format(error))
            return

    def get_db_connection(self):
        connection = None
        host = ""
        user = ""
        dbname = ""
        password = ""
        sslmode = ""
        conn_string = "host={0} user={1} dbname={2} password={3} sslmode={4}".format(host, user, dbname, password, sslmode)
        try:
            connection = psycopg2.connect(conn_string)
            logging.info(" Successfully connected to postgres DB")
        except (Exception, psycopg2.Error) as error:
            logging.error("Error while connecting to PostgreSQL {}".format(error))
        return connection
