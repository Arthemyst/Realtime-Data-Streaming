import logging
from datetime import datetime

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col

def create_keyspace(session):
    # create keyspace here
    pass


def create_table(session):
    # create table here
    pass


def insert_data(session, **kwargs):
    # insertion here
    pass


def create_spark_connection():
    spark_conn = None
    try:
        spark_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.41,"
                                           "org.apache.spark:spark-sql-kafka-0.-10_2.13:3.4.1") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()
        spark_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f'Could not create the spark session due to exception: {e}')

    return spark_conn


def create_cassandra_connection():
    # creating cassandra connection
    pass


if __name__ == "__main__":
    spark_connection = create_spark_connection()
