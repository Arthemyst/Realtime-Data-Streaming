import logging

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession


def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!")


def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.created_users (
        id UUID PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        gender TEXT,
        address TEXT,
        post_code TEXT,
        email TEXT,
        username TEXT,
        registered_date TEXT,
        phone TEXT,
        picture TEXT
    """)

    print("Table created successfully!")


def insert_data(session, **kwargs):
    print("inserting data...")

    user_id = kwargs.get('id')
    first_name = kwargs.get('first_name')
    last_name = kwargs.get('last_name')
    gender = kwargs.get('gender')
    address = kwargs.get('address')
    postcode = kwargs.get('postcode', 'no postcode')
    email = kwargs.get('email')
    username = kwargs.get('username')
    dob = kwargs.get('dob')
    registered_date = kwargs.get('registered_date')
    phone = kwargs.get('phone')
    picture = kwargs.get('picture', 'no picture')

    try:
        session.execute("""
            INSERT INTO spark_streams.created_users(user_id, first_name, last_name, gender, address, 
            postcode, email, username, dob, registered_date, phone, picture)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (user_id, first_name, last_name, gender, address,
              postcode, email, username, dob, registered_date, phone, picture))
        logging.info(f"Data inserted for {first_name} {last_name}")
    except Exception as e:
        logging.error(f'Could not insert data due to exception: {e}')


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
    cassandra_session = None
    try:
        cluster = Cluster(['localhost'])
        cassandra_session = cluster.connect()
    except Exception as e:
        logging.error(f'Could not create cassandra connection due to exception: {e}')

    return cassandra_session


if __name__ == "__main__":
    spark_connection = create_spark_connection()

    if spark_connection is not None:
        session = create_cassandra_connection()

        if session:
            create_keyspace(session)
            create_table(session)
