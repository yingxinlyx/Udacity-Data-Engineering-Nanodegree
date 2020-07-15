import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType, TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
        This function is to create a spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
        This function loads data in json format from S3, 
        then extracts needed fields to 2 dimensional tables: songs and artists.
        Finally it writes above tables to parquet files and stores back to S3
    """

    # read data
    song_data = input_data + 'song_data/*/*/*/*.json'
    df = spark.read.json(song_data)
    print('song_data has been read successfully!')
    
    # songs table
    songs_table = df.select(['artist_id', 'duration', 'title', 'year'])\
                    .dropDuplicates().withColumn('song_id', monotonically_increasing_id())
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data + 'songs.parquet', mode="overwrite")
    print('songs table has been created successfully!')

    # artists table 
    artists_fields = ["artist_id", "artist_name as name", "artist_location as location", "artist_latitude as latitude", \
                      "artist_longitude as longitude"]
    artists_table = df.selectExpr(artists_fields).dropDuplicates()
    artists_table.write.parquet(output_data + 'artists.parquet', mode="overwrite")
    print('artists table has been created successfully!')


def process_log_data(spark, input_data, output_data):
    """
        This function loads data in json format from S3, 
        then extracts needed fields to 3 dimensional tables: users, time and songplays.
        Finally it writes above tables to parquet files and stores back to S3 
    """
    
    # read data
    log_data = input_data + 'log_data/*/*/*.json'
    df = spark.read.json(log_data)
    df = df.filter(df.page == 'NextSong')
    print('log_data has been read successfully!')

    # users table    
    users_fields = ["userId as user_id", "firstName as first_name", "lastName as last_name", "gender", "level"]
    users_table = df.selectExpr(users_fields).dropDuplicates()
    
    users_table.write.parquet(output_data + 'users.parquet', mode="overwrite")
    print('users table has been created successfully!')

    # time table
    @udf(TimestampType())
    def get_timestamp (ts):
        return datetime.fromtimestamp(ts/1000.0)
    
    df = df.withColumn('ts', get_timestamp('ts'))
    
    df.createOrReplaceTempView("ts_table")
    time_table = spark.sql('''
        SELECT DISTINCT ts       AS start_time,
               hour(ts)          AS hour,
               day(ts)           AS day,
               weekofyear(ts)    AS week,
               month(ts)         AS month,
               year(ts)          AS year,
               dayofweek(ts)     AS weekday
        FROM ts_table
    ''')
    
    time_table.write.partitionBy("year", "month").parquet(output_data + 'time.parquet', mode="overwrite")
    print('time table has been created successfully!')

    # songplays table
    song_data = input_data + 'song_data/*/*/*/*.json'
    song_df = spark.read.json(song_data)

    df.createOrReplaceTempView('log_table')
    song_df.createOrReplaceTempView('song_table')
    songplays_table = spark.sql('''
        SELECT  e.ts           AS start_time, 
                year(e.ts)     AS year,
                month(e.ts)    AS month,
                e.userId       AS user_id, 
                e.level        AS level, 
                s.song_id      AS song_id, 
                s.artist_id    AS artist_id, 
                e.sessionId    AS session_id, 
                e.location     AS location, 
                e.userAgent    AS user_agent
        FROM log_table e
        JOIN song_table s ON (e.song = s.title AND e.artist = s.artist_name)
        order by 1
    ''')
    songplays_table = songplays_table.withColumn('songplays_id', monotonically_increasing_id())

    songplays_table.write.partitionBy('year', 'month').parquet(output_data + 'songplays.parquet', mode="overwrite")
    print('songplays table has been created successfully!')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
