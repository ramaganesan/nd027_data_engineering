import configparser
import shutil
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, desc, dense_rank, dayofweek, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType, DateType
from pyspark.sql.window import Window

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Creates Spark Session
    :return: SparkSession
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .config("spark.sql.broadcastTimeout", "36000") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Loads the songs data from S3 and creates
    artists and songs table using Spark
    :param spark:
    :param input_data:
    :param output_data:
    :return:
    """
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data', '*', '*', '*')

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(['song_id', 'title', 'artist_id', 'year', 'duration'])

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(os.path.join(output_data, 'songs'), partitionBy=['year', 'artist_id'])

    # extract columns to create artists table
    artists_table = df.selectExpr(
        ['artist_id', 'artist_name as name', 'artist_location as location', 'artist_latitude as latitude',
         'artist_longitude as longitude'])

    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists'))


def process_log_data(spark, input_data, output_data):
    """
    Loads the logs data from S3 and creates
    users, time and songplays table using Spark
    :param spark:
    :param input_data:
    :param output_data:
    :return:
    """
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data', '*', '*')

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.where(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.selectExpr(['cast(userId as int) user_id', 'firstName as first_name',
                                 'lastName as last_name', 'gender', 'level', 'ts'])
    ## Below Code needed to update the users with last entry incase they changes
    ## their level
    w = Window.partitionBy('user_id').orderBy(desc('ts'))
    users_table = users_table.withColumn('row_rank', dense_rank().over(w))
    users_table.printSchema()
    users_table.filter(users_table.row_rank == 1).drop('row_rank', 'ts')

    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users'))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda ts: datetime.fromtimestamp(ts / 1000).isoformat())
    df = df.withColumn('start_time', get_timestamp('ts').cast(TimestampType()))

    # create datetime column from original timestamp column
    # get_datetime = udf(lambda ts: datetime.fromtimestamp(ts / 1000).isoformat())
    # df = df.withColumn('date_time', get_timestamp('ts').cast(DateType()))


    # extract columns to create time table
    time_table = df.select('start_time')
    time_table = time_table.withColumn('hour', hour('start_time'))
    time_table = time_table.withColumn('hour', hour('start_time'))
    time_table = time_table.withColumn('day', dayofmonth('start_time'))
    time_table = time_table.withColumn('week', weekofyear('start_time'))
    time_table = time_table.withColumn('month', month('start_time'))
    time_table = time_table.withColumn('year', year('start_time'))
    time_table = time_table.withColumn('weekday', dayofweek('start_time'))

    time_table.printSchema()

    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(os.path.join(output_data, 'time'), partitionBy=['year', 'month'])

    # read in song data to use for songplays table
    song_df = spark.read.json(os.path.join(input_data, 'song_data', '*', '*', '*'))

    # extract columns from joined song and log datasets to create songplays table
    # song_df.createOrReplaceTempView('songs')
    # df.createOrReplaceTempView('logs')
    # songplays_table = spark.sql("""
    #     SELECT
    #         row_number() as songplay_id,
    #         logs.ts as start_time,
    #         logs.userId ,
    #         logs.level,
    #         songs.song_id,
    #         songs.artist_id,
    #         logs.sessionId as session_id,
    #         logs.location,
    #         logs.userAgent as user_agent,
    #         year(logs.start_time) as year,
    #         month(logs.start_time) as month
    #     FROM logs
    #     LEFT JOIN songs  ON
    #         logs.song = songs.title AND
    #         logs.artist = songs.artist_name
    # """)

    songs_plays = df.join(song_df, [df.artist == song_df.artist_name, df.song == song_df.title], "left")

    songs_plays = songs_plays.withColumn("songplay_id", monotonically_increasing_id())
    songs_plays = songs_plays.withColumn('start_time', get_timestamp('ts').cast(TimestampType()))
    songs_plays = songs_plays.withColumn('year', year('start_time'))
    songs_plays = songs_plays.withColumn('month', month('start_time'))
    songs_plays = songs_plays.selectExpr(
        ['songplay_id', 'start_time', 'userId', 'level',
         'song_id', 'artist_id', 'sessionId as session_id', 'location', 'userAgent as user_agent', 'year', 'month'])
    songs_plays.printSchema()

    # write songplays table to parquet files partitioned by year and month
    songs_plays.write.parquet(os.path.join(output_data, 'songplays'), partitionBy=['year', 'month'])


def main():
    """
    Main function
    :return:
    """
    spark = create_spark_session()
    input_data = config['input_data']
    output_data = config['output_data']

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)
    spark.stop()


if __name__ == "__main__":
    main()
