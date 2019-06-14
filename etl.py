import configparser
from datetime import datetime
import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofmonth, dayofweek, date_format, monotonically_increasing_id


config = configparser.ConfigParser()
#config.read('dl.cfg')

#os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
#os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def delete_table_if_exists(path):
    print("checking if table exists")
    if os.path.exists(path):
        shutil.rmtree(path)
        print("Table exits, table deleted")
    else:
        print("Table does not exist")


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + "/song_data/[A-Z]/[A-Z]/[A-Z]/*\.json"
    
    # read song data file
    print("reading song_data in spark dataframe")
    df =  spark.read.json(song_data)
    df.printSchema()
    print(df.count())

    # extract columns to create songs table
    print("extracting columns to create songs_table")
    songs_table = df.select(["song_id", "title", "artist_id", "year", "duration"]).dropDuplicates()
    songs_table.printSchema()
    print(songs_table.count())
    
    songs_table_path = output_data + "/songs_table"
    delete_table_if_exists(songs_table_path)
    # write songs table to parquet files partitioned by year and artist

    print("writing songs_table")
    songs_table.write.partitionBy("year", "artist_id").parquet(songs_table_path)

    # extract columns to create artists table
    print("extracting columns to create artists_table")
    artists_table = df.select(["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]).dropDuplicates()
    artists_table.printSchema()
    print(artists_table.count())
    
    artists_table_path = output_data + "/artists_table"
    delete_table_if_exists(artists_table_path)
    # write artists table to parquet files

    print("writing artists_table")
    artists_table.write.parquet(artists_table_path)


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + "/log-data/*\.json"

    # read log data file
    print("Reading log_data in spark dataframe")
    df = spark.read.json(log_data) 
    df.printSchema()
    print(df.count())
    
    # filter by actions for song plays
    print("Filter on NextSong")
    df = df.filter(df["page"] == "NextSong")
    print(df.count())

    # extract columns for users table    
    print("extract columns to create users table")
    users_table = df.select(["userId", "firstName", "lastName", "gender", "level"]).dropDuplicates()
    users_table.printSchema()
    print(users_table.count())
    
    # write users table to parquet files
    users_table_path = output_data + "/users_table"
    delete_table_if_exists(users_table_path)

    print("writing users_table")
    users_table.write.parquet(users_table_path)

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda ts: datetime.fromtimestamp(ts/1000).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    timestamp_df = df.select(["timestamp"]).dropDuplicates()
    timestamp_df.printSchema()
    print(timestamp_df.count())
    
    # extract columns to create time table
    time_table = timestamp_df.select(
                                     date_format("timestamp", "yyyy-MM-dd HH:mm:ss").alias("start_time"),
                                     hour("timestamp").alias("hour"),
                                     dayofmonth("timestamp").alias("day"),
                                     weekofyear("timestamp").alias("week"),
                                     month("timestamp").alias("month"),
                                     year("timestamp").alias("year"),
                                     dayofweek("timestamp").alias("weekday")
                                     )

    time_table.printSchema()
    print(time_table.count())
    
    # write time table to parquet files partitioned by year and month
    time_table_path = output_data + "/time_table"
    delete_table_if_exists(time_table_path)

    print("writing time_table")
    time_table.write.partitionBy("year", "month").parquet(time_table_path)


    # read in song data to use for songplays table
    song_table = spark.read.parquet(output_data + "/songs_table")
    artists_table = spark.read.parquet(output_data + "/artists_table") 
    song_df = song_table.join(artists_table, song_table.artist_id == artists_table.artist_id).drop(song_table.artist_id)
    print("the join artist and song table")
    song_df.printSchema()
    print(song_df.count())

    # extract columns from joined song and log datasets to create songplays table 
    df_join_song_df = df.join(
                              song_df, 
                              (df.song == song_df.title) & 
                              (df.artist == song_df.artist_name) &
                              (df.length == song_df.duration) 
                              )

    df_join_song_df = df_join_song_df.withColumn("songplay_id", monotonically_increasing_id())
    df_join_song_df.printSchema()
    print(df_join_song_df.count())
    
    
    songplays_table = df_join_song_df.select(
                                             col("songplay_id"),
                                             date_format("timestamp", "yyyy-MM-dd HH:mm:ss").alias("start_time"),
                                             col("userId"),
                                             col("level"),
                                             col("song_id"),
                                             col("artist_id"),
                                             col("sessionId"),
                                             col("artist_location"),
                                             col("userAgent")
                                            )

    songplays_table.printSchema()


    # write songplays table to parquet files partitioned by year and month
    songplays_table_path = output_data + "/songplays_table"
    delete_table_if_exists(songplays_table_path)

    print("writing songplays_table")
    songplays_table.withColumn(
                              "year", 
                              year(
                                   songplays_table.start_time
                                   )).withColumn(
                                                "month", 
                                                month(
                                                      songplays_table.start_time
                                                      )).write.partitionBy(
                                                                           "year", 
                                                                           "month").parquet(
                                                                                            songplays_table_path
                                                                                            )


def main():
    spark = create_spark_session()
    #input_data = "s3a://udacity-dend/"
    input_data = os.path.expanduser('~') + "/DataEngNanoDegree/projects/DataLakeWithSpark/data"
    output_data = os.path.expanduser('~') + "/DataEngNanoDegree/projects/DataLakeWithSpark/outdata"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
