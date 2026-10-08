import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

@dlt.table
def most_streamed_tracks_per_week():
    """
    Identify top 5 tracks each week for the last 4 weeks
    based on the maximum date available in the dataset.
    """

    streams = dlt.read("factstream_staging")  # track_id, stream_timestamp, stream_id

    # Determine max date in data
    max_date = streams.agg(F.max("stream_timestamp").alias("max_date")).collect()[0]["max_date"]

    # Filter last 4 weeks based on max date
    streams_filtered = streams.filter(
        F.col("stream_timestamp") >= F.date_sub(F.lit(max_date), 28)
    )

    # Extract week number and year
    streams_weekly = streams_filtered.withColumn("week", F.weekofyear(F.col("stream_timestamp"))) \
                                     .withColumn("year", F.year(F.col("stream_timestamp")))

    # Count streams per track per week
    track_counts = streams_weekly.groupBy("year", "week", "track_id") \
                                 .agg(F.count("stream_id").alias("total_streams"))

    # Rank top 5 tracks per week
    window = Window.partitionBy("year", "week").orderBy(F.desc("total_streams"))
    top_tracks = track_counts.withColumn("rank", F.row_number().over(window)) \
                             .filter(F.col("rank") <= 5)

    return top_tracks
