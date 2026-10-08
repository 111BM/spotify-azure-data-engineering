import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

@dlt.table
def top_tracks_per_country():
    """
    Aggregate top 5 tracks per country based on all available data.
    """
    # Read Gold Core tables
    users = dlt.read("dimuser_staging")      # user_id, country
    streams = dlt.read("factstream_staging") # user_id, track_id, stream_timestamp

    # Join with users to get country
    joined = streams.join(users, on="user_id", how="inner")

    # Count streams per track per country
    track_counts = joined.groupBy("country", "track_id") \
        .agg(F.count("stream_id").alias("total_streams"))

    # Rank top 5 tracks per country
    window = Window.partitionBy("country").orderBy(F.desc("total_streams"))
    top_tracks = track_counts.withColumn("rank", F.row_number().over(window)) \
                             .filter(F.col("rank") <= 5)

    return top_tracks
