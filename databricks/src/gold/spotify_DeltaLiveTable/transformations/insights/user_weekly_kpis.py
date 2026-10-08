import dlt
from pyspark.sql import functions as F

@dlt.table
def user_weekly_kpis():
    """
    Aggregate weekly KPIs: weekly active users and total streams
    by subscription type for all available data (no current_date filter).
    """
    # Read Gold Core tables
    users = dlt.read("dimuser_staging")         # user_id, subscription_type, etc.
    streams = dlt.read("factstream_staging")    # user_id, track_id, stream_timestamp, listen_duration

    # Extract ISO week number and year
    streams_weekly = streams.withColumn(
        "week", F.weekofyear(F.col("stream_timestamp"))
    ).withColumn(
        "year", F.year(F.col("stream_timestamp"))
    )

    # Join with users to get subscription_type
    joined = streams_weekly.join(users, on="user_id", how="inner")

    # Aggregate KPIs
    kpi_df = joined.groupBy("year", "week", "subscription_type") \
        .agg(
            F.countDistinct("user_id").alias("weekly_active_users"),
            F.count("stream_id").alias("total_streams"),
            F.sum("listen_duration").alias("total_listen_duration")
        ) \
        .orderBy("year", "week", "subscription_type")

    return kpi_df
