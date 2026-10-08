import dlt
from pyspark.sql import functions as F

@dlt.table
def avg_listen_duration_per_device():
    """
    Aggregate average listen duration by device type for all available data.
    """
    # Read Gold Core facts table
    streams = dlt.read("factstream_staging")  # device_type, listen_duration, stream_timestamp

    # Aggregate average listen duration per device
    avg_duration = streams.groupBy("device_type") \
                          .agg(F.avg("listen_duration").alias("avg_listen_duration"))

    return avg_duration
