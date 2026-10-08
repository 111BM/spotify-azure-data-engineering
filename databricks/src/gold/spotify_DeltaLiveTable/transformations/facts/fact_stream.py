import dlt
#then use decorator, because use it in dlt world, which create table
#It can be streaming, batch or load.

#it will create a table based on the datasets present in the source. It is called declarative pipeline.
#You do not have say how to do but say what to do.
@dlt.table
def factstream_staging():
    df=spark.readStream.table('spotify_catalog.silver.factstream')
    return df


#create auto cdc or scd-2
#step
#1. create empty streaming table
#Note:- above table does not have a name so if not given function name will be name.
dlt.create_streaming_table('factstream')

#2. create auto cdc flow, which will create a dimension automatically using cdc flow.
#just a configuration, you will get scd-2 as per standard having start date, end date.

dlt.create_auto_cdc_flow(
target = "factstream",
source = "factstream_staging",
keys = ["stream_id"],
sequence_by = "stream_timestamp",
stored_as_scd_type = 1,
track_history_except_column_list = None,
name = None,
once = False
)
