# Databricks notebook source
#Loads the autoreload extension.
%load_ext autoreload 
#Automatically reloads all imported Python modules before every cell execution.
%autoreload 2

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

#import method from customize python class 
import os
import sys

#add path  /Workspace/Users/<your-user>/spotify_dab/src/silver/../..  in our system path
project_pth=os.path.join(os.getcwd(), '..','..')
p_th=sys.path.append(project_pth)
from utils.transformations import reusable

# COMMAND ----------

# MAGIC %md
# MAGIC ## ** Read DimUser **

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW EXTERNAL LOCATIONS;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT READ FILES ON EXTERNAL LOCATION `silver` TO `<your-user>`;
# MAGIC GRANT WRITE FILES ON EXTERNAL LOCATION `silver` TO `<your-user>`;

# COMMAND ----------

#AUTOLOADER
# for schema location, we will creating one dedicated directory in adsl silver container DimUser, within that we will create data  where we will store data and within data we create another folder called checkpoint where autoloader will store all the information to track the idempotency behaviour.
df_user= (
    spark.readStream.format('cloudFiles')
    .option('cloudFiles.format', 'parquet')
    .option('cloudFiles.schemaLocation', 
                    "abfss://silver@tamstorageazureproject.dfs.core.windows.net/DimUser/checkpoint"
                    ) 
    .option('schemaEvolutionode', 'rescue') #or by default addNewColumn()
    .load("abfss://bronze@tamstorageazureproject.dfs.core.windows.net/DimUser")
)

# COMMAND ----------

display(df_user)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_user();

# COMMAND ----------

#LETS APPLY SOME SPARK TRANSFORMATION
# lets make user all name into uppoer case.
df_user_name=df_user.withColumn("user_name",upper(col("user_name")))
display(df_user_name)

# COMMAND ----------

#/Workspace/Users/<your-user>/spotify_dab/utils

from spotify_dab.utils.transformation import *


# COMMAND ----------

import os

#get current working directory
print(os.getcwd())

# COMMAND ----------

import os

##go to two folder back that is in spotify_dab
print(os.path.join(os.getcwd(), '..','..'))

# COMMAND ----------

import os
import sys

#add path  /Workspace/Users/<your-user>/spotify_dab/src/silver/../..  in our system path

project_pth=os.path.join(os.getcwd(), '..','..')
p_th=sys.path.append(project_pth)
from utils.transformations import var_biresh
print (var_biresh)






# COMMAND ----------

#Adjust above path, import on top of the notebook.

# COMMAND ----------

#test importing method from python class
#create an object of it.
df_user_object=reusable()
df_user=df_user_object.dropColumns(df_user, ['_rescued_data'])
display(df_user)


# COMMAND ----------

# MAGIC %md
# MAGIC lets add tranformation for deduplication

# COMMAND ----------

#obviously we will not be having any duplicates in the source but sometimes we use extra ordinary sources which through some duplicates. You will ask a source person, whatever your upstream is API, sql database, datalake. you can let them know and fix it. But your code should be robust enough to handle those things internally as well but ideally you should inform them.

df_user_object=reusable()
df_user=df_user_object.dropColumns(df_user, ['_rescued_data'])
df_user=df_user.dropDuplicates(['user_id'])

display(df_user)

# COMMAND ----------

#write df to delta format
#df_user to delta format
# trigger(once=True) write code into location stop automatically
#you can also say (processingTime='10 seconds') but for processing once just as below
#start() is the location where we want write a data
#currently we are simply writing the data
#But we can also create a table in silver layer--it is a good practice because whenever you create dimensional model, we will get better performance, better query, optimsation technique
#because data will be available natively in the catalogue and more readable
#Otherwise you will be using little complex way to load the data incrementally but if delta format then pretty much simpler
df_user.writeStream.format('delta')\
                    .outputMode('append')\
                    .option('checkpointLocation','abfss://silver@tamstorageazureproject.dfs.core.windows.net/DimUser/checkpoint')\
                    .trigger(once=True)\
                    .option('path','abfss://silver@tamstorageazureproject.dfs.core.windows.net/DimUser/data')\
                    .toTable('spotify_catalog.silver.DimUser')


# COMMAND ----------

# MAGIC %md
# MAGIC ## ** Read DimArtist **

# COMMAND ----------

#let's try to read the DimArtist data using autoloader
df_artist= (
    spark.readStream.format('cloudFiles')
    .option('cloudFiles.format', 'parquet')
    .option('cloudFiles.schemaLocation', 
                    "abfss://silver@tamstorageazureproject.dfs.core.windows.net/DimArtist/checkpoint"
                    ) 
    .option('schemaEvolutionode', 'rescue') #or by default addNewColumn()
    .load("abfss://bronze@tamstorageazureproject.dfs.core.windows.net/DimArtist")
)


# COMMAND ----------

#view datasets
display(df_artist)

# COMMAND ----------

#Perform simple transformation
#drop column _rescued_data

df_artist_object=reusable() #created an object for the class reusable
df_artist=df_artist_object.dropColumns(df_artist, ['_rescued_data'])
df_artist=df_artist.dropDuplicates(['artist_id'])

#see the data
display(df_artist)


# COMMAND ----------

#write df to delta format
df_artist.writeStream.format('delta')\
                    .outputMode('append')\
                    .option('checkpointLocation','abfss://silver@tamstorageazureproject.dfs.core.windows.net/DimArtist/checkpoint')\
                    .trigger(once=True)\
                    .option('path','abfss://silver@tamstorageazureproject.dfs.core.windows.net/DimArtist/data')\
                    .toTable('spotify_catalog.silver.DimArtist')
                    

# COMMAND ----------

# MAGIC %md
# MAGIC ## ** Read DimTrack **

# COMMAND ----------

#let's try to read the DimTrack data using autoloader
df_track= (
    spark.readStream.format('cloudFiles')
    .option('cloudFiles.format', 'parquet')
    .option('cloudFiles.schemaLocation', 
                    "abfss://silver@tamstorageazureproject.dfs.core.windows.net/DimTrack/checkpoint"
                    ) 
    .option('schemaEvolutionode', 'rescue') #or by default addNewColumn()
    .load("abfss://bronze@tamstorageazureproject.dfs.core.windows.net/DimTrack")
)


# COMMAND ----------

display(df_track)

# COMMAND ----------

#Give a group for column duration_sec based on duration
df_track=df_track.withColumn('durationFlag',when(df_track['duration_sec']<150,'low')
                             .when((df_track['duration_sec']>=150) & (df_track['duration_sec']<=300),'medium')
                             .otherwise('high'))

#replace hypen with normal space
#Transformation on track name
df_track=df_track.withColumn('track_name',regexp_replace(df_track['track_name'],'-',' '))

# Finally perform drop transformation
#drop column _rescued_data
df_track=reusable().dropColumns(df_track, ['_rescued_data'])


display(df_track)

# COMMAND ----------

#write df to delta format
df_track.writeStream.format('delta')\
                    .outputMode('append')\
                    .option('checkpointLocation','abfss://silver@tamstorageazureproject.dfs.core.windows.net/DimTrack/checkpoint')\
                    .trigger(once=True)\
                    .option('path','abfss://silver@tamstorageazureproject.dfs.core.windows.net/DimTrack/data')\
                    .toTable('spotify_catalog.silver.DimTrack')
                    

# COMMAND ----------

# MAGIC %md
# MAGIC ## ** Read Date **

# COMMAND ----------

#let's try to read the Date data using autoloader
df_date= (
    spark.readStream.format('cloudFiles')
    .option('cloudFiles.format', 'parquet')
    .option('cloudFiles.schemaLocation', 
                    "abfss://silver@tamstorageazureproject.dfs.core.windows.net/DimDate/checkpoint"
                    ) 
    .option('schemaEvolutionode', 'rescue') #or by default addNewColumn()
    .load("abfss://bronze@tamstorageazureproject.dfs.core.windows.net/DimDate")
)

# COMMAND ----------

display(df_date)

# COMMAND ----------

#drop column _rescued_data
df_date=reusable().dropColumns(df_date, ['_rescued_data'])
display(df_date)

# COMMAND ----------

#write to delta format 
df_date.writeStream.format('delta')\
                    .outputMode('append')\
                    .option('checkpointLocation','abfss://silver@tamstorageazureproject.dfs.core.windows.net/DimDate/checkpoint')\
                    .trigger(once=True)\
                    .option('path','abfss://silver@tamstorageazureproject.dfs.core.windows.net/DimDate/data')\
                    .toTable('spotify_catalog.silver.DimDate')
                    

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## ** Read FactStream **

# COMMAND ----------

#let's try to read the factstream data using autoloader
df_fact= (
    spark.readStream.format('cloudFiles')
    .option('cloudFiles.format', 'parquet')
    .option('cloudFiles.schemaLocation', 
                    "abfss://silver@tamstorageazureproject.dfs.core.windows.net/FactStream/checkpoint"
                    ) 
    .option('schemaEvolutionode', 'rescue') #or by default addNewColumn()
    .load("abfss://bronze@tamstorageazureproject.dfs.core.windows.net/FactStream")
)

# COMMAND ----------

#view data
display(df_fact)


# COMMAND ----------

#drop column _rescued_data
df_fact=reusable().dropColumns(df_fact, ['_rescued_data'])
display(df_fact)

# COMMAND ----------

#write df to delta format
df_fact.writeStream.format('delta')\
                    .outputMode('append')\
                    .option('checkpointLocation','abfss://silver@tamstorageazureproject.dfs.core.windows.net/FactStream/checkpoint')\
                    .trigger(once=True)\
                    .option('path','abfss://silver@tamstorageazureproject.dfs.core.windows.net/FactStream/data')\
                    .toTable('spotify_catalog.silver.FactStream')
                    

# COMMAND ----------

# Final pipeline run testing

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- before running spotify_incremental_load dataset
# MAGIC  select *from spotify_catalog.gold.dimtrack
# MAGIC --  where track_id=46
# MAGIC  where '_END_AT' IS NOT NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC -- datasets rows number read from bronze and dumped into silver
# MAGIC SELECT COUNT(*) 
# MAGIC FROM spotify_catalog.silver.dimtrack;
# MAGIC

# COMMAND ----------

#below was query run after running declarative data pipelines

# COMMAND ----------

# MAGIC %sql
# MAGIC -- datasets rows number read from silver and dumped into gold
# MAGIC SELECT COUNT(*) 
# MAGIC FROM spotify_catalog.gold.dimtrack;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- list two new recs
# MAGIC SELECT * FROM spotify_catalog.gold.dimtrack
# MAGIC WHERE __END_AT IS NOT NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC -- list two new recs
# MAGIC -- previous value of 46 artist_id was 225 and new is 9
# MAGIC -- previous value of 5 artist_id was 340 and new is 5
# MAGIC SELECT * FROM spotify_catalog.gold.dimtrack
# MAGIC WHERE track_id in (46,5)