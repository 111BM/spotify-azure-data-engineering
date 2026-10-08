# Databricks notebook source
# Read average listen duration per device table
avg_device_df = spark.read.table("Spotify_catalog.gold.avg_listen_duration_per_device")

# Display average duration per device
display(avg_device_df.orderBy("device_type"))
