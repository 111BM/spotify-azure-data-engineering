# Databricks notebook source
# Read the DLT table
top_tracks_df = spark.read.table("spotify_catalog.gold.most_streamed_tracks_per_week")

# Show first few rows
display(top_tracks_df)
