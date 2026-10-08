# Databricks notebook source
# Read top tracks per country table
top_tracks_df = spark.read.table("Spotify_catalog.gold.top_tracks_per_country")

# Show top 5 tracks per country
display(top_tracks_df.orderBy("country", "rank"))
