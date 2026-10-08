# Databricks notebook source
# read insight delta live table created spotify_DeltaLiveTable 
kpi_df = spark.read.table("spotify_catalog.gold.user_weekly_kpis")

# Display it
display(kpi_df.orderBy("week", "subscription_type"))