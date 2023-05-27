# Databricks notebook source
# MAGIC %md
# MAGIC Load/Prepare/Publish AirBnB Review Layer 1 database.

# COMMAND ----------

dbutils.notebook.run('/Users/slsb.brown1@outlook.com/GenericNotebook',60)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE tbldb.tblAirBnBReview
# MAGIC (ListingId INTEGER,
# MAGIC  ReviewDate TIMESTAMP,
# MAGIC  ReviewerName STRING,
# MAGIC  ReviewCommentDesc STRING,
# MAGIC  SentimentTypeName STRING,
# MAGIC  FirstCreatedDate TIMESTAMP,
# MAGIC  ProcessLoadName STRING);
# MAGIC
# MAGIC INSERT INTO tbldb.tblAirBnBReview
# MAGIC SELECT 
# MAGIC ListingId,
# MAGIC ReviewDate,
# MAGIC ReviewerName,
# MAGIC ReviewCommentDesc,
# MAGIC SentimentTypeName,
# MAGIC current_timestamp(),
# MAGIC current_user()
# MAGIC FROM stgdb.stgAirBnBReview;

# COMMAND ----------


